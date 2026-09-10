/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Duplicates a narrowing pass-through project below a window-bearing project directly over
 * a table scan, so only the window's required columns cross the gather:
 *
 * <pre>
 *   Project(window expressions)       coordinator
 *     ExchangeReducer
 *       Project(required input refs)  shard
 *         Scan
 * </pre>
 *
 * <p>The window project remains above the exchange through its existing SINGLETON cost gate.
 * The lower project is row-wise and can stay with the scan, allowing scan backends to read only
 * supported and required columns.
 *
 * @opensearch.internal
 */
public class OpenSearchWindowInputProjectSplitRule extends RelOptRule {

    public OpenSearchWindowInputProjectSplitRule() {
        super(operand(OpenSearchProject.class, any()), "OpenSearchWindowInputProjectSplitRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchProject project = call.rel(0);
        if (!project.containsOver()) {
            return;
        }

        RelNode child = RelNodeUtils.unwrapHep(project.getInput());
        if (!(child instanceof OpenSearchTableScan tableScan)) {
            return;
        }

        Set<Integer> referenced = new LinkedHashSet<>();
        RexShuttle collector = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                referenced.add(ref.getIndex());
                return ref;
            }
        };
        project.getProjects().forEach(expr -> expr.accept(collector));

        int inputFieldCount = child.getRowType().getFieldCount();
        if (referenced.isEmpty() || referenced.size() == inputFieldCount) {
            return;
        }

        List<Integer> kept = referenced.stream().sorted().toList();
        int[] remap = new int[inputFieldCount];
        Arrays.fill(remap, -1);

        List<RexNode> lowerExprs = new ArrayList<>(kept.size());
        RelDataTypeFactory.Builder lowerTypeBuilder = project.getCluster().getTypeFactory().builder();
        List<RelDataTypeField> inputFields = child.getRowType().getFieldList();
        for (int newIndex = 0; newIndex < kept.size(); newIndex++) {
            int oldIndex = kept.get(newIndex);
            remap[oldIndex] = newIndex;
            RelDataTypeField field = inputFields.get(oldIndex);
            lowerExprs.add(project.getCluster().getRexBuilder().makeInputRef(field.getType(), oldIndex));
            lowerTypeBuilder.add(field.getName(), field.getType());
        }
        RelDataType lowerRowType = lowerTypeBuilder.build();

        OpenSearchProject lower = new OpenSearchProject(
            project.getCluster(),
            child.getTraitSet(),
            child,
            lowerExprs,
            lowerRowType,
            tableScan.getViableBackends()
        );

        RexShuttle rewriter = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                int newIndex = remap[ref.getIndex()];
                if (newIndex < 0) {
                    throw new IllegalStateException("Window project references an input omitted from its lower projection");
                }
                return project.getCluster().getRexBuilder().makeInputRef(lowerRowType.getFieldList().get(newIndex).getType(), newIndex);
            }
        };
        List<RexNode> upperExprs = project.getProjects().stream().map(expr -> expr.accept(rewriter)).toList();

        call.transformTo(
            new OpenSearchProject(
                project.getCluster(),
                project.getTraitSet(),
                lower,
                upperExprs,
                project.getRowType(),
                project.getViableBackends(),
                project.isPinAboveExchange()
            )
        );
    }
}
