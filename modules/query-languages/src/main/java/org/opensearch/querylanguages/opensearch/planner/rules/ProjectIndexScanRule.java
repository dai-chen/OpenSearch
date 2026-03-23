/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.planner.rules;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.opensearch.querylanguages.opensearch.storage.OpenSearchIndex;
import org.opensearch.querylanguages.opensearch.storage.scan.CalciteLogicalIndexScan;

/** Planner rule that pushes a {@link LogicalProject} down to {@link CalciteLogicalIndexScan}. */
public class ProjectIndexScanRule extends RelOptRule {

    public static final ProjectIndexScanRule INSTANCE = new ProjectIndexScanRule();

    private ProjectIndexScanRule() {
        super(
            operand(
                LogicalProject.class,
                operand(CalciteLogicalIndexScan.class, none())),
            "ProjectIndexScanRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalProject project = call.rel(0);
        final CalciteLogicalIndexScan scan = call.rel(1);

        final RelOptTable table = scan.getTable();
        requireNonNull(table.unwrap(OpenSearchIndex.class));

        final SelectedColumns selectedColumns = new SelectedColumns();
        final RexVisitorImpl<Void> visitor =
            new RexVisitorImpl<Void>(true) {
                @Override
                public Void visitInputRef(RexInputRef inputRef) {
                    if (!selectedColumns.contains(inputRef.getIndex())) {
                        selectedColumns.add(inputRef.getIndex());
                    }
                    return null;
                }
            };
        visitor.visitEach(project.getProjects());
        if (!selectedColumns.isEmpty()
            && !selectedColumns.isIdentity(scan.getRowType().getFieldCount())) {
            Mapping mapping =
                Mappings.target(selectedColumns, scan.getRowType().getFieldCount());
            CalciteLogicalIndexScan newScan = scan.pushDownProject(selectedColumns);
            if (newScan != null) {
                final List<RexNode> newProjectRexNodes =
                    RexUtil.apply(mapping, project.getProjects());
                if (RexUtil.isIdentity(newProjectRexNodes, newScan.getRowType())) {
                    call.transformTo(newScan);
                } else {
                    call.transformTo(
                        call.builder().push(newScan).project(newProjectRexNodes).build());
                }
            }
        }
    }

    static final class SelectedColumns extends ArrayList<Integer> {
        private boolean isSequential = true;
        private Integer current = 0;

        @Override
        public boolean add(Integer integer) {
            if (isSequential && !Objects.equals(integer, current++)) {
                isSequential = false;
            }
            return super.add(integer);
        }

        public boolean isIdentity(Integer size) {
            return isSequential && size == size();
        }
    }
}
