/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.querylanguages.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.querylanguages.opensearch.storage.scan.CalciteLogicalIndexScan;
import org.opensearch.sql.calcite.plan.rel.LogicalDedup;
import org.opensearch.sql.calcite.utils.PPLHintUtils;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.utils.Utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;

/**
 * DedupPushdownRule converts dedup to aggregate and pushes down to scan.
 *
 * @opensearch.internal
 */
public class DedupPushdownRule extends RelOptRule {

    /** Singleton instance. */
    public static final DedupPushdownRule INSTANCE = new DedupPushdownRule();

    private static final Logger LOG = LogManager.getLogger(DedupPushdownRule.class);

    private DedupPushdownRule() {
        super(
            operand(LogicalDedup.class, operand(LogicalProject.class, operand(CalciteLogicalIndexScan.class, none()))),
            "DedupPushdownRule"
        );
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalDedup dedup = call.rel(0);
        final CalciteLogicalIndexScan scan = call.rel(2);
        // Cannot push dedup if keepEmpty=true
        if (dedup.getKeepEmpty()) {
            return false;
        }
        // Project must not be pushed, and no limit/aggregate already pushed
        return Predicate.not(AbstractCalciteIndexScan::isLimitPushed).and(AbstractCalciteIndexScan::noAggregatePushed).test(scan);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalDedup logicalDedup = call.rel(0);
        final LogicalProject projectWithExpr = call.rel(1);
        final CalciteLogicalIndexScan scan = call.rel(2);

        List<RexNode> dedupColumns = logicalDedup.getDedupeFields();
        if (dedupColumns.stream()
            .filter(rex -> rex.isA(SqlKind.INPUT_REF))
            .anyMatch(rex -> rex.getType().getSqlTypeName() == SqlTypeName.MAP || rex.getType().getSqlTypeName() == SqlTypeName.ARRAY)) {
            LOG.debug("Cannot pushdown the dedup since the dedup fields contains MAP/ARRAY type");
            return;
        }

        RelBuilder relBuilder = call.builder();
        relBuilder.push(projectWithExpr);

        List<RexNode> targetProjections = new ArrayList<>();
        Set<Integer> dedupFieldsIndexSet = new HashSet<>();
        for (RexNode dedupColumn : dedupColumns) {
            if (dedupColumn instanceof RexInputRef) {
                RexInputRef ref = (RexInputRef) dedupColumn;
                targetProjections.add(dedupColumn);
                dedupFieldsIndexSet.add(ref.getIndex());
            } else {
                LOG.warn("The dedup column {} is illegal.", dedupColumn);
                return;
            }
        }
        IntStream.range(0, projectWithExpr.getProjects().size())
            .boxed()
            .filter(index -> !dedupFieldsIndexSet.contains(index))
            .map(relBuilder::field)
            .forEach(targetProjections::add);

        relBuilder.project(targetProjections);
        LogicalProject targetChildProject = (LogicalProject) relBuilder.peek();

        if (targetChildProject.getNamedProjects()
            .stream()
            .limit(dedupColumns.size())
            .anyMatch(pair -> Utils.resolveNestedPath(pair.getValue(), scan.getOsIndex().getFieldTypes()) != null)) {
            return;
        }

        List<Integer> newGroupByList = IntStream.range(0, dedupColumns.size()).boxed().toList();
        relBuilder.aggregate(
            relBuilder.groupKey(relBuilder.fields(newGroupByList)),
            relBuilder.literalAgg(logicalDedup.getAllowedDuplication())
        );

        PPLHintUtils.addIgnoreNullBucketHintToAggregate(relBuilder);
        LogicalAggregate aggregate = (LogicalAggregate) relBuilder.build();

        CalciteLogicalIndexScan newScan = (CalciteLogicalIndexScan) scan.pushDownAggregate(aggregate, targetChildProject);
        if (newScan != null) {
            call.transformTo(newScan.copyWithNewSchema(logicalDedup.getRowType()));
            PlanUtils.tryPruneRelNodes(call);
        }
    }
}
