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
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.querylanguages.opensearch.storage.scan.CalciteLogicalIndexScan;

/**
 * Planner rule that pushes a {@link LogicalAggregate} down to {@link CalciteLogicalIndexScan}.
 *
 * @opensearch.internal
 */
public class AggregateIndexScanRule extends RelOptRule {

    /** Agg-Project-TableScan: aggregate with project push-down. */
    public static final AggregateIndexScanRule DEFAULT = new AggregateIndexScanRule(
        operand(LogicalAggregate.class, operand(LogicalProject.class, operand(CalciteLogicalIndexScan.class, none()))),
        "AggregateIndexScanRule:Agg-Project-TableScan"
    );

    /** Agg[count()]-TableScan: COUNT(*) without project. */
    public static final AggregateIndexScanRule COUNT_STAR = new AggregateIndexScanRule(
        operand(LogicalAggregate.class, operand(CalciteLogicalIndexScan.class, none())),
        "AggregateIndexScanRule:Agg-TableScan"
    );

    private AggregateIndexScanRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        if (call.rels.length == 3) {
            // Agg-Project-Scan
            final LogicalAggregate aggregate = call.rel(0);
            final LogicalProject project = call.rel(1);
            final CalciteLogicalIndexScan scan = call.rel(2);
            if (scan.isLimitPushed() || !scan.noAggregatePushed()) {
                return;
            }
            apply(call, aggregate, project, scan);
        } else if (call.rels.length == 2) {
            // COUNT(*) without project
            final LogicalAggregate aggregate = call.rel(0);
            final CalciteLogicalIndexScan scan = call.rel(1);
            if (scan.isLimitPushed() || !scan.noAggregatePushed()) {
                return;
            }
            if (!aggregate.getGroupSet().isEmpty()
                || !aggregate.getAggCallList()
                    .stream()
                    .allMatch(c -> c.getAggregation().kind == SqlKind.COUNT && c.getArgList().isEmpty())) {
                return;
            }
            apply(call, aggregate, null, scan);
        }
    }

    private void apply(RelOptRuleCall call, LogicalAggregate aggregate, LogicalProject project, CalciteLogicalIndexScan scan) {
        AbstractRelNode newRelNode = scan.pushDownAggregate(aggregate, project);
        if (newRelNode != null) {
            call.transformTo(newRelNode);
        }
    }
}
