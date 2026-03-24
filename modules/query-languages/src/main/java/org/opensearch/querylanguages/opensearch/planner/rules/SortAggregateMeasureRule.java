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
import org.apache.calcite.rel.logical.LogicalSort;
import org.opensearch.querylanguages.opensearch.storage.scan.CalciteLogicalIndexScan;

/** SortAggregateMeasureRule planner rule. */
public class SortAggregateMeasureRule extends RelOptRule {

    /** Singleton instance. */
    public static final SortAggregateMeasureRule INSTANCE = new SortAggregateMeasureRule();

    private SortAggregateMeasureRule() {
        super(operand(LogicalSort.class, operand(CalciteLogicalIndexScan.class, none())), "SortAggregateMeasureRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalSort sort = call.rel(0);
        final CalciteLogicalIndexScan scan = call.rel(1);
        CalciteLogicalIndexScan newScan = scan.pushDownSortAggregateMeasure(sort);
        if (newScan != null) {
            call.transformTo(newScan);
        }
    }
    /** Rule configuration. */
}
