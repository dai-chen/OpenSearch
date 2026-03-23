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
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.opensearch.querylanguages.opensearch.storage.scan.CalciteLogicalIndexScan;

/** Planner rule that pushes a {@link LogicalFilter} down to {@link CalciteLogicalIndexScan}. */
public class FilterIndexScanRule extends RelOptRule {

    /** Singleton instance. */
    public static final FilterIndexScanRule INSTANCE = new FilterIndexScanRule();

    private FilterIndexScanRule() {
        super(operand(LogicalFilter.class, operand(CalciteLogicalIndexScan.class, none())), "FilterIndexScanRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalFilter filter = call.rel(0);
        final CalciteLogicalIndexScan scan = call.rel(1);
        if (!scan.noAggregatePushed() || scan.isLimitPushed()) {
            return;
        }
        AbstractRelNode newRel = scan.pushDownFilter(filter);
        if (newRel != null) {
            call.transformTo(newRel);
        }
    }
}
