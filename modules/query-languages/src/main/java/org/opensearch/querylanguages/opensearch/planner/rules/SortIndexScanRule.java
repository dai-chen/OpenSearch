/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Sort;
import org.opensearch.querylanguages.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.querylanguages.opensearch.storage.scan.CalciteEnumerableIndexScan;

/** SortIndexScanRule planner rule. */
public class SortIndexScanRule extends RelOptRule {

    /** Singleton instance. */
    public static final SortIndexScanRule INSTANCE = new SortIndexScanRule();

    private SortIndexScanRule() {
        super(operand(EnumerableSort.class, operand(CalciteEnumerableIndexScan.class, none())), "SortIndexScanRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        final AbstractCalciteIndexScan scan = call.rel(1);
        if (sort.getConvention() != scan.getConvention()) {
            return;
        }

        var collations = sort.collation.getFieldCollations();
        AbstractCalciteIndexScan newScan = scan.pushDownSort(collations);
        if (newScan != null) {
            call.transformTo(newScan);
        }
    }
    /** Rule configuration. */
}
