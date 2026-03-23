/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.opensearch.querylanguages.opensearch.storage.scan.CalciteEnumerableIndexScan;
import org.opensearch.querylanguages.opensearch.storage.scan.CalciteLogicalIndexScan;

/** Rule to convert a {@link CalciteLogicalIndexScan} to a {@link CalciteEnumerableIndexScan}. */
public class EnumerableIndexScanRule extends ConverterRule {
    /** Default configuration. */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE.as(Config.class)
        .withConversion(
            CalciteLogicalIndexScan.class,
            s -> s.getOsIndex() != null,
            Convention.NONE,
            EnumerableConvention.INSTANCE,
            "EnumerableIndexScanRule"
        )
        .withRuleFactory(EnumerableIndexScanRule::new);

    /** Creates an EnumerableIndexScanRule.
    * @param config the rule config
    */
    protected EnumerableIndexScanRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        CalciteLogicalIndexScan scan = call.rel(0);
        return scan.getVariablesSet().isEmpty();
    }

    @Override
    public RelNode convert(RelNode rel) {
        final CalciteLogicalIndexScan scan = (CalciteLogicalIndexScan) rel;
        return new CalciteEnumerableIndexScan(
            scan.getCluster(),
            // Retains RelDistribution and RelCollation but replaces Convention
            scan.getTraitSet().plus(EnumerableConvention.INSTANCE),
            scan.getHints(),
            scan.getTable(),
            scan.getOsIndex(),
            scan.getSchema(),
            scan.getPushDownContext()
        );
    }
}
