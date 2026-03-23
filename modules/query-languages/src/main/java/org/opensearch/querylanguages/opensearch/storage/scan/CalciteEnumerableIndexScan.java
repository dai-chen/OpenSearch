/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.storage.scan;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.querylanguages.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.querylanguages.opensearch.storage.OpenSearchIndex;
import org.opensearch.querylanguages.opensearch.storage.scan.context.PushDownContext;

import java.util.List;
import java.util.stream.Collectors;

import org.checkerframework.checker.nullness.qual.Nullable;

/** The physical relational operator representing a scan of an OpenSearchIndex type. */
public class CalciteEnumerableIndexScan extends AbstractCalciteIndexScan implements EnumerableRel {
    private static final Logger LOG = LogManager.getLogger(CalciteEnumerableIndexScan.class);

    /**
     * Creates an CalciteOpenSearchIndexScan.
     *
     * @param cluster Cluster
     * @param table Table
     * @param osIndex OpenSearch index
     */
    public CalciteEnumerableIndexScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        OpenSearchIndex osIndex,
        RelDataType schema,
        PushDownContext pushDownContext
    ) {
        super(cluster, traitSet, hints, table, osIndex, schema, pushDownContext);
    }

    @Override
    protected AbstractCalciteIndexScan buildScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelHint> hints,
        RelOptTable table,
        OpenSearchIndex osIndex,
        RelDataType schema,
        PushDownContext pushDownContext
    ) {
        return new CalciteEnumerableIndexScan(cluster, traitSet, hints, table, osIndex, schema, pushDownContext);
    }

    @Override
    public void register(RelOptPlanner planner) {
        // TODO: OpenSearch optimization rules will be registered when fully migrated
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        /* In Calcite enumerable operators, row of single column will be optimized to a scalar value.
         * See {@link PhysTypeImpl}.
         * Since we need to combine this operator with their original ones,
         * let's follow this convention to apply the optimization here and ensure `scan` method
         * returns the correct data format for single column rows.
         * See {@link OpenSearchIndexEnumerator}
         * Besides, we replace all dots in fields to avoid the Calcite codegen bug.
         * https://github.com/opensearch-project/sql/issues/4619
         */
        PhysType physType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            // TODO: OpenSearchRelOptUtil.replaceDot not yet migrated to core
            getRowType(),
            pref.preferArray()
        );

        Expression scanOperator = implementor.stash(this, CalciteEnumerableIndexScan.class);
        return implementor.result(physType, Blocks.toBlock(Expressions.call(scanOperator, "scan")));
    }

    /**
     * This Enumerator may be iterated for multiple times, so we need to create opensearch request for
     * each time to avoid reusing source builder. That's because the source builder has stats like PIT
     * or SearchAfter recorded during previous search.
     */
    public Enumerable<@Nullable Object> scan() {
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<Object> enumerator() {
                OpenSearchRequestBuilder requestBuilder = getOrCreateRequestBuilder();
                return new OpenSearchIndexEnumerator(
                    osIndex.getClient(),
                    getFieldPath(),
                    requestBuilder.getMaxResponseSize(),
                    requestBuilder.getMaxResultWindow(),
                    osIndex.buildRequest(requestBuilder),
                    osIndex.createOpenSearchResourceMonitor()
                );
            }
        };
    }

    private List<String> getFieldPath() {
        return getRowType().getFieldNames().stream().map(f -> osIndex.getAliasMapping().getOrDefault(f, f)).collect(Collectors.toList());
    }

    /**
     * In some edge cases where the digests of more than one scan are the same, and then the Calcite
     * planner will reuse the same scan along with the same PushDownContext inner it. However, the
     * `OpenSearchRequestBuilder` inner `PushDownContext` is not reusable since it has status changed
     * in the search process.
     *
     * <p>To avoid this issue and try to construct `OpenSearchRequestBuilder` as less as possible,
     * this method will get and reuse the `OpenSearchRequestBuilder` in PushDownContext for the first
     * time, and then construct new ones for the following invoking.
     */
    private volatile boolean isRequestBuilderUsedByEnumerator = false;

    private OpenSearchRequestBuilder getOrCreateRequestBuilder() {
        synchronized (this.pushDownContext) {
            if (isRequestBuilderUsedByEnumerator) {
                return this.pushDownContext.createRequestBuilder();
            }
            isRequestBuilderUsedByEnumerator = true;
            return this.pushDownContext.getRequestBuilder();
        }
    }
}
