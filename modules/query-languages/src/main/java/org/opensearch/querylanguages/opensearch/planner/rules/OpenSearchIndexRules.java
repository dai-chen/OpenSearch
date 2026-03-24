/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.planner.rules;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;

import java.util.List;

/** Registry of OpenSearch index scan push-down rules. */
public class OpenSearchIndexRules {

    /** All push-down rules for the Calcite engine. */
    public static final List<RelOptRule> OPEN_SEARCH_INDEX_SCAN_RULES = ImmutableList.of(
        ProjectIndexScanRule.INSTANCE,
        FilterIndexScanRule.INSTANCE,
        LimitIndexScanRule.INSTANCE,
        AggregateIndexScanRule.DEFAULT,
        AggregateIndexScanRule.COUNT_STAR,
        SortIndexScanRule.INSTANCE,
        SortAggregateMeasureRule.INSTANCE,
        RareTopPushdownRule.INSTANCE
    );

    /** Relevance function pushdown rule (match, match_phrase, etc.). */
    public static final RelOptRule RELEVANCE_FUNCTION_PUSHDOWN = RelevanceFunctionPushdownRule.INSTANCE;

    private OpenSearchIndexRules() {}
}
