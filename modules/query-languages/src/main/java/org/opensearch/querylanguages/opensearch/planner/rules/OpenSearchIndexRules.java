/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.planner.rules;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;

/** Registry of OpenSearch index scan push-down rules. */
public class OpenSearchIndexRules {

    public static final List<RelOptRule> OPEN_SEARCH_INDEX_SCAN_RULES =
        ImmutableList.of(
            ProjectIndexScanRule.INSTANCE,
            FilterIndexScanRule.INSTANCE,
            LimitIndexScanRule.INSTANCE);

    // TODO: Phase 3 - Relevance function pushdown
    public static final RelOptRule RELEVANCE_FUNCTION_PUSHDOWN = FilterIndexScanRule.INSTANCE;

    private OpenSearchIndexRules() {}
}
