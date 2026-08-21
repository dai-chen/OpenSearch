/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.spi.BackendShardPreference;
import org.opensearch.analytics.spi.ShardPreferenceContext;

import java.util.OptionalInt;

/**
 * Lucene's per-shard preference: opt in to drive count-fast-path fragments when the user has
 * enabled {@code analytics.planner.prefer_metadata_driver}.
 *
 * <p>Today the only signal is the cluster setting + fragment shape. Future shard-local
 * inputs (deletes, segment count, query-cache warmth) plug into the same scoring function as
 * {@link ShardPreferenceContext} grows.
 *
 * @opensearch.internal
 */
final class LuceneShardPreference implements BackendShardPreference {

    /** Wants-to-drive score — beats generic alternatives (score 0). */
    private static final int COUNT_FAST_PATH_SCORE = 100;

    /**
     * Value-scan score. Positive so {@code PlanAlternativeSelector.constrainToParentBackends} keeps a
     * Lucene shard stage alive under a DataFusion coordinator stage (the "blessed cross-backend
     * producer" case), but below {@link #COUNT_FAST_PATH_SCORE} because a metadata-only count is
     * always cheaper than materialising rows.
     */
    private static final int VALUE_SCAN_SCORE = 50;

    /** Veto score — actively don't pick this plan. Lucene returns this when the fragment
     *  is neither a count fast path nor a doc-values-readable value scan, so the selector doesn't
     *  accidentally collapse to a non-drivable Lucene alternative just because it appeared first in
     *  PlanForker order. */
    private static final int NOT_DRIVABLE_SCORE = -1;

    @Override
    public OptionalInt scoreFor(RelNode fragment, ShardPreferenceContext ctx) {
        if (ctx.preferMetadataDriver() == false) return OptionalInt.empty();
        if (LuceneFragmentConvertor.isCountFastPath(fragment)) {
            return OptionalInt.of(COUNT_FAST_PATH_SCORE);
        }
        // Plain (non-composite) indices keep their doc values in Lucene, so scan / passthrough-project
        // / filter fragments over them are drivable end-to-end. isValueScanFastPath checks the
        // fragment's per-column doc-value formats, which is what keeps this from firing on a composite
        // index where the same columns' values live in the parquet primary.
        if (LuceneFragmentConvertor.isValueScanFastPath(fragment)) {
            return OptionalInt.of(VALUE_SCAN_SCORE);
        }
        return OptionalInt.of(NOT_DRIVABLE_SCORE);
    }
}
