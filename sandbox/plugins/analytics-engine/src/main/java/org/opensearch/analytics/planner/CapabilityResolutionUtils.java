/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DataTransferCapability;
import org.opensearch.analytics.spi.ExchangeSinkProvider;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility logic that operates on {@link CapabilityRegistry} results.
 *
 * @opensearch.internal
 */
public final class CapabilityResolutionUtils {

    private CapabilityResolutionUtils() {}

    /**
     * Filters viable backends to those that can act as coordinator-side executors,
     * i.e., backends that provide a non-null {@link ExchangeSinkProvider}.
     *
     * <p>When none of {@code viableBackends} can reduce, falls back to every reduce-capable backend in
     * the registry — a <em>cross-backend gather</em>. This is reached when an operator's viable set has
     * narrowed to a scan-only backend, which is the normal case for a plain (non-composite) index: only
     * Lucene can read its doc values, and Lucene has no exchange sink. The gather is sound because the
     * producer stage ships Arrow record batches over Flight and the reducer only concatenates them —
     * it never needs to read the producer's storage. It is also not a new shape: the count fast path
     * already runs {@code SHARD_FRAGMENT=lucene → COORDINATOR_REDUCE=datafusion}, with the producer
     * describing its output schema to the reducer via
     * {@link org.opensearch.analytics.spi.FragmentConvertor#convertSchemaOnlyRead}.
     *
     * <p>The fallback only engages when the intersection is empty, so plans whose input can already
     * reduce (every composite-index plan) keep their previous backend set exactly.
     */
    public static List<String> filterByReduceCapability(CapabilityRegistry registry, List<String> viableBackends) {
        List<String> result = new ArrayList<>();
        for (String name : viableBackends) {
            AnalyticsSearchBackendPlugin backend = registry.getBackend(name);
            if (backend.getExchangeSinkProvider() != null) {
                result.add(name);
            }
        }
        if (result.isEmpty()) {
            for (AnalyticsSearchBackendPlugin backend : registry.getBackends()) {
                if (backend.getExchangeSinkProvider() != null) {
                    result.add(backend.name());
                }
            }
        }
        if (result.isEmpty()) {
            throw new IllegalStateException(
                "No viable backend supports coordinator reduce among " + viableBackends + " nor anywhere in the registry"
            );
        }
        return result;
    }

    /**
     * Filters viable backends to those that can drive a hash-shuffle producer stage, i.e., backends
     * that declare a {@link DataTransferCapability} with {@link DataTransferCapability.Kind#PRODUCER}.
     * A scan-only backend (e.g. Lucene, which declares no data-transfer capabilities) can be viable
     * for the shuffle's underlying scan but cannot serialize+ship hash partitions — if selected it
     * fails at execution with "Lucene driver does not handle instruction type: SHUFFLE_PRODUCER".
     * Mirrors {@link #filterByReduceCapability} so {@code OpenSearchDistributionTraitDef} prunes such
     * backends before building the {@code OpenSearchShuffleExchange}.
     */
    public static List<String> filterByShuffleProducerCapability(CapabilityRegistry registry, List<String> viableBackends) {
        List<String> result = new ArrayList<>();
        for (String name : viableBackends) {
            boolean canProduce = registry.getBackend(name)
                .getCapabilityProvider()
                .dataTransferCapabilities()
                .stream()
                .anyMatch(cap -> cap.kind() == DataTransferCapability.Kind.PRODUCER);
            if (canProduce) {
                result.add(name);
            }
        }
        if (result.isEmpty()) {
            throw new IllegalStateException("No viable backend supports hash-shuffle producer among " + viableBackends);
        }
        return result;
    }
}
