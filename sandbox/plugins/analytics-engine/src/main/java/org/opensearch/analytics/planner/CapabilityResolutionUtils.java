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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
            // A reduce stage consumes exchanged Arrow batches and does not scan the child's
            // storage format. Permit any registered sink-capable backend to execute it.
            for (AnalyticsSearchBackendPlugin backend : registry.getBackends()) {
                if (viableBackends.contains(backend.name()) == false && backend.getExchangeSinkProvider() != null) {
                    result.add(backend.name());
                }
            }
        }
        if (result.isEmpty()) {
            throw new IllegalStateException("No registered backend supports coordinator reduce for " + viableBackends);
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

    /**
     * Resolves coordinator/worker backends that can consume the transfer format emitted by
     * the selected shuffle producers. Producer and consumer backend IDs may differ, as with
     * Lucene doc-values producing Arrow IPC for a DataFusion worker stage.
     */
    public static List<String> filterByCompatibleShuffleConsumer(
        CapabilityRegistry registry,
        List<String> producerBackends,
        List<String> viableConsumers
    ) {
        Set<String> producerFormats = new HashSet<>();
        for (String name : producerBackends) {
            registry.getBackend(name)
                .getCapabilityProvider()
                .dataTransferCapabilities()
                .stream()
                .filter(capability -> capability.kind() == DataTransferCapability.Kind.PRODUCER)
                .map(DataTransferCapability::format)
                .forEach(producerFormats::add);
        }

        List<String> result = new ArrayList<>();
        for (String name : viableConsumers) {
            boolean canConsume = registry.getBackend(name)
                .getCapabilityProvider()
                .dataTransferCapabilities()
                .stream()
                .anyMatch(
                    capability -> capability.kind() == DataTransferCapability.Kind.CONSUMER
                        && producerFormats.contains(capability.format())
                );
            if (canConsume) {
                result.add(name);
            }
        }
        if (result.isEmpty()) {
            throw new IllegalStateException(
                "No viable shuffle consumer among "
                    + viableConsumers
                    + " accepts producer formats "
                    + producerFormats
                    + " from "
                    + producerBackends
            );
        }
        return result;
    }
}
