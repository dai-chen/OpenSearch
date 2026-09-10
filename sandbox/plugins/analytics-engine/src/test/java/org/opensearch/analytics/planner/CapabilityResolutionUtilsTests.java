/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.spi.DataTransferCapability;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

public class CapabilityResolutionUtilsTests extends OpenSearchTestCase {

    public void testShuffleProducerAndConsumerCanUseDifferentBackends() {
        MockBackend luceneProducer = transferBackend(
            "lucene",
            Set.of(new DataTransferCapability(DataTransferCapability.Kind.PRODUCER, "arrow-ipc-partitioned"))
        );
        MockBackend dataFusionConsumer = transferBackend(
            "datafusion",
            Set.of(new DataTransferCapability(DataTransferCapability.Kind.CONSUMER, "arrow-ipc-partitioned"))
        );
        MockBackend incompatibleConsumer = transferBackend(
            "other",
            Set.of(new DataTransferCapability(DataTransferCapability.Kind.CONSUMER, "other-format"))
        );
        CapabilityRegistry registry = new CapabilityRegistry(
            List.of(luceneProducer, dataFusionConsumer, incompatibleConsumer),
            metadata -> null
        );

        assertEquals(
            List.of("datafusion"),
            CapabilityResolutionUtils.filterByCompatibleShuffleConsumer(
                registry,
                List.of("lucene"),
                List.of("datafusion", "other")
            )
        );
    }

    public void testShuffleConsumerRequiresCompatibleFormat() {
        MockBackend producer = transferBackend(
            "producer",
            Set.of(new DataTransferCapability(DataTransferCapability.Kind.PRODUCER, "arrow-ipc-partitioned"))
        );
        MockBackend consumer = transferBackend(
            "consumer",
            Set.of(new DataTransferCapability(DataTransferCapability.Kind.CONSUMER, "other-format"))
        );
        CapabilityRegistry registry = new CapabilityRegistry(List.of(producer, consumer), metadata -> null);

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> CapabilityResolutionUtils.filterByCompatibleShuffleConsumer(
                registry,
                List.of("producer"),
                List.of("consumer")
            )
        );
        assertTrue(exception.getMessage(), exception.getMessage().contains("arrow-ipc-partitioned"));
    }

    private static MockBackend transferBackend(String name, Set<DataTransferCapability> capabilities) {
        return new MockBackend() {
            @Override
            public String name() {
                return name;
            }

            @Override
            protected Set<DataTransferCapability> dataTransferCapabilities() {
                return capabilities;
            }
        };
    }
}
