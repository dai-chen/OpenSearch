/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.index.engine.exec.IndexReaderProvider;

import java.io.IOException;
import java.util.List;

/**
 * Produces a shard's column <em>values</em> as Arrow batches, independently of which back-end
 * executes the fragment those values feed.
 *
 * <p>This exists so a back-end that can read a shard's storage but cannot compute over it — Lucene
 * doc values on an index with no columnar primary — can act as the scan <em>source</em> for a
 * back-end that can, rather than having to drive the whole fragment itself. The consumer registers
 * the batches as its own scan leaf; every operator above is then the consumer's, unchanged.
 *
 * <p>Resolution is by shard, not by configuration: the analytics layer offers each shard's reader to
 * the registered producers and threads the one that {@link #supports accepts} onto the
 * {@link ShardScanExecutionContext}. A producer therefore never needs to know how the index was
 * configured, only whether the reader publishes a format it can read.
 *
 * <p>No filter is passed. A consumer applies its own predicates to the produced batches; pushing a
 * predicate back into the producer is a separate concern served by
 * {@link org.opensearch.analytics.spi.FilterDelegationHandle}.
 *
 * @opensearch.internal
 */
public interface ValueScanProducer {

    /**
     * Whether this producer can read {@code reader} — i.e. whether the reader publishes a data format
     * whose values this producer knows how to materialise.
     */
    boolean supports(IndexReaderProvider.Reader reader);

    /**
     * The Arrow schema this producer will emit for {@code columns}, in that order. Must match the
     * batches {@link #produce} pushes, field for field including nullability: a consumer that
     * registers a stream declares this schema up front and binds arriving batches positionally
     * against it.
     *
     * @throws IllegalStateException if a column is not readable on this shard
     */
    Schema schema(List<String> columns, ShardScanExecutionContext context);

    /**
     * Materialises {@code columns} for every live document on the shard, handing each batch to
     * {@code sink}. The root passed to the sink is owned by the producer and is valid only for the
     * duration of the call — a consumer that needs to retain it must copy or export it.
     */
    void produce(List<String> columns, ShardScanExecutionContext context, BatchSink sink) throws IOException;

    /** Receives one batch. */
    @FunctionalInterface
    interface BatchSink {
        void accept(VectorSchemaRoot batch) throws IOException;
    }
}
