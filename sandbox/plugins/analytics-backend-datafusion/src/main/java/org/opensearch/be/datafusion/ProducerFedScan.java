/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.analytics.spi.ValueScanProducer;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.SessionContextHandle;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.List;

import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;

/**
 * Runs a shard fragment whose leaf comes from a {@link ValueScanProducer} rather than from a native
 * storage reader.
 *
 * <p>Used when the shard has no {@code DatafusionReader} — a plain (non-composite) index, whose values
 * live in Lucene doc values. Instead of surrendering the fragment to the producing back-end, this
 * registers the producer's Arrow batches as a streaming table under the <em>same</em> name the
 * fragment's Substrait {@code NamedTable} references, so the plan binds unchanged and every operator
 * above the leaf — filter, projection, sort, window, partial aggregate — is ordinary DataFusion
 * execution on the data node.
 *
 * <p>The session is worker-mode ({@link NativeBridge#createWorkerSessionContext}): no shard view, no
 * listing table, no parquet metadata — the same session kind a hash-shuffle worker uses for a plan
 * that reads only from named-input streams.
 *
 * <p>Batches are drained on a background thread, for the reason {@code ShuffleScanHandler} documents:
 * the native partition stream is a bounded mpsc, and the consumer does not run until every instruction
 * handler has returned, so a synchronous drain would deadlock once the channel fills.
 *
 * @opensearch.internal
 */
final class ProducerFedScan {

    private static final Logger LOGGER = LogManager.getLogger(ProducerFedScan.class);

    private ProducerFedScan() {}

    static DataFusionSessionState open(
        DataFusionPlugin plugin,
        ShardScanExecutionContext context,
        ShardScanInstructionNode node,
        ValueScanProducer producer
    ) {
        if (node.requestsRowIds()) {
            // QTF's narrowed scan emits __row_id__, which is a parquet row-id concept; a producer's
            // rows carry no such identity, so fetch-by-row-id could not resolve them later.
            throw new IllegalStateException(
                "Row-id emitting scans are not supported over a value-scan producer on shard " + context.getShardId()
            );
        }
        List<String> columns = scanColumns(context.getFragmentBytes());
        if (columns.isEmpty()) {
            throw new IllegalStateException("Value-scan producer fragment on shard " + context.getShardId() + " declares no scan columns");
        }
        String tableName = node.getLogicalTableName() != null ? node.getLogicalTableName() : context.getTableName();
        org.apache.arrow.vector.types.pojo.Schema schema = producer.schema(columns, context);

        long runtimePtr = plugin.getDataFusionService().getNativeRuntime().get();
        long contextId = context.getTask() != null ? context.getTask().getId() : 0L;
        WireConfigSnapshot snapshot = plugin.getDatafusionSettings().getSnapshot();

        SessionContextHandle sessionCtxHandle;
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);
            sessionCtxHandle = NativeBridge.createWorkerSessionContext(runtimePtr, contextId, segment.address());
        }

        long senderPtr;
        try {
            senderPtr = NativeBridge.registerPartitionStreamOnSessionContext(
                sessionCtxHandle.getPointer(),
                tableName,
                ArrowSchemaIpc.toBytes(schema)
            );
        } catch (Exception e) {
            throw new IllegalStateException("Failed to register value-scan stream for table [" + tableName + "]", e);
        }

        DatafusionPartitionSender sender = new DatafusionPartitionSender(senderPtr);
        startDrain(producer, columns, context, sender, tableName);
        return new DataFusionSessionState(sessionCtxHandle);
    }

    /**
     * Pumps the producer's batches into {@code sender} off-thread. A drain failure {@link
     * DatafusionPartitionSender#fail fails} the stream rather than closing it: closing signals EOF, and
     * the plan would then produce a silently short result from partial input.
     */
    private static void startDrain(
        ValueScanProducer producer,
        List<String> columns,
        ShardScanExecutionContext context,
        DatafusionPartitionSender sender,
        String tableName
    ) {
        BufferAllocator allocator = context.getAllocator();
        Thread drain = new Thread(() -> {
            Throwable failure = null;
            int[] batches = { 0 };
            try {
                producer.produce(columns, context, batch -> {
                    ArrowArray array = ArrowArray.allocateNew(allocator);
                    ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
                    boolean handedOff = false;
                    try {
                        Data.exportVectorSchemaRoot(allocator, batch, null, array, arrowSchema);
                        sender.send(array.memoryAddress(), arrowSchema.memoryAddress());
                        handedOff = true;
                        batches[0]++;
                    } finally {
                        if (handedOff == false) {
                            try {
                                array.close();
                            } catch (Throwable ignore) {
                                // best-effort — the primary error is being surfaced
                            }
                            try {
                                arrowSchema.close();
                            } catch (Throwable ignore) {
                                // best-effort — the primary error is being surfaced
                            }
                        }
                    }
                });
            } catch (Throwable t) {
                failure = t;
                LOGGER.error("ProducerFedScan drain FAILED for table [" + tableName + "] — failing the consumer stream", t);
            } finally {
                if (failure == null) {
                    sender.close();
                    LOGGER.debug(
                        "[producer-fed-scan] shardId={} table={} columns={} batches={}",
                        context.getShardId(),
                        tableName,
                        columns,
                        batches
                    );
                } else {
                    sender.fail("value-scan producer drain failed: " + failure);
                }
            }
        }, "producer-fed-scan-drain");
        drain.setDaemon(true);
        drain.start();
    }

    /**
     * The scan leaf's column names, read from the fragment's Substrait {@code ReadRel.base_schema}.
     * That is the same declaration the native side binds the table against, so taking the names from
     * here keeps the producer's batches and the plan's expectations in agreement by construction.
     */
    private static List<String> scanColumns(byte[] fragmentBytes) {
        if (fragmentBytes == null || fragmentBytes.length == 0) {
            return List.of();
        }
        try {
            Plan plan = Plan.parseFrom(fragmentBytes);
            for (PlanRel relation : plan.getRelationsList()) {
                List<String> names = findReadNames(relation.getRoot().getInput());
                if (names.isEmpty() == false) {
                    return names;
                }
            }
            return List.of();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read scan columns from the fragment's Substrait plan", e);
        }
    }

    /** Walks down to the first {@code ReadRel} and returns its base-schema names. */
    private static List<String> findReadNames(io.substrait.proto.Rel rel) {
        return switch (rel.getRelTypeCase()) {
            case READ -> rel.getRead().getBaseSchema().getNamesList();
            case FILTER -> findReadNames(rel.getFilter().getInput());
            case PROJECT -> findReadNames(rel.getProject().getInput());
            case AGGREGATE -> findReadNames(rel.getAggregate().getInput());
            case SORT -> findReadNames(rel.getSort().getInput());
            case FETCH -> findReadNames(rel.getFetch().getInput());
            default -> List.of();
        };
    }
}
