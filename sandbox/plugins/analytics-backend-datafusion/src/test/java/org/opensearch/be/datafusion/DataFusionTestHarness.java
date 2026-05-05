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
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.substrait.extension.SimpleExtension;

import static org.apache.arrow.c.Data.importField;

/**
 * Test-only helper that encapsulates the native DataFusion bootstrap + in-memory table
 * registration + Substrait plan execution + Arrow C Data result import used across every
 * DataFusion unit test in this module.
 *
 * <p>Why this is test-only: the production path goes through {@code DefaultPlanExecutor} →
 * {@code DAGBuilder} → {@code Scheduler}, which requires Guice-wired cluster collaborators
 * (TransportService, ClusterService, NodeClient, TaskManager). That path is appropriate for
 * integration tests ({@code OpenSearchIntegTestCase}), not unit tests. For in-process UDF /
 * RelNode correctness checks, bypassing the scheduler and driving the native runtime
 * directly is the established idiom — see {@code DatafusionReduceSinkTests},
 * {@code DatafusionMemtableReduceSinkTests}, {@code DataFusionQueryExecutionTests}.
 *
 * <p>Typical usage in a {@code OpenSearchTestCase} subclass:
 * <pre>
 * private DataFusionTestHarness harness;
 *
 * {@literal @}Override public void setUp() throws Exception {
 *     super.setUp();
 *     harness = DataFusionTestHarness.open(createTempDir("spill"));
 * }
 * {@literal @}Override public void tearDown() throws Exception {
 *     harness.close();
 *     super.tearDown();
 * }
 *
 * public void testX() throws Exception {
 *     byte[] substrait = ... ;                 // RelNode → DataFusionFragmentConvertor
 *     Schema schema = ... ;                    // Arrow schema of the input
 *     VectorSchemaRoot batch = ... ;           // in-memory Arrow batch
 *     List&lt;Object[]&gt; rows =
 *         harness.execute("input_table", schema, List.of(batch), substrait);
 *     assertEquals(expected, rows.get(0)[0]);
 * }
 * </pre>
 */
final class DataFusionTestHarness implements AutoCloseable {

    private final NativeRuntimeHandle runtime;
    private final SimpleExtension.ExtensionCollection extensions;
    private final RootAllocator allocator;

    private DataFusionTestHarness(NativeRuntimeHandle runtime, SimpleExtension.ExtensionCollection extensions, RootAllocator allocator) {
        this.runtime = runtime;
        this.extensions = extensions;
        this.allocator = allocator;
    }

    /**
     * Boots the Tokio runtime manager, creates a global DataFusion runtime backed by
     * {@code spillDir}, loads the default Substrait extension catalog with the caller's
     * classloader as TCCL (required by isthmus' YAML loader), and opens a single Arrow
     * {@link RootAllocator} that callers must use when building input batches (Arrow's C Data
     * export requires the export allocator to be the root of the batch's allocator tree).
     */
    public static DataFusionTestHarness open(Path spillDir) {
        NativeBridge.initTokioRuntimeManager(2);
        long runtimePtr = NativeBridge.createGlobalRuntime(
            64 * 1024 * 1024,         // memory limit
            0L,                       // no cache
            spillDir.toString(),
            32 * 1024 * 1024          // spill limit
        );
        NativeRuntimeHandle runtime = new NativeRuntimeHandle(runtimePtr);

        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        SimpleExtension.ExtensionCollection extensions;
        try {
            t.setContextClassLoader(DataFusionTestHarness.class.getClassLoader());
            extensions = SubstraitExtensionLoader.load();
        } finally {
            t.setContextClassLoader(prev);
        }
        return new DataFusionTestHarness(runtime, extensions, new RootAllocator(Long.MAX_VALUE));
    }

    /** Substrait default extension collection — pass to {@link DataFusionFragmentConvertor}. */
    public SimpleExtension.ExtensionCollection extensions() {
        return extensions;
    }

    /** Shared Arrow allocator — use this to build input {@link VectorSchemaRoot} batches. */
    public RootAllocator allocator() {
        return allocator;
    }

    /**
     * Registers the given Arrow batches as a memtable under {@code inputTable}, executes the
     * Substrait plan against it, and returns all output rows (each an {@code Object[]} of per-column
     * values as produced by Arrow's {@code ValueVector.getObject}).
     */
    public List<Object[]> execute(String inputTable, Schema inputSchema, List<VectorSchemaRoot> batches, byte[] substrait)
        throws Exception {
        try (DatafusionLocalSession session = new DatafusionLocalSession(runtime.get())) {
            // Export every batch across the C Data interface using the SHARED allocator (Arrow
            // requires the export allocator to be the root of the batch's allocator tree), then
            // hand all FFI structs over to the native side in a single registerMemtable call.
            // Native takes ownership on success — wrappers become no-op on close after that.
            List<ArrowArray> arrays = new ArrayList<>(batches.size());
            List<ArrowSchema> schemas = new ArrayList<>(batches.size());
            try {
                for (VectorSchemaRoot batch : batches) {
                    ArrowArray arr = ArrowArray.allocateNew(allocator);
                    ArrowSchema sch = ArrowSchema.allocateNew(allocator);
                    Data.exportVectorSchemaRoot(allocator, batch, null, arr, sch);
                    arrays.add(arr);
                    schemas.add(sch);
                }
                long[] arrayPtrs = arrays.stream().mapToLong(ArrowArray::memoryAddress).toArray();
                long[] schemaPtrs = schemas.stream().mapToLong(ArrowSchema::memoryAddress).toArray();
                byte[] schemaIpc = ArrowSchemaIpc.toBytes(inputSchema);
                NativeBridge.registerMemtable(session.getPointer(), inputTable, schemaIpc, arrayPtrs, schemaPtrs);
            } finally {
                for (ArrowArray a : arrays) {
                    a.close();
                }
                for (ArrowSchema s : schemas) {
                    s.close();
                }
                for (VectorSchemaRoot b : batches) {
                    b.close();
                }
            }

            long streamPtr = NativeBridge.executeLocalPlan(session.getPointer(), substrait);
            return drainStream(streamPtr);
        }
    }

    private List<Object[]> drainStream(long streamPtr) throws Exception {
        List<Object[]> rows = new ArrayList<>();
        try (
            StreamHandle streamHandle = new StreamHandle(streamPtr, runtime);
            CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()
        ) {
            long schemaAddr = asyncCall(listener -> NativeBridge.streamGetSchema(streamHandle.getPointer(), listener));
            Schema outSchema = new Schema(
                importField(allocator, ArrowSchema.wrap(schemaAddr), dictProvider).getChildren(),
                null
            );
            try (VectorSchemaRoot root = VectorSchemaRoot.create(outSchema, allocator)) {
                while (true) {
                    long arrayAddr = asyncCall(
                        listener -> NativeBridge.streamNext(runtime.get(), streamHandle.getPointer(), listener)
                    );
                    if (arrayAddr == 0) {
                        break;
                    }
                    Data.importIntoVectorSchemaRoot(allocator, ArrowArray.wrap(arrayAddr), root, dictProvider);
                    int cols = root.getFieldVectors().size();
                    for (int r = 0; r < root.getRowCount(); r++) {
                        Object[] row = new Object[cols];
                        for (int c = 0; c < cols; c++) {
                            row[c] = root.getFieldVectors().get(c).getObject(r);
                        }
                        rows.add(row);
                    }
                }
            }
        }
        return rows;
    }

    private long asyncCall(java.util.function.Consumer<ActionListener<Long>> call) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        call.accept(new ActionListener<>() {
            @Override
            public void onResponse(Long v) {
                future.complete(v);
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
            }
        });
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for native callback", e);
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new AssertionError("native callback failed", cause);
        }
    }

    @Override
    public void close() {
        try {
            allocator.close();
        } finally {
            runtime.close();
        }
    }
}
