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
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;

import static org.apache.arrow.c.Data.importField;

/**
 * Base class for PPL → DataFusion E2E tests.
 *
 * <p>Provides infrastructure for the full pipeline:
 * PPL text → UnifiedQueryPlanner → RelNode → DatetimeTypeRewriter → Substrait → DataFusion → results.
 *
 * <p>Subclasses define the table schema and test data, then use {@link #executePpl(String)} to
 * run a PPL query end-to-end and get results back.
 */
public abstract class PplDataFusionTestBase extends OpenSearchTestCase {

    private SimpleExtension.ExtensionCollection extensions;
    private NativeRuntimeHandle runtimeHandle;
    protected RootAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Load Substrait extensions
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(getClass().getClassLoader());
            SimpleExtension.ExtensionCollection customExtensions = SimpleExtension.load(
                List.of("/delegation_functions.yaml", "/functions_opensearch_datetime.yaml")
            );
            extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION.merge(customExtensions);
        } finally {
            t.setContextClassLoader(prev);
        }

        // Initialize native runtime
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        runtimeHandle = new NativeRuntimeHandle(
            NativeBridge.createGlobalRuntime(128 * 1024 * 1024, 0L, spillDir.toString(), 64 * 1024 * 1024)
        );
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        runtimeHandle.close();
        super.tearDown();
    }

    // ── Abstract methods for subclasses ─────────────────────────────────────────

    /** Table name used in PPL queries (e.g., "t"). */
    protected abstract String tableName();

    /** Calcite schema defining the table(s) available to PPL queries. */
    protected abstract AbstractSchema calciteSchema();

    /** Arrow schema matching the Calcite schema for the memtable. */
    protected abstract Schema arrowSchema();

    /** Build and return the Arrow batch(es) to register as the memtable. */
    protected abstract VectorSchemaRoot createTestBatch();

    // ── Public API for subclasses ───────────────────────────────────────────────

    /**
     * Execute a PPL query end-to-end: PPL → RelNode → Substrait → DataFusion → results.
     */
    protected List<Object[]> executePpl(String ppl) throws Exception {
        logger.info("══════════════════════════════════════════════════════════════");
        logger.info("  [STEP 1] PPL Query: {}", ppl);
        logger.info("══════════════════════════════════════════════════════════════");

        RelNode plan = planPpl(ppl);
        logger.info("  [STEP 1] RelNode from UnifiedQueryPlanner (with UDT types):\n{}", plan.explain());

        byte[] substrait = toSubstraitBytes(plan);

        logger.info("  [STEP 4] Executing Substrait plan in DataFusion...");
        List<Object[]> rows = executeInDataFusion(substrait);

        logger.info("══════════════════════════════════════════════════════════════");
        logger.info("  [STEP 5] DataFusion Results: {} row(s)", rows.size());
        for (int i = 0; i < rows.size(); i++) {
            Object[] row = rows.get(i);
            StringBuilder sb = new StringBuilder("    row[").append(i).append("] = {");
            for (int c = 0; c < row.length; c++) {
                if (c > 0) sb.append(", ");
                sb.append(row[c]).append(" (").append(row[c] == null ? "null" : row[c].getClass().getSimpleName()).append(")");
            }
            sb.append("}");
            logger.info(sb.toString());
        }
        logger.info("══════════════════════════════════════════════════════════════");
        return rows;
    }

    // ── Internal helpers ────────────────────────────────────────────────────────

    private RelNode planPpl(String ppl) {
        try (UnifiedQueryContext context = UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog("opensearch", calciteSchema())
            .defaultNamespace("opensearch")
            .build()) {
            return new UnifiedQueryPlanner(context).plan(ppl);
        } catch (Exception e) {
            throw new RuntimeException("Failed to plan PPL: " + e.getMessage(), e);
        }
    }

    private byte[] toSubstraitBytes(RelNode relNode) {
        RelNode leaf = relNode;
        while (leaf.getInputs() != null && leaf.getInputs().isEmpty() == false) {
            leaf = leaf.getInput(0);
        }
        RelNode replacement = new DataFusionFragmentConvertor.StageInputTableScan(
            leaf.getCluster(), leaf.getTraitSet(), tableName(), leaf.getRowType()
        );
        RelNode rewritten = replaceLeafScan(relNode, replacement);

        logger.info("══════════════════════════════════════════════════════════════");
        logger.info("  [STEP 2] RelNode after table scan normalization:");
        logger.info("══════════════════════════════════════════════════════════════\n{}", rewritten.explain());

        DataFusionFragmentConvertor convertor = new DataFusionFragmentConvertor(extensions);
        byte[] substrait = convertor.convertShardScanFragment(tableName(), rewritten);

        logger.info("══════════════════════════════════════════════════════════════");
        logger.info("  [STEP 3] Substrait plan produced: {} bytes", substrait.length);
        logger.info("══════════════════════════════════════════════════════════════");
        return substrait;
    }

    private RelNode replaceLeafScan(RelNode node, RelNode replacement) {
        if (node.getInputs().isEmpty()) {
            return replacement;
        }
        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : node.getInputs()) {
            newInputs.add(replaceLeafScan(input, replacement));
        }
        return node.copy(node.getTraitSet(), newInputs);
    }

    private List<Object[]> executeInDataFusion(byte[] substraitBytes) throws Exception {
        VectorSchemaRoot batch = createTestBatch();
        DatafusionLocalSession session = new DatafusionLocalSession(runtimeHandle.get());
        try {
            try (ArrowArray array = ArrowArray.allocateNew(allocator);
                 ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
                Data.exportVectorSchemaRoot(allocator, batch, null, array, schema);
                NativeBridge.registerMemtable(
                    session.getPointer(), tableName(), schemaIpc(arrowSchema()),
                    new long[]{array.memoryAddress()}, new long[]{schema.memoryAddress()}
                );
            }
            batch.close();

            long streamPtr = NativeBridge.executeLocalPlan(session.getPointer(), substraitBytes);
            assertTrue("stream ptr must be non-zero", streamPtr != 0);

            try (StreamHandle streamHandle = new StreamHandle(streamPtr, runtimeHandle);
                 CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()) {

                long schemaAddr = asyncCall(
                    listener -> NativeBridge.streamGetSchema(streamHandle.getPointer(), listener)
                );
                Schema outSchema = new Schema(
                    importField(allocator, ArrowSchema.wrap(schemaAddr), dictProvider).getChildren(), null
                );

                List<Object[]> rows = new ArrayList<>();
                try (VectorSchemaRoot root = VectorSchemaRoot.create(outSchema, allocator)) {
                    while (true) {
                        long arrayAddr = asyncCall(
                            listener -> NativeBridge.streamNext(runtimeHandle.get(), streamHandle.getPointer(), listener)
                        );
                        if (arrayAddr == 0) break;
                        Data.importIntoVectorSchemaRoot(allocator, ArrowArray.wrap(arrayAddr), root, dictProvider);
                        for (int r = 0; r < root.getRowCount(); r++) {
                            Object[] row = new Object[root.getFieldVectors().size()];
                            for (int c = 0; c < row.length; c++) {
                                row[c] = root.getFieldVectors().get(c).getObject(r);
                            }
                            rows.add(row);
                        }
                    }
                }
                return rows;
            }
        } finally {
            session.close();
        }
    }

    private static byte[] schemaIpc(Schema schema) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (WriteChannel channel = new WriteChannel(Channels.newChannel(baos))) {
            MessageSerializer.serialize(channel, schema);
        }
        return baos.toByteArray();
    }

    private long asyncCall(java.util.function.Consumer<ActionListener<Long>> call) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        call.accept(new ActionListener<>() {
            @Override
            public void onResponse(Long v) { future.complete(v); }
            @Override
            public void onFailure(Exception e) { future.completeExceptionally(e); }
        });
        return future.join();
    }
}
