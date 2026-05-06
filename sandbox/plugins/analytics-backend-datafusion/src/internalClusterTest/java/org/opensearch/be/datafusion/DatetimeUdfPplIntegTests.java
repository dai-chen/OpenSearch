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
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
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

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;

import static org.apache.arrow.c.Data.importField;

/**
 * End-to-end PPL integration tests for datetime UDF cleanup PoC.
 * Full pipeline: PPL text → RelNode → DatetimeTypeRewriter → Substrait → DataFusion native execution → results.
 */
public class DatetimeUdfPplIntegTests extends OpenSearchTestCase {

    // 2024-01-15T10:30:00Z in epoch millis
    private static final long INPUT_MILLIS = 1705312200000L;

    private SimpleExtension.ExtensionCollection extensions;
    private NativeRuntimeHandle runtimeHandle;
    private RootAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Load Substrait extensions
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(DatetimeUdfPplIntegTests.class.getClassLoader());
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

    private AbstractSchema createTestSchema() {
        return new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Map.of("t", new AbstractTable() {
                    @Override
                    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                        return typeFactory.builder()
                            .add("created", typeFactory.createTypeWithNullability(
                                typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true))
                            .build();
                    }
                });
            }
        };
    }

    private RelNode planPpl(String ppl) {
        try (UnifiedQueryContext context = UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog("opensearch", createTestSchema())
            .defaultNamespace("opensearch")
            .build()) {
            return new UnifiedQueryPlanner(context).plan(ppl);
        } catch (Exception e) {
            throw new RuntimeException("Failed to plan PPL: " + e.getMessage(), e);
        }
    }

    private byte[] toSubstraitBytes(RelNode relNode) {
        // The PPL planner produces LogicalTableScan with qualified name ["catalog", "table"].
        // DataFusion's local session expects just ["table"]. We pass the RelNode through
        // convertShardScanFragment which uses StageInputTableScan internally for the leaf.
        // For this test, we replace the leaf TableScan with a StageInputTableScan.
        RelNode leaf = relNode;
        while (leaf.getInputs() != null && leaf.getInputs().isEmpty() == false) {
            leaf = leaf.getInput(0);
        }
        // Build a StageInputTableScan with the same row type as the original scan
        RelNode replacement = new DataFusionFragmentConvertor.StageInputTableScan(
            leaf.getCluster(), leaf.getTraitSet(), "t", leaf.getRowType()
        );
        // Replace the leaf in the plan by rebuilding from bottom up
        RelNode rewritten = replaceLeafScan(relNode, replacement);

        logger.info("══════════════════════════════════════════════════════════════");
        logger.info("  [STEP 2] RelNode after table scan normalization:");
        logger.info("══════════════════════════════════════════════════════════════\n{}", rewritten.explain());

        DataFusionFragmentConvertor convertor = new DataFusionFragmentConvertor(extensions);
        // convertShardScanFragment internally calls DatetimeTypeRewriter.rewrite() then Substrait conversion
        byte[] substrait = convertor.convertShardScanFragment("t", rewritten);

        logger.info("══════════════════════════════════════════════════════════════");
        logger.info("  [STEP 3] Substrait plan produced: {} bytes", substrait.length);
        logger.info("══════════════════════════════════════════════════════════════");
        return substrait;
    }

    /** Recursively replace the leaf TableScan with the given replacement. */
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

    /**
     * Execute Substrait plan in DataFusion with a memtable containing one timestamp row.
     */
    private List<Object[]> executeInDataFusion(byte[] substraitBytes) throws Exception {
        Schema arrowSchema = new Schema(List.of(
            new Field("created", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null)
        ));

        // Build test batch: one row with INPUT_MILLIS
        VectorSchemaRoot batch = VectorSchemaRoot.create(arrowSchema, allocator);
        batch.allocateNew();
        TimeStampMilliVector col = (TimeStampMilliVector) batch.getVector(0);
        col.setSafe(0, INPUT_MILLIS);
        col.setValueCount(1);
        batch.setRowCount(1);

        // Create session and register memtable
        DatafusionLocalSession session = new DatafusionLocalSession(runtimeHandle.get());
        try {
            // Export batch via Arrow C Data
            try (ArrowArray array = ArrowArray.allocateNew(allocator);
                 ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
                Data.exportVectorSchemaRoot(allocator, batch, null, array, schema);
                NativeBridge.registerMemtable(
                    session.getPointer(), "t", schemaIpc(arrowSchema),
                    new long[]{array.memoryAddress()}, new long[]{schema.memoryAddress()}
                );
            }
            batch.close();

            // Execute Substrait plan
            long streamPtr = NativeBridge.executeLocalPlan(session.getPointer(), substraitBytes);
            assertTrue("stream ptr must be non-zero", streamPtr != 0);

            // Read results
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

    // ── Tests ──────────────────────────────────────────────────────────────────

    /**
     * PPL: source=t | eval d = DATE_ADD(created, INTERVAL 1 DAY)
     * Verifies full E2E: PPL → RelNode → Substrait → DataFusion → results.
     * Expected: created = "2024-01-15..." (string), d = "2024-01-16..." (string, +1 day)
     */
    public void testDateAddEndToEnd() throws Exception {
        String ppl = "source=t | eval d = DATE_ADD(created, INTERVAL 1 DAY)";
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
        assertEquals("Expected 1 row", 1, rows.size());
        Object[] row = rows.get(0);
        logger.info("    created = {} ({})", row[0], row[0].getClass().getSimpleName());
        logger.info("    d       = {} ({})", row[1], row[1].getClass().getSimpleName());
        logger.info("══════════════════════════════════════════════════════════════");
        assertNotNull("created must not be null", row[0]);
        assertNotNull("d must not be null", row[1]);
    }

    /**
     * PPL: source=t | eval d = LAST_DAY(created)
     * Expected: d = "2024-01-31..." (last day of January 2024)
     */
    public void testLastDayEndToEnd() throws Exception {
        String ppl = "source=t | eval d = LAST_DAY(created)";
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
        assertEquals("Expected 1 row", 1, rows.size());
        Object[] row = rows.get(0);
        logger.info("    created = {} ({})", row[0], row[0].getClass().getSimpleName());
        logger.info("    d       = {} ({})", row[1], row[1].getClass().getSimpleName());
        logger.info("══════════════════════════════════════════════════════════════");
        assertNotNull("d must not be null", row[1]);
        assertTrue("d should contain 2024-01-31", row[1].toString().contains("2024-01-31"));
    }

    /**
     * PPL: source=t | where created > TIMESTAMP('2020-01-01 00:00:00')
     * Expected: 1 row returned (2024-01-15 > 2020-01-01)
     */
    public void testTimestampFilterEndToEnd() throws Exception {
        String ppl = "source=t | where created > TIMESTAMP('2020-01-01 00:00:00')";
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
        assertEquals("Expected 1 row (filter passes)", 1, rows.size());
        logger.info("    created = {} ({})", rows.get(0)[0], rows.get(0)[0].getClass().getSimpleName());
        logger.info("══════════════════════════════════════════════════════════════");
        assertNotNull("created must not be null", rows.get(0)[0]);
    }
}
