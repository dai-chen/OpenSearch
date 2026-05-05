/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * True end-to-end UDF test: PPL text → RelNode → Substrait → native DataFusion → result rows.
 *
 * <p>Uses {@link DataFusionTestHarness} to bypass the cluster-aware executor pipeline
 * ({@code DefaultPlanExecutor} → {@code DAGBuilder} → {@code Scheduler}) and drive the native
 * runtime directly with an in-memory table. See the harness javadoc for why this is the right
 * level for a UDF unit test.
 */
public class DatetimeUdfE2ETests extends OpenSearchTestCase {

    private static final String TABLE = "t";
    private static final long INPUT_MILLIS = Instant.parse("2024-01-01T00:00:00Z").toEpochMilli();
    private static final long EXPECTED_MILLIS = Instant.parse("2024-01-02T00:00:00Z").toEpochMilli();

    private DataFusionTestHarness harness;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        harness = DataFusionTestHarness.open(createTempDir("datafusion-spill"));
    }

    @Override
    public void tearDown() throws Exception {
        harness.close();
        super.tearDown();
    }

    public void testDateAddEndToEnd() throws Exception {
        // 1. PPL → RelNode
        String ppl = "source=t | eval d = DATE_ADD(created, INTERVAL 1 DAY)";
        RelNode plan = planPpl(ppl);
        logger.info("PPL: {}", ppl);
        logger.info("RelNode rowType: {}", plan.getRowType());

        // 1b. WORKAROUND: unified-query-core's CalciteRexNodeVisitor.visitInterval stores the raw
        //     interval value (1) instead of Calcite's canonical milliseconds encoding (86_400_000
        //     for 1 DAY). Isthmus reads the field as milliseconds, so without rewriting we get
        //     `created + 1ms` instead of `created + 1 day`. Remove once unified-query-core is
        //     patched to call SqlParserUtil.intervalToMillis before makeIntervalLiteral.
        plan = rewriteIntervalLiteralsToMillis(plan);

        // 2. RelNode → Substrait bytes
        byte[] substrait = new DataFusionFragmentConvertor(harness.extensions()).convertShardScanFragment(TABLE, plan);

        // 3. Run it: build a one-row input batch using the harness's shared allocator (Arrow C
        //    Data export requires the export allocator to be the root of the batch's allocator
        //    tree), then execute via the harness.
        Schema arrowSchema = new Schema(
            List.of(new Field("created", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null))
        );
        VectorSchemaRoot batch = VectorSchemaRoot.create(arrowSchema, harness.allocator());
        batch.allocateNew();
        TimeStampMilliVector v = (TimeStampMilliVector) batch.getVector(0);
        v.setSafe(0, INPUT_MILLIS);
        v.setValueCount(1);
        batch.setRowCount(1);

        List<Object[]> rows = harness.execute(TABLE, arrowSchema, List.of(batch), substrait);

        // 4. Assert on actual result values
        logger.info("rows returned: {}", rows.size());
        for (Object[] row : rows) {
            logger.info("  row: created={} d={}", row[0], row[1]);
        }
        assertEquals("one row expected", 1, rows.size());
        assertEquals("created passes through unchanged", INPUT_MILLIS, toEpochMillis(rows.get(0)[0]));
        assertEquals("d = created + 1 day", EXPECTED_MILLIS, toEpochMillis(rows.get(0)[1]));
    }

    /**
     * Diagnostic companion to {@link #testDateAddEndToEnd()}: drives another common datetime UDF
     * ({@code LAST_DAY}) combined with a WHERE-clause UDT comparison. This exercises the full
     * mapping pipeline: the PPL planner wraps the call as {@code TIMESTAMP(LAST_DAY($0))}, so
     * conversion hits two Calcite operators that need registration.
     *
     * <p>Today {@code LAST_DAY} IS registered (via the custom
     * {@code functions_opensearch_datetime.yaml} + a {@code FunctionMappings.Sig} in
     * {@code DataFusionFragmentConvertor}), so isthmus now successfully converts the
     * {@code LAST_DAY} call. The next unmapped call in the RelNode is the outer
     * {@code TIMESTAMP(string?)} cast wrapper — that's where isthmus now throws.
     *
     * <p>Uses {@link #expectThrows} so the test stays green while the gap exists — the actual
     * exception message is logged under {@code LAST_DAY_EXPECTED_FAILURE} so each test run
     * records which call is still unmapped. When all calls are wired (and backed by matching
     * DataFusion UDFs on the native side), promote this to a real end-to-end assertion.
     */
    public void testLastDayExpectedFailureDiagnostic() throws Exception {
        String ppl = "source=t | where created < LAST_DAY(created)";
        RelNode plan = planPpl(ppl);
        logger.info("PPL: {}", ppl);
        logger.info("RelNode:\n{}", org.apache.calcite.plan.RelOptUtil.toString(plan));

        DataFusionFragmentConvertor convertor = new DataFusionFragmentConvertor(harness.extensions());
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> convertor.convertShardScanFragment(TABLE, plan)
        );
        logger.info("LAST_DAY_EXPECTED_FAILURE: {}", ex.getMessage());
        assertTrue(
            "expected 'Unable to convert call X(...)' pinpointing the next unmapped operator, got: " + ex.getMessage(),
            ex.getMessage() != null && ex.getMessage().startsWith("Unable to convert call")
        );
    }

    /**
     * Contrast test: pins down which input shapes trigger the PPL planner's implicit
     * TIMESTAMP(...) cast wrapping. Empirically:
     * <ul>
     *   <li>{@code where created < DATE_ADD(created, INTERVAL 1 DAY)} — NO wrap. DATE_ADD's
     *       declared return type is {@code EXPR_TIMESTAMP}, same {@code ExprCoreType.TIMESTAMP}
     *       family as the compared column after coercion — no widening needed.</li>
     *   <li>{@code where created < LAST_DAY(created)} — wrap. LAST_DAY returns
     *       {@code EXPR_DATE}, which is a different {@code ExprCoreType.DATE} family;
     *       {@code PPLFuncImpTable.resolveWithCoercion} widens DATE → TIMESTAMP by calling
     *       {@code ExtendedRexBuilder.makeCast(EXPR_TIMESTAMP_UDT, ...)}, which is overridden
     *       to emit {@code makeCall(PPLBuiltinOperators.TIMESTAMP, ...)} instead of a standard
     *       SQL CAST. That's the {@code TIMESTAMP(...)} we see.</li>
     * </ul>
     * So the cast is triggered by TYPE WIDENING across DATE ↔ TIMESTAMP UDT families in a
     * comparison, not by "any UDT return in any comparison" — and it shows up as a
     * {@link org.opensearch.sql.expression.function.PPLBuiltinOperators#TIMESTAMP} UDF call
     * because {@code ExtendedRexBuilder} re-routes UDT casts to PPL-specific UDFs.
     */
    public void testCompareWithDateAddHasNoCastWrapping() throws Exception {
        String ppl = "source=t | where created < DATE_ADD(created, INTERVAL 1 DAY)";
        RelNode plan = planPpl(ppl);
        String planStr = org.apache.calcite.plan.RelOptUtil.toString(plan);
        logger.info("PPL: {}", ppl);
        logger.info("RelNode:\n{}", planStr);
        // DATE_ADD returns EXPR_TIMESTAMP — same family as `created`, so no widening cast is
        // inserted. The raw DATE_ADD call appears directly in the filter condition.
        assertTrue(
            "expected no TIMESTAMP cast wrapping around DATE_ADD (same-family comparison), got:\n" + planStr,
            planStr.contains("DATE_ADD(") && !planStr.contains("TIMESTAMP(DATE_ADD(")
        );
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static RelNode planPpl(String ppl) throws Exception {
        try (
            UnifiedQueryContext ctx = UnifiedQueryContext.builder()
                .language(QueryType.PPL)
                .catalog("opensearch", buildCalciteSchema())
                .defaultNamespace("opensearch")
                .build()
        ) {
            return new UnifiedQueryPlanner(ctx).plan(ppl);
        }
    }

    /** Single-table Calcite schema with one nullable TIMESTAMP column {@code created}. */
    private static AbstractSchema buildCalciteSchema() {
        Table table = new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();
                builder.add(
                    "created",
                    typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true)
                );
                return builder.build();
            }
        };
        Map<String, Table> tables = new HashMap<>();
        tables.put(TABLE, table);
        return new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tables;
            }
        };
    }

    /**
     * Rewrites Calcite interval day-time literals whose raw value is in "units" (as produced by
     * the PPL parser) into the milliseconds encoding that Calcite's SQL parser uses and isthmus
     * expects.
     */
    private static RelNode rewriteIntervalLiteralsToMillis(RelNode node) {
        RexBuilder rexBuilder = node.getCluster().getRexBuilder();
        RexShuttle shuttle = new RexShuttle() {
            @Override
            public RexNode visitLiteral(RexLiteral literal) {
                RelDataType type = literal.getType();
                SqlTypeName sql = type.getSqlTypeName();
                if (sql == null || !SqlTypeName.DAY_INTERVAL_TYPES.contains(sql)) {
                    return literal;
                }
                long raw = ((BigDecimal) literal.getValue()).longValue();
                long multiplier = switch (sql) {
                    case INTERVAL_DAY, INTERVAL_DAY_HOUR -> 86_400_000L;
                    case INTERVAL_HOUR -> 3_600_000L;
                    case INTERVAL_MINUTE -> 60_000L;
                    case INTERVAL_SECOND -> 1_000L;
                    default -> 1L;
                };
                return rexBuilder.makeIntervalLiteral(BigDecimal.valueOf(raw * multiplier), type.getIntervalQualifier());
            }
        };
        return node.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(LogicalProject project) {
                return super.visit(project).accept(shuttle);
            }
        });
    }

    /** Coerces Arrow's {@code getObject()} timestamp variants to epoch millis. */
    private static long toEpochMillis(Object o) {
        if (o instanceof Long l) {
            return l;
        }
        if (o instanceof LocalDateTime ldt) {
            return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
        }
        throw new AssertionError("unexpected timestamp repr: " + (o == null ? "null" : o.getClass().getName() + " / " + o));
    }
}
