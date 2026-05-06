/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.Expression;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.Rel;

/**
 * End-to-end tests for PPL datetime UDF cleanup PoC.
 * Verifies: RelNode with datetime UDFs → DatetimeTypeRewriter → Substrait conversion.
 * The DatetimeTypeRewriter is applied inside convertShardScanFragment automatically.
 */
public class DatetimeUdfE2ETests extends OpenSearchTestCase {

    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private SimpleExtension.ExtensionCollection extensions;

    // Datetime function operators (matching what PPL planner would produce)
    private static final SqlFunction DATE_ADD_OP = new SqlFunction(
        "DATE_ADD", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0_NULLABLE,
        null, OperandTypes.ANY_ANY, SqlFunctionCategory.TIMEDATE
    );
    private static final SqlFunction LAST_DAY_OP = new SqlFunction(
        "LAST_DAY", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0_NULLABLE,
        null, OperandTypes.ANY, SqlFunctionCategory.TIMEDATE
    );
    private static final SqlFunction TIMESTAMP_OP = new SqlFunction(
        "TIMESTAMP", SqlKind.OTHER_FUNCTION, ReturnTypes.TIMESTAMP_NULLABLE,
        null, OperandTypes.STRING, SqlFunctionCategory.TIMEDATE
    );
    private static final SqlFunction NOW_OP = new SqlFunction(
        "NOW", SqlKind.OTHER_FUNCTION, ReturnTypes.TIMESTAMP_NULLABLE,
        null, OperandTypes.NILADIC, SqlFunctionCategory.TIMEDATE
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        cluster = RelOptCluster.create(planner, rexBuilder);

        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(DatetimeUdfE2ETests.class.getClassLoader());
            SimpleExtension.ExtensionCollection customExtensions = SimpleExtension.load(List.of(
                "/delegation_functions.yaml",
                "/functions_opensearch_datetime.yaml"
            ));
            extensions = DefaultExtensionCatalog.DEFAULT_COLLECTION.merge(customExtensions);
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    private DataFusionFragmentConvertor newConvertor() {
        return new DataFusionFragmentConvertor(extensions);
    }

    /** Builds a row type with a single nullable TIMESTAMP column. */
    private RelDataType timestampRowType(String columnName) {
        return typeFactory.builder()
            .add(columnName, typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true))
            .build();
    }

    private Plan decodeSubstrait(byte[] bytes) throws Exception {
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
        return Plan.parseFrom(bytes);
    }

    private Rel rootRel(Plan plan) {
        assertFalse(plan.getRelationsList().isEmpty());
        PlanRel planRel = plan.getRelationsList().get(0);
        assertTrue(planRel.hasRoot());
        return planRel.getRoot().getInput();
    }

    // ── Tests ──────────────────────────────────────────────────────────────────

    /**
     * Project(created, DATE_ADD(created, INTERVAL 1 DAY)) → Substrait with date_add function.
     * Verifies the DatetimeTypeRewriter replaces UDT type and the function maps to Substrait.
     */
    public void testDateAddSubstraitConversion() throws Exception {
        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(
            cluster, cluster.traitSet(), "t", timestampRowType("created")
        );

        // Simulate UDT return type (VARCHAR) — this is what PPL planner produces
        RelDataType udtReturnType = typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.VARCHAR), true
        );
        RexNode createdRef = rexBuilder.makeInputRef(scan, 0);
        // Use an integer literal as interval placeholder (1 day = 86400000 ms).
        // The actual interval type handling is tested at the DataFusion UDF level.
        RexNode intervalLiteral = rexBuilder.makeLiteral(
            86400000, typeFactory.createSqlType(SqlTypeName.INTEGER), true
        );
        // DATE_ADD call with UDT (VARCHAR) return type — simulates what PPL produces
        RexNode dateAddCall = rexBuilder.makeCall(udtReturnType, DATE_ADD_OP, List.of(createdRef, intervalLiteral));

        RelNode project = LogicalProject.create(scan, List.of(), List.of(createdRef, dateAddCall), List.of("created", "d"));

        // convertShardScanFragment applies DatetimeTypeRewriter internally
        byte[] bytes = newConvertor().convertShardScanFragment("t", project);
        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);

        // The root should be a ProjectRel (the output cast project added by rewriter)
        assertTrue("root must be a ProjectRel (output cast)", root.hasProject());
        ProjectRel outerProject = root.getProject();

        // The outer project should contain CAST expressions (cast to varchar)
        // At least one expression should be a cast
        boolean hasCast = false;
        for (Expression expr : outerProject.getExpressionsList()) {
            if (expr.hasCast()) {
                hasCast = true;
                break;
            }
        }
        assertTrue("Output project must contain CAST to VARCHAR", hasCast);

        // The inner project should contain the date_add scalar function
        Rel innerRel = outerProject.getInput();
        assertTrue("inner must be a ProjectRel", innerRel.hasProject());
        ProjectRel innerProject = innerRel.getProject();

        boolean hasDateAdd = false;
        for (Expression expr : innerProject.getExpressionsList()) {
            if (expr.hasScalarFunction()) {
                hasDateAdd = true;
                break;
            }
        }
        assertTrue("Inner project must contain date_add scalar function", hasDateAdd);

        logger.info("testDateAddSubstraitConversion PASSED — Substrait plan:\n{}", plan);
    }

    /**
     * Project(LAST_DAY(created)) → Substrait with last_day function.
     */
    public void testLastDaySubstraitConversion() throws Exception {
        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(
            cluster, cluster.traitSet(), "t", timestampRowType("created")
        );

        RelDataType udtReturnType = typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.VARCHAR), true
        );
        RexNode createdRef = rexBuilder.makeInputRef(scan, 0);
        RexNode lastDayCall = rexBuilder.makeCall(udtReturnType, LAST_DAY_OP, List.of(createdRef));

        RelNode project = LogicalProject.create(scan, List.of(), List.of(lastDayCall), List.of("d"));

        byte[] bytes = newConvertor().convertShardScanFragment("t", project);
        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);

        assertTrue("root must be a ProjectRel", root.hasProject());
        // Verify the plan contains a scalar function (last_day)
        boolean hasScalarFunc = false;
        ProjectRel outerProject = root.getProject();
        // Check inner project for last_day
        if (outerProject.getInput().hasProject()) {
            for (Expression expr : outerProject.getInput().getProject().getExpressionsList()) {
                if (expr.hasScalarFunction()) {
                    hasScalarFunc = true;
                    break;
                }
            }
        }
        assertTrue("Plan must contain last_day scalar function", hasScalarFunc);
        logger.info("testLastDaySubstraitConversion PASSED — Substrait plan:\n{}", plan);
    }

    /**
     * Project(TIMESTAMP('2024-01-01 00:00:00')) → Substrait with timestamp_parse function.
     */
    public void testTimestampParseSubstraitConversion() throws Exception {
        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(
            cluster, cluster.traitSet(), "t", timestampRowType("created")
        );

        RelDataType udtReturnType = typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.VARCHAR), true
        );
        RexNode literal = rexBuilder.makeLiteral("2024-01-01 00:00:00");
        RexNode timestampCall = rexBuilder.makeCall(udtReturnType, TIMESTAMP_OP, List.of(literal));

        RelNode project = LogicalProject.create(scan, List.of(), List.of(timestampCall), List.of("ts"));

        byte[] bytes = newConvertor().convertShardScanFragment("t", project);
        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);

        assertTrue("root must be a ProjectRel", root.hasProject());
        logger.info("testTimestampParseSubstraitConversion PASSED — Substrait plan:\n{}", plan);
    }

    /**
     * Verifies that the DatetimeTypeRewriter adds a final CAST-to-VARCHAR project
     * for datetime output columns.
     */
    public void testFinalProjectCastsDatetimeToVarchar() throws Exception {
        // A simple scan with TIMESTAMP column — the rewriter should add CAST(created AS VARCHAR)
        RelNode scan = new DataFusionFragmentConvertor.StageInputTableScan(
            cluster, cluster.traitSet(), "t", timestampRowType("created")
        );

        // Just pass the scan through (no explicit project) — the rewriter should still
        // add a final project with CAST since the output has a TIMESTAMP field
        byte[] bytes = newConvertor().convertShardScanFragment("t", scan);
        Plan plan = decodeSubstrait(bytes);
        Rel root = rootRel(plan);

        // Root should be a ProjectRel with a CAST expression
        assertTrue("root must be a ProjectRel (output cast)", root.hasProject());
        ProjectRel projectRel = root.getProject();
        boolean hasCast = false;
        for (Expression expr : projectRel.getExpressionsList()) {
            if (expr.hasCast()) {
                hasCast = true;
                // Verify the cast target type is string/varchar
                assertTrue("Cast must target string type",
                    expr.getCast().getType().hasString() || expr.getCast().getType().hasVarchar());
                break;
            }
        }
        assertTrue("Output project must contain CAST to VARCHAR for timestamp column", hasCast);
        logger.info("testFinalProjectCastsDatetimeToVarchar PASSED — Substrait plan:\n{}", plan);
    }
}
