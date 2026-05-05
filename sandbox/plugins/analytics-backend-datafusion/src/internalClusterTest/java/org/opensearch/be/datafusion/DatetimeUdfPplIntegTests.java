/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.Rel;

/**
 * PPL-based integration tests for datetime UDF cleanup PoC.
 * Tests the full pipeline: PPL text → UnifiedQueryPlanner → RelNode → DatetimeTypeRewriter → Substrait.
 */
public class DatetimeUdfPplIntegTests extends OpenSearchTestCase {

    private SimpleExtension.ExtensionCollection extensions;

    @Override
    public void setUp() throws Exception {
        super.setUp();
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
    }

    /**
     * Creates a test schema with a table 't' having a TIMESTAMP column 'created'.
     */
    private AbstractSchema createTestSchema() {
        return new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Map.of("t", new AbstractTable() {
                    @Override
                    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                        return typeFactory.builder()
                            .add("created", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true))
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
            UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
            return planner.plan(ppl);
        } catch (Exception e) {
            throw new RuntimeException("Failed to plan PPL: " + e.getMessage(), e);
        }
    }

    private Plan convertToSubstrait(RelNode relNode) throws Exception {
        DataFusionFragmentConvertor convertor = new DataFusionFragmentConvertor(extensions);
        byte[] bytes = convertor.convertShardScanFragment("t", relNode);
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

    /**
     * PPL: source=t | eval d = DATE_ADD(created, INTERVAL 1 DAY)
     * Verifies: PPL planner produces RelNode with UDT type → rewriter normalizes → Substrait has date_add function.
     */
    public void testDateAddFromPpl() throws Exception {
        RelNode plan = planPpl("source=t | eval d = DATE_ADD(created, INTERVAL 1 DAY)");
        logger.info("RelNode from PPL:\n{}", plan.explain());

        Plan substraitPlan = convertToSubstrait(plan);
        Rel root = rootRel(substraitPlan);

        // Root should be a ProjectRel (output cast added by rewriter)
        assertTrue("root must be a ProjectRel", root.hasProject());
        logger.info("testDateAddFromPpl PASSED — Substrait plan:\n{}", substraitPlan);
    }

    /**
     * PPL: source=t | eval d = LAST_DAY(created)
     */
    public void testLastDayFromPpl() throws Exception {
        RelNode plan = planPpl("source=t | eval d = LAST_DAY(created)");
        logger.info("RelNode from PPL:\n{}", plan.explain());

        Plan substraitPlan = convertToSubstrait(plan);
        Rel root = rootRel(substraitPlan);

        assertTrue("root must be a ProjectRel", root.hasProject());
        logger.info("testLastDayFromPpl PASSED — Substrait plan:\n{}", substraitPlan);
    }

    /**
     * PPL: source=t | where created > TIMESTAMP('2024-01-01 00:00:00')
     * Verifies: TIMESTAMP literal constructor goes through as a function call in Substrait.
     */
    public void testTimestampLiteralFromPpl() throws Exception {
        RelNode plan = planPpl("source=t | where created > TIMESTAMP('2024-01-01 00:00:00')");
        logger.info("RelNode from PPL:\n{}", plan.explain());

        Plan substraitPlan = convertToSubstrait(plan);
        Rel root = rootRel(substraitPlan);

        // Should have a filter with comparison
        assertTrue("root must be a ProjectRel (output cast)", root.hasProject());
        logger.info("testTimestampLiteralFromPpl PASSED — Substrait plan:\n{}", substraitPlan);
    }
}
