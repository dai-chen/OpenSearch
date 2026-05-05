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

import java.util.HashMap;
import java.util.Map;

import io.substrait.extension.SimpleExtension;

/**
 * End-to-end probe: PPL text → RelNode → Substrait proto bytes.
 *
 * <p>Drives the same Substrait-conversion step that {@code DefaultPlanExecutor}
 * invokes synchronously inside {@code execute()} via {@code FragmentConversionDriver},
 * bypassing the executor boilerplate (TransportService, ClusterService, NodeClient)
 * that can't be constructed in a unit test.
 *
 * <p>The test does NOT catch or wrap exceptions — any failure from either
 * {@link UnifiedQueryPlanner#plan(String)} or
 * {@link DataFusionFragmentConvertor#convertShardScanFragment(String, RelNode)}
 * propagates with full stack trace into the test report.
 *
 * <p>Expected failure site: the isthmus {@code SubstraitRelVisitor.apply()} call
 * inside {@code DataFusionFragmentConvertor.convertToSubstrait()}, triggered by
 * the PPL datetime UDF's return type — {@code ExprSqlType} with
 * {@code sqlTypeName=VARCHAR} but {@code typeString='EXPR_TIMESTAMP'} — which
 * isthmus's {@code TypeConverter.DEFAULT} has no UDT handling for.
 */
public class DatetimeUdfSubstraitConversionTests extends OpenSearchTestCase {

    private SimpleExtension.ExtensionCollection extensions;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Load the Substrait extension catalog with the test classloader as TCCL —
        // mirrors DataFusionFragmentConvertorTests and DataFusionPlugin#loadSubstraitExtensions.
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(DatetimeUdfSubstraitConversionTests.class.getClassLoader());
            extensions = SubstraitExtensionLoader.load();
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    /** Single-table schema: {@code t} with one TIMESTAMP column {@code created}. */
    private static AbstractSchema buildTestSchema() {
        Table t = new AbstractTable() {
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
        tables.put("t", t);
        return new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return tables;
            }
        };
    }

    private static RelNode planPpl(String ppl) throws Exception {
        try (
            UnifiedQueryContext ctx = UnifiedQueryContext.builder()
                .language(QueryType.PPL)
                .catalog("opensearch", buildTestSchema())
                .defaultNamespace("opensearch")
                .build()
        ) {
            return new UnifiedQueryPlanner(ctx).plan(ppl);
        }
    }

    public void testConvertDateAddRelNodeToSubstrait() throws Exception {
        String ppl = "source=t | eval d = DATE_ADD(created, INTERVAL 1 DAY)";
        RelNode plan = planPpl(ppl);
        logger.info("PPL: {}", ppl);
        logger.info("RelNode row type: {}", plan.getRowType());

        DataFusionFragmentConvertor convertor = new DataFusionFragmentConvertor(extensions);
        byte[] bytes = convertor.convertShardScanFragment("t", plan);
        logger.info("Substrait plan: {} bytes", bytes.length);
    }
}
