/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.apache.calcite.plan.RelOptUtil;
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

/**
 * Drives the PPL → RelNode pipeline on a PPL query with a datetime UDF.
 *
 * <p>Intentionally does NOT catch or wrap exceptions — any failure from
 * {@link UnifiedQueryPlanner#plan(String)} propagates as-is, so the full
 * stack trace is visible in the test report.
 */
public class DatetimeUdfRelNodeTests extends OpenSearchTestCase {

    /**
     * Single-table schema: {@code t} with one TIMESTAMP column {@code created}.
     */
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

    public void testPlanPplWithDatetimeUdf() throws Exception {
        String ppl = "source=t | eval d = DATE_ADD(created, INTERVAL 1 DAY) | where d > created";
        try (
            UnifiedQueryContext ctx = UnifiedQueryContext.builder()
                .language(QueryType.PPL)
                .catalog("opensearch", buildTestSchema())
                .defaultNamespace("opensearch")
                .build()
        ) {
            RelNode plan = new UnifiedQueryPlanner(ctx).plan(ppl);
            logger.info("PPL: {}", ppl);
            logger.info("RelNode:\n{}", RelOptUtil.toString(plan));
        }
    }
}
