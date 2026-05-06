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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Map;

/**
 * End-to-end PPL datetime UDF tests.
 * Full pipeline: PPL → RelNode → DatetimeTypeRewriter → Substrait → DataFusion → results.
 *
 * <p>Table: t(created TIMESTAMP) with one row: 2024-01-15T10:30:00Z
 */
public class DatetimeUdfPplIntegTests extends PplDataFusionTestBase {

    // 2024-01-15T10:30:00Z in epoch millis
    private static final long INPUT_MILLIS = 1705312200000L;

    @Override
    protected String tableName() {
        return "t";
    }

    @Override
    protected AbstractSchema calciteSchema() {
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

    @Override
    protected Schema arrowSchema() {
        return new Schema(List.of(
            new Field("created", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null)
        ));
    }

    @Override
    protected VectorSchemaRoot createTestBatch() {
        VectorSchemaRoot batch = VectorSchemaRoot.create(arrowSchema(), allocator);
        batch.allocateNew();
        TimeStampMilliVector col = (TimeStampMilliVector) batch.getVector(0);
        col.setSafe(0, INPUT_MILLIS);
        col.setValueCount(1);
        batch.setRowCount(1);
        return batch;
    }

    // ── Tests ──────────────────────────────────────────────────────────────────

    /**
     * source=t | eval d = DATE_ADD(created, INTERVAL 1 DAY)
     * Expected: created + d both non-null, d = created + 1 day
     */
    public void testDateAddEndToEnd() throws Exception {
        List<Object[]> rows = executePpl("source=t | eval d = DATE_ADD(created, INTERVAL 1 DAY)");

        assertEquals(1, rows.size());
        assertNotNull("created must not be null", rows.get(0)[0]);
        assertNotNull("d must not be null", rows.get(0)[1]);
    }

    /**
     * source=t | eval d = LAST_DAY(created)
     * Expected: d = 2024-01-31 (last day of January)
     */
    public void testLastDayEndToEnd() throws Exception {
        List<Object[]> rows = executePpl("source=t | eval d = LAST_DAY(created)");

        assertEquals(1, rows.size());
        assertNotNull("d must not be null", rows.get(0)[1]);
        assertTrue("d should contain 2024-01-31", rows.get(0)[1].toString().contains("2024-01-31"));
    }

    /**
     * source=t | where created > TIMESTAMP('2020-01-01 00:00:00')
     * Expected: 1 row (2024-01-15 > 2020-01-01 is true)
     */
    public void testTimestampFilterEndToEnd() throws Exception {
        List<Object[]> rows = executePpl("source=t | where created > TIMESTAMP('2020-01-01 00:00:00')");

        assertEquals("Filter should pass", 1, rows.size());
        assertNotNull("created must not be null", rows.get(0)[0]);
    }
}
