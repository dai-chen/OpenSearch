/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.IntegerRoundingCastAdapter;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;
import java.util.Locale;

/**
 * PoC (dialect=clickhouse): {@code FLOOR(datetime TO unit)} lowers to DataFusion
 * {@code date_trunc('unit', ts)} via {@link DateTimeAdapters#LOCAL_DATE_TRUNC_OP}. Numeric FLOOR
 * keeps the existing {@link IntegerRoundingCastAdapter} behaviour.
 */
final class FloorDatetimeAdapter implements ScalarFunctionAdapter {

    private final ScalarFunctionAdapter numeric = new IntegerRoundingCastAdapter(SqlStdOperatorTable.FLOOR);

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        List<RexNode> operands = original.getOperands();
        if (operands.size() == 2
            && SqlTypeFamily.DATETIME.contains(operands.get(0).getType())
            && operands.get(1) instanceof RexLiteral unit
            && unit.getValue() instanceof TimeUnitRange range) {
            String name = range.name().toLowerCase(Locale.ROOT);
            RexNode unitLiteral = cluster.getRexBuilder().makeLiteral(name);
            return cluster.getRexBuilder()
                .makeCall(original.getType(), DateTimeAdapters.LOCAL_DATE_TRUNC_OP, List.of(unitLiteral, operands.get(0)));
        }
        return numeric.adapt(original, fieldStorage, cluster);
    }
}
