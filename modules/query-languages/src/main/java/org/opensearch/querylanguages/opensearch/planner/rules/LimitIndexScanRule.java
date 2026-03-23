/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.planner.rules;

import java.util.Objects;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.querylanguages.opensearch.storage.scan.CalciteLogicalIndexScan;

/**
 * Planner rule that pushes a {@link LogicalSort} with LIMIT semantics down to
 * {@link CalciteLogicalIndexScan}.
 */
public class LimitIndexScanRule extends RelOptRule {

    public static final LimitIndexScanRule INSTANCE = new LimitIndexScanRule();

    private LimitIndexScanRule() {
        super(
            operand(
                LogicalSort.class,
                operand(CalciteLogicalIndexScan.class, none())),
            "LimitIndexScanRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalSort sort = call.rel(0);
        if (!PlanUtils.isLogicalSortLimit(sort)) {
            return;
        }
        final CalciteLogicalIndexScan scan = call.rel(1);

        if (!sort.getCollation().getFieldCollations().isEmpty()
            && sort.getCollation() != scan.getTraitSet().getCollation()) {
            return;
        }

        Integer limitValue = extractLimitValue(sort.fetch);
        Integer offsetValue = extractOffsetValue(sort.offset);
        if (limitValue != null && offsetValue != null) {
            AbstractRelNode newOperator = scan.pushDownLimit(sort, limitValue, offsetValue);
            if (newOperator != null) {
                call.transformTo(newOperator);
            }
        }
    }

    private static Integer extractLimitValue(RexNode fetch) {
        if (fetch instanceof RexLiteral) {
            return ((RexLiteral) fetch).getValueAs(Integer.class);
        }
        return null;
    }

    private static Integer extractOffsetValue(RexNode offset) {
        if (Objects.isNull(offset)) {
            return 0;
        }
        if (offset instanceof RexLiteral) {
            return ((RexLiteral) offset).getValueAs(Integer.class);
        }
        return null;
    }
}
