/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexWindow;
import org.opensearch.querylanguages.opensearch.storage.scan.CalciteLogicalIndexScan;
import org.opensearch.querylanguages.opensearch.storage.scan.context.RareTopDigest;
import org.opensearch.sql.calcite.utils.PlanUtils;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** RareTopPushdownRule planner rule. */
public class RareTopPushdownRule extends RelOptRule {

    /** Singleton instance. */
    public static final RareTopPushdownRule INSTANCE = new RareTopPushdownRule();

    private RareTopPushdownRule() {
        super(
            operand(
                LogicalProject.class,
                operand(
                    LogicalSort.class,
                    operand(LogicalAggregate.class, operand(LogicalProject.class, operand(CalciteLogicalIndexScan.class, none())))
                )
            ),
            "RareTopPushdownRule"
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalFilter filter = call.rel(0);
        final LogicalProject project = call.rel(1);
        final CalciteLogicalIndexScan scan = call.rel(2);
        RareTopDigest digest;
        try {
            RexLiteral numberLiteral = (RexLiteral) ((RexCall) filter.getCondition()).getOperands().get(1);
            Integer number = numberLiteral.getValueAs(Integer.class);
            List<RexWindow> windows = PlanUtils.getRexWindowFromProject(project);
            if (windows.size() != 1) {
                return;
            }
            final List<String> fieldNameList = project.getInput().getRowType().getFieldNames();
            List<Integer> groupIndices = PlanUtils.getSelectColumns(windows.get(0).partitionKeys);
            List<String> byList = groupIndices.stream().map(fieldNameList::get).collect(Collectors.toList());

            if (windows.get(0).orderKeys.size() != 1) {
                return;
            }
            RexFieldCollation orderKey = windows.get(0).orderKeys.get(0);
            List<Integer> orderIndices = PlanUtils.getSelectColumns(List.of(orderKey.getKey()));
            List<String> orderList = orderIndices.stream().map(fieldNameList::get).collect(Collectors.toList());
            List<String> targetList = fieldNameList.stream()
                .filter(Predicate.not(byList::contains))
                .filter(Predicate.not(orderList::contains))
                .collect(Collectors.toList());
            if (targetList.size() != 1) {
                return;
            }
            String targetName = targetList.get(0);
            digest = new RareTopDigest(targetName, byList, number, orderKey.getDirection());
        } catch (Exception e) {
            return;
        }
        CalciteLogicalIndexScan newScan = scan.pushDownRareTop(project, digest);
        if (newScan != null) {
            call.transformTo(newScan);
        }
    }
}
