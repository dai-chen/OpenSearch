/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.planner.rules;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.querylanguages.opensearch.util.OpenSearchRelOptUtil;
import org.opensearch.sql.calcite.utils.PlanUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Transposes Sort over Project when the sort expression can be mapped to an input field sort.
 *
 * @opensearch.internal
 */
public class SortProjectExprTransposeRule extends RelOptRule {

    /** Singleton instance. */
    public static final SortProjectExprTransposeRule INSTANCE = new SortProjectExprTransposeRule();

    private SortProjectExprTransposeRule() {
        super(operand(Sort.class, operand(Project.class, any())), "SortProjectExprTransposeRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Project project = call.rel(1);
        return !project.containsOver() && PlanUtils.containsRexCall(project);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        final Project project = call.rel(1);

        if (sort.getConvention() != project.getConvention()) {
            return;
        }

        List<RelFieldCollation> pushable = new ArrayList<>();
        boolean allPushable = true;
        for (RelFieldCollation fieldCollation : sort.getCollation().getFieldCollations()) {
            RexNode expr = project.getProjects().get(fieldCollation.getFieldIndex());
            Optional<Pair<Integer, Boolean>> pushableExprInputInfo = OpenSearchRelOptUtil.getOrderEquivalentInputInfo(expr);
            if (pushableExprInputInfo.isEmpty()) {
                allPushable = false;
                break;
            }
            int inputIndex = pushableExprInputInfo.get().getLeft();
            boolean flipped = pushableExprInputInfo.get().getRight();
            Direction dir = flipped ? fieldCollation.getDirection().reverse() : fieldCollation.getDirection();
            pushable.add(new RelFieldCollation(inputIndex, dir, fieldCollation.nullDirection));
        }
        if (!allPushable || pushable.isEmpty()) {
            return;
        }

        RelCollation inputCollation = RelCollations.of(pushable);
        Sort lowerSort = sort.copy(sort.getTraitSet().replace(inputCollation), project.getInput(), inputCollation, null, null);
        RelNode result;
        if (sort.fetch == null && sort.offset == null) {
            result = project.copy(sort.getTraitSet(), lowerSort, project.getProjects(), project.getRowType());
        } else {
            Sort limitSort = sort.copy(
                sort.getTraitSet().replace(RelCollations.EMPTY),
                lowerSort,
                RelCollations.EMPTY,
                sort.offset,
                sort.fetch
            );
            result = project.copy(sort.getTraitSet(), limitSort, project.getProjects(), project.getRowType());
        }

        Map<RelNode, RelNode> equiv;
        if (sort.offset == null
            && sort.fetch == null
            && project.getCluster().getPlanner().getRelTraitDefs().contains(RelCollationTraitDef.INSTANCE)) {
            equiv = ImmutableMap.of(lowerSort, project.getInput());
        } else {
            equiv = ImmutableMap.of();
        }
        call.transformTo(result, equiv);
    }
}
