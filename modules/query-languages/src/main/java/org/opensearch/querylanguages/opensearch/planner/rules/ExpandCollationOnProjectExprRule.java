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
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.querylanguages.opensearch.storage.scan.CalciteEnumerableIndexScan;
import org.opensearch.querylanguages.opensearch.util.OpenSearchRelOptUtil;
import org.opensearch.sql.calcite.utils.PlanUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Expands collation on project expressions by finding equivalent field collations.
 *
 * @opensearch.internal
 */
public class ExpandCollationOnProjectExprRule extends RelOptRule {

    /** Singleton instance. */
    public static final ExpandCollationOnProjectExprRule INSTANCE = new ExpandCollationOnProjectExprRule();

    private ExpandCollationOnProjectExprRule() {
        super(operand(AbstractConverter.class, operand(Project.class, any())), "ExpandCollationOnProjectExprRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Project project = call.rel(1);
        return !project.containsOver() && PlanUtils.containsRexCall(project);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final AbstractConverter converter = call.rel(0);
        final Project project = call.rel(1);
        final RelTraitSet toTraits = converter.getTraitSet();
        final RelCollation toCollation = toTraits.getTrait(RelCollationTraitDef.INSTANCE);

        if (toCollation == null || toCollation.getFieldCollations() == null) {
            return;
        }

        Map<Integer, Optional<Pair<Integer, Boolean>>> orderEquivInfoMap = new HashMap<>();
        for (RelFieldCollation relFieldCollation : toCollation.getFieldCollations()) {
            orderEquivInfoMap.put(
                relFieldCollation.getFieldIndex(),
                OpenSearchRelOptUtil.getOrderEquivalentInputInfo(project.getProjects().get(relFieldCollation.getFieldIndex()))
            );
        }

        // Branch 1: Check if complex expressions are already sorted by scan
        if (handleComplexExpressionsSortedByScan(call, project, toTraits, toCollation, orderEquivInfoMap)) {
            return;
        }

        // Branch 2: Handle simple expressions that can be transformed to field sorts
        handleSimpleExpressionFieldSorts(call, project, toTraits, toCollation, orderEquivInfoMap);
    }

    private boolean handleComplexExpressionsSortedByScan(
        RelOptRuleCall call,
        Project project,
        RelTraitSet toTraits,
        RelCollation toCollation,
        Map<Integer, Optional<Pair<Integer, Boolean>>> orderEquivInfoMap
    ) {
        if (toCollation.getFieldCollations().isEmpty()) {
            return false;
        }
        CalciteEnumerableIndexScan scan = extractEnumerableScanFromInput(project.getInput());
        if (scan == null) {
            return false;
        }
        if (OpenSearchRelOptUtil.canScanProvideSortCollation(scan, project, toCollation, orderEquivInfoMap)) {
            Project newProject = project.copy(toTraits, project.getInput(), project.getProjects(), project.getRowType());
            call.transformTo(newProject);
            return true;
        }
        return false;
    }

    private void handleSimpleExpressionFieldSorts(
        RelOptRuleCall call,
        Project project,
        RelTraitSet toTraits,
        RelCollation toCollation,
        Map<Integer, Optional<Pair<Integer, Boolean>>> orderEquivInfoMap
    ) {
        RelTrait fromTrait = project.getInput().getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
        if (fromTrait instanceof RelCollation) {
            RelCollation fromCollation = (RelCollation) fromTrait;
            if (toCollation.getFieldCollations().isEmpty()
                || fromCollation.getFieldCollations().size() < toCollation.getFieldCollations().size()) {
                return;
            }
            for (int i = 0; i < toCollation.getFieldCollations().size(); i++) {
                RelFieldCollation toFieldCollation = toCollation.getFieldCollations().get(i);
                if (!OpenSearchRelOptUtil.sourceCollationSatisfiesTargetCollation(
                    fromCollation.getFieldCollations().get(i),
                    toFieldCollation,
                    orderEquivInfoMap.get(toFieldCollation.getFieldIndex())
                )) {
                    return;
                }
            }
            Project newProject = project.copy(toTraits, project.getInput(), project.getProjects(), project.getRowType());
            call.transformTo(newProject);
            PlanUtils.tryPruneRelNodes(call);
        }
    }

    private static CalciteEnumerableIndexScan extractEnumerableScanFromInput(RelNode input) {
        if (input instanceof CalciteEnumerableIndexScan) {
            return (CalciteEnumerableIndexScan) input;
        }
        if (input instanceof RelSubset) {
            RelSubset subset = (RelSubset) input;
            RelNode bestPlan = subset.getBest();
            if (bestPlan != null) {
                return extractEnumerableScanFromInput(bestPlan);
            }
        }
        return null;
    }
}
