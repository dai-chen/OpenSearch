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
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.opensearch.querylanguages.opensearch.storage.scan.CalciteLogicalIndexScan;

import java.util.Locale;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.MULTI_FIELDS_RELEVANCE_FUNCTION_SET;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.SINGLE_FIELD_RELEVANCE_FUNCTION_SET;

/** RelevanceFunctionPushdownRule planner rule. */
public class RelevanceFunctionPushdownRule extends RelOptRule {

    /** Singleton instance. */
    public static final RelevanceFunctionPushdownRule INSTANCE = new RelevanceFunctionPushdownRule();

    private RelevanceFunctionPushdownRule() {
        super(operand(LogicalFilter.class, operand(CalciteLogicalIndexScan.class, none())), "RelevanceFunctionPushdownRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        if (call.rels.length == 2) {
            final LogicalFilter filter = call.rel(0);
            final CalciteLogicalIndexScan scan = call.rel(1);

            // This rule is only used when pushdown is disabled,
            // so we only push down filters that contain relevance functions
            if (containsRelevanceFunction(filter.getCondition())) {
                apply(call, filter, scan);
            }
        } else {
            throw new AssertionError(String.format(Locale.ROOT, "The length of rels should be 2 but got %s", call.rels.length));
        }
    }

    protected void apply(RelOptRuleCall call, Filter filter, CalciteLogicalIndexScan scan) {
        AbstractRelNode newRel = scan.pushDownFilter(filter);
        if (newRel != null) {
            call.transformTo(newRel);
        }
    }

    /**
     * Checks if a RexNode contains any relevance functions.
     *
     * @param node The RexNode to check
     * @return true if the node contains relevance functions, false otherwise
     */
    private boolean containsRelevanceFunction(RexNode node) {
        RelevanceFunctionVisitor visitor = new RelevanceFunctionVisitor();
        node.accept(visitor);
        return visitor.hasRelevanceFunction();
    }

    /** Visitor to detect relevance functions in a RexNode tree. */
    private static class RelevanceFunctionVisitor extends RexVisitorImpl<Void> {
        private boolean foundRelevanceFunction = false;

        RelevanceFunctionVisitor() {
            super(true);
        }

        @Override
        public Void visitCall(RexCall call) {
            SqlOperator operator = call.getOperator();
            String operatorName = operator.getName().toLowerCase(Locale.ROOT);

            // Check if this is a relevance function
            if (SINGLE_FIELD_RELEVANCE_FUNCTION_SET.contains(operatorName) || MULTI_FIELDS_RELEVANCE_FUNCTION_SET.contains(operatorName)) {
                foundRelevanceFunction = true;
                return null; // Stop traversing once we find a relevance function
            }

            // Continue traversing the tree
            return super.visitCall(call);
        }

        boolean hasRelevanceFunction() {
            return foundRelevanceFunction;
        }
    }

    /** Rule configuration. */
}
