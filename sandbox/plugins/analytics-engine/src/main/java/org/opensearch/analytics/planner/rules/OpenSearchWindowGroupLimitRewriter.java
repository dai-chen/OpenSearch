/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Post-CBO rewriter that pushes a <em>rank-limited window</em> below the exchange, so each shard
 * emits only the rows that could belong to the global answer instead of its whole matching set.
 *
 * <p>Matches the canonical shape other engines special-case:
 *
 * <pre>
 *   Filter(rank &lt;= N)
 *     Project(..., RANK-LIKE() OVER (PARTITION BY k [ORDER BY o]))
 *       ExchangeReducer
 *         &lt;shard subtree&gt;
 * </pre>
 *
 * and rewrites the shard subtree to compute the same window and apply the same rank bound locally,
 * projecting the rank column away again so the exchange's row type — and therefore everything above
 * it — is untouched:
 *
 * <pre>
 *   ExchangeReducer
 *     Project(original columns)          &lt;- drops the local rank column
 *       Filter(rank &lt;= N)
 *         Project(original columns, RANK-LIKE() OVER (PARTITION BY k [ORDER BY o]))
 *           &lt;shard subtree&gt;
 * </pre>
 *
 * <p><b>Why this is sound.</b> For a rank-like function the local rank of a row is never greater
 * than its global rank: adding the other shards' rows to a partition can only push a row further
 * down. So every row satisfying {@code global_rank <= N} also satisfies {@code local_rank <= N} —
 * the shard-side filter discards only rows that the coordinator's filter would discard anyway. The
 * coordinator keeps its own copy of the window and the filter and remains the authority; the shard
 * copy is a pre-filter, exactly like a PARTIAL aggregate is a pre-reduction.
 *
 * <p>This is the same optimisation as:
 *
 * <ul>
 *   <li>Spark's {@code InsertWindowGroupLimit}, which inserts a {@code WindowGroupLimit} below the
 *       shuffle for {@code RowNumber}/{@code Rank}/{@code DenseRank} under a rank predicate, keeping
 *       the post-shuffle window as the final authority.</li>
 *   <li>Flink's batch {@code Rank} operator, planned as a local rank before the exchange and a
 *       global rank after it.</li>
 *   <li>Apache Ignite's / Calcite's general preference for evaluating a limiting predicate on the
 *       producing side of an exchange.</li>
 * </ul>
 *
 * <p><b>Deliberately not matched.</b> An <em>additive</em> window measure under a predicate —
 * {@code COUNT() OVER (PARTITION BY k) > 100} — is unsound to push: a shard's partial count is not
 * the global count, so a locally-failing row may pass globally. Only rank-like functions, whose
 * local value bounds the global value in the safe direction, qualify. That is why PPL's
 * {@code eventstats count() ... | sort - c | head N} is not affected by this rule.
 *
 * @opensearch.internal
 */
public final class OpenSearchWindowGroupLimitRewriter {

    private OpenSearchWindowGroupLimitRewriter() {}

    /** Window functions whose local rank bounds their global rank. Mirrors Spark's {@code RankLike}. */
    private static boolean isRankLike(SqlKind kind) {
        return kind == SqlKind.ROW_NUMBER || kind == SqlKind.RANK || kind == SqlKind.DENSE_RANK;
    }

    public static Optional<RelNode> rewrite(RelNode root) {
        Match match = find(root);
        if (match == null) {
            return Optional.empty();
        }
        RelNode shardSubtree = match.er.getInputs().get(0);
        List<String> shardViable = match.shardViable;
        RexBuilder rb = match.filter.getCluster().getRexBuilder();

        // The window Project above the ER is expressed over the ER's output, which is row-identical
        // to the shard subtree's output, so its RexOver can be reused verbatim on the shard side.
        RelDataType shardRowType = shardSubtree.getRowType();
        int fieldCount = shardRowType.getFieldCount();

        List<RexNode> withRank = new ArrayList<>(fieldCount + 1);
        List<String> withRankNames = new ArrayList<>(fieldCount + 1);
        for (int i = 0; i < fieldCount; i++) {
            withRank.add(rb.makeInputRef(shardRowType.getFieldList().get(i).getType(), i));
            withRankNames.add(shardRowType.getFieldNames().get(i));
        }
        withRank.add(match.over);
        withRankNames.add("_local_rank_");

        OpenSearchProject shardRankProject = new OpenSearchProject(
            match.filter.getCluster(),
            shardSubtree.getTraitSet(),
            shardSubtree,
            withRank,
            rb.getTypeFactory().createStructType(withRank.stream().map(RexNode::getType).toList(), withRankNames),
            shardViable
        );

        // rank <= N, with the rank column at the end of the shard project's output.
        RexNode rankRef = rb.makeInputRef(match.over.getType(), fieldCount);
        RexNode shardCondition = rb.makeCall(match.comparison, rankRef, match.bound);
        OpenSearchFilter shardFilter = new OpenSearchFilter(
            match.filter.getCluster(),
            shardRankProject.getTraitSet(),
            shardRankProject,
            shardCondition,
            shardViable
        );

        // Drop the local rank column so the exchange's row type is exactly what it was.
        List<RexNode> stripped = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            stripped.add(rb.makeInputRef(shardRowType.getFieldList().get(i).getType(), i));
        }
        OpenSearchProject shardStrip = new OpenSearchProject(
            match.filter.getCluster(),
            shardFilter.getTraitSet(),
            shardFilter,
            stripped,
            shardRowType,
            shardViable
        );

        RelNode newEr = match.er.copy(match.er.getTraitSet(), List.of(shardStrip));
        return Optional.of(replaceInTree(root, match.er, newEr));
    }

    /**
     * Finds {@code Filter(rank <= N)} over {@code Project(RANK-LIKE OVER (PARTITION BY ...))} over an
     * {@link OpenSearchExchangeReducer}, or {@code null} when the plan is not that shape.
     */
    private static Match find(RelNode node) {
        if (node instanceof OpenSearchFilter filter && filter.getInput() instanceof OpenSearchProject project) {
            Match m = match(filter, project);
            if (m != null) {
                return m;
            }
        }
        for (RelNode input : node.getInputs()) {
            Match m = find(input);
            if (m != null) {
                return m;
            }
        }
        return null;
    }

    private static Match match(OpenSearchFilter filter, OpenSearchProject project) {
        if (!(project.getInput() instanceof OpenSearchExchangeReducer er) || er.getInputs().isEmpty()) {
            return null;
        }
        // The shard subtree must be an analytics node so its viable-backend set can be carried onto the
        // nodes we add; a foreign rel there means the plan was not fully converted and we leave it be.
        if (!(er.getInputs().get(0) instanceof OpenSearchRelNode shardNode)) {
            return null;
        }
        // Exactly one rank-like RexOver, partitioned. An unpartitioned window would make every shard
        // keep its own global top-N, which is still sound but pointless — the coordinator sees the
        // same rows either way once N is large relative to the shard count, and PPL never emits it.
        RexOver over = null;
        int rankIndex = -1;
        for (int i = 0; i < project.getProjects().size(); i++) {
            RexNode expr = project.getProjects().get(i);
            if (expr instanceof RexOver o) {
                if (over != null) {
                    return null;
                }
                if (!isRankLike(o.getAggOperator().getKind())) {
                    return null;
                }
                RexWindow window = o.getWindow();
                if (window.partitionKeys.isEmpty()) {
                    return null;
                }
                for (RexNode key : window.partitionKeys) {
                    if (!(key instanceof RexInputRef)) {
                        return null;
                    }
                }
                over = o;
                rankIndex = i;
            } else if (RexOver.containsOver(expr)) {
                // A rank nested inside a larger expression: the filter's reference cannot be matched
                // to it positionally, so leave the plan alone.
                return null;
            }
        }
        if (over == null) {
            return null;
        }
        // Condition must be `rank <= literal` (or `<`), referencing the rank column directly. Filter
        // conditions carry an AnnotatedPredicate wrapper at this stage (it records which backends can
        // evaluate the predicate), so unwrap before inspecting the comparison.
        RexNode condition = filter.getCondition();
        if (condition instanceof AnnotatedPredicate annotated) {
            condition = annotated.unwrap();
        }
        if (!(condition instanceof RexCall call)) {
            return null;
        }
        SqlKind kind = call.getKind();
        if (kind != SqlKind.LESS_THAN_OR_EQUAL && kind != SqlKind.LESS_THAN) {
            return null;
        }
        if (call.getOperands().size() != 2
            || !(call.getOperands().get(0) instanceof RexInputRef ref)
            || ref.getIndex() != rankIndex
            || !(call.getOperands().get(1) instanceof RexLiteral bound)) {
            return null;
        }
        return new Match(filter, er, shardNode.getViableBackends(), over, call.getOperator(), bound);
    }

    /** Replaces {@code oldNode} with {@code newNode} everywhere it appears under {@code root}. */
    private static RelNode replaceInTree(RelNode root, RelNode oldNode, RelNode newNode) {
        if (root == oldNode) {
            return newNode;
        }
        List<RelNode> children = root.getInputs();
        boolean changed = false;
        RelNode[] newChildren = new RelNode[children.size()];
        for (int i = 0; i < children.size(); i++) {
            newChildren[i] = replaceInTree(children.get(i), oldNode, newNode);
            if (newChildren[i] != children.get(i)) {
                changed = true;
            }
        }
        if (!changed) {
            return root;
        }
        return root.copy(root.getTraitSet(), List.of(newChildren));
    }

    private record Match(OpenSearchFilter filter, OpenSearchExchangeReducer er, List<String> shardViable, RexOver over,
        org.apache.calcite.sql.SqlOperator comparison, RexLiteral bound) {
    }
}
