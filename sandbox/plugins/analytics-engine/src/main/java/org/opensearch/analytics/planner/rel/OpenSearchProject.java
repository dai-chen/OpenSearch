/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * OpenSearch custom Project carrying viable backend list and per-expression annotations.
 *
 * @opensearch.internal
 */
public class OpenSearchProject extends Project implements OpenSearchRelNode, DistributionAware {

    private final List<String> viableBackends;

    /**
     * When true, this Project must stay ABOVE the ExchangeReducer (in the coordinator fragment) —
     * {@link #computeSelfCost} returns infinite cost unless its input is already gathered
     * (SINGLETON/ANY), forcing Volcano to place an ER below it. Used to keep an aggregate's literal
     * config arg (e.g. percentile's {@code 50}) adjacent to the aggregate while a duplicate,
     * unpinned, physical-only Project pushes below the gather for projection-pushdown. Mirrors the
     * RexOver gate, which has the same coordinator-side requirement.
     */
    private final boolean pinAboveExchange;

    public OpenSearchProject(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<? extends RexNode> projects,
        RelDataType rowType,
        List<String> viableBackends
    ) {
        this(cluster, traitSet, input, projects, rowType, viableBackends, false);
    }

    public OpenSearchProject(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<? extends RexNode> projects,
        RelDataType rowType,
        List<String> viableBackends,
        boolean pinAboveExchange
    ) {
        super(cluster, traitSet, List.of(), input, projects, rowType);
        this.viableBackends = viableBackends;
        this.pinAboveExchange = pinAboveExchange;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /** See {@link #pinAboveExchange}. */
    public boolean isPinAboveExchange() {
        return pinAboveExchange;
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        RelNode input = RelNodeUtils.unwrapHep(getInput());
        if (!(input instanceof OpenSearchRelNode openSearchChild)) {
            throw new IllegalStateException("Project child is not OpenSearchRelNode: " + input.getClass().getSimpleName());
        }
        List<FieldStorageInfo> inputStorage = openSearchChild.getOutputFieldStorage();

        List<FieldStorageInfo> result = new ArrayList<>(getProjects().size());
        for (int i = 0; i < getProjects().size(); i++) {
            RexNode expr = getProjects().get(i);
            if (expr instanceof RexInputRef ref && ref.getIndex() < inputStorage.size()) {
                result.add(inputStorage.get(ref.getIndex()));
            } else {
                String fieldName = getRowType().getFieldList().get(i).getName();
                LinkedHashSet<String> deps = RelNodeUtils.resolvePhysicalDeps(expr, inputStorage);
                result.add(FieldStorageInfo.derivedColumn(fieldName, getRowType().getFieldList().get(i).getType().getSqlTypeName(), deps));
            }
        }
        return result;
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new OpenSearchProject(getCluster(), traitSet, input, projects, rowType, viableBackends, pinAboveExchange);
    }

    /**
     * Projects containing {@code RexOver} (window functions) constrain where they may run. A window
     * <em>partitioned</em> by one or more columns is satisfiable by input hash-partitioned on (a subset of)
     * those columns — every row of a given partition then lands on one worker, which is all the window
     * frame needs. An unpartitioned window ({@code OVER ()}) has a single global frame and genuinely needs
     * fully-gathered input. Projects flagged {@link #pinAboveExchange} must likewise stay in the coordinator
     * fragment (they carry an aggregate's literal config arg).
     *
     * <p>So: tiny cost when the input is SINGLETON/ANY, or when it is HASH_DISTRIBUTED on keys contained in
     * the window's PARTITION BY set; infinite otherwise, which makes Volcano place an exchange below. This
     * mirrors {@code IgniteWindow.satisfiesDistribution} and the {@code hash(partitionKeys)}-else-SINGLETON
     * requirement in Drill's {@code WindowPrule} and Flink's {@code BatchPhysicalOverAggregateRule}.
     *
     * <p>Plain projects (neither window nor pinned) have no ordering requirement — tiny cost unconditionally.
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (!containsOver() && !pinAboveExchange) {
            return planner.getCostFactory().makeTinyCost();
        }
        // containsOver() is Calcite's own — inherited from Project.
        List<Integer> partitionKeys = pinAboveExchange ? List.of() : windowPartitionKeys();
        for (int i = 0; i < getInput().getTraitSet().size(); i++) {
            RelTrait trait = getInput().getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution distribution) {
                if (distribution.getType() == RelDistribution.Type.SINGLETON || distribution.getType() == RelDistribution.Type.ANY) {
                    continue;
                }
                if (satisfiedByHash(distribution, partitionKeys)) {
                    continue;
                }
                return planner.getCostFactory().makeInfiniteCost();
            }
        }
        return planner.getCostFactory().makeTinyCost();
    }

    /**
     * Whether a HASH_DISTRIBUTED input satisfies a window partitioned by {@code partitionKeys}: the hash
     * keys must be a subset of the partition keys. Hashing on a subset is safe — it is a coarser
     * partitioning, so all rows sharing a PARTITION BY tuple still co-locate. Hashing on a superset is
     * not: two rows with the same PARTITION BY tuple could differ in the extra key and split across
     * workers, which would compute the window over a fragment of its frame.
     */
    private static boolean satisfiedByHash(OpenSearchDistribution distribution, List<Integer> partitionKeys) {
        return distribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED
            && partitionKeys.isEmpty() == false
            && partitionKeys.containsAll(distribution.getKeys());
    }

    /**
     * Input-column indices this project's window functions all partition by, or empty when the window
     * cannot be distributed on a key.
     *
     * <p>Empty is returned — meaning "this window needs SINGLETON" — when there is no window at all, when
     * any window is unpartitioned ({@code OVER ()}, a single global frame), when a partition key is not a
     * plain column reference, or when several windows in the same project disagree on their partition keys.
     * The last case is a deliberate simplification: one input distribution cannot satisfy two different
     * partitionings, and Calcite emits one {@code Project} per {@code Window.Group} only after physical
     * translation, so here we keep the conservative answer rather than split the project.
     */
    private List<Integer> windowPartitionKeys() {
        List<LinkedHashSet<Integer>> perWindow = new ArrayList<>();
        RexShuttle collector = new RexShuttle() {
            @Override
            public RexNode visitOver(RexOver over) {
                LinkedHashSet<Integer> keys = new LinkedHashSet<>();
                for (RexNode key : over.getWindow().partitionKeys) {
                    if (key instanceof RexInputRef ref) {
                        keys.add(ref.getIndex());
                    } else {
                        // Non-trivial partition expression — cannot map it to an input column to hash on.
                        keys.clear();
                        break;
                    }
                }
                perWindow.add(keys);
                return super.visitOver(over);
            }
        };
        for (RexNode expr : getProjects()) {
            expr.accept(collector);
        }
        if (perWindow.isEmpty()) {
            return List.of();
        }
        LinkedHashSet<Integer> first = perWindow.getFirst();
        if (first.isEmpty()) {
            return List.of();
        }
        for (LinkedHashSet<Integer> keys : perWindow) {
            if (!keys.equals(first)) {
                return List.of();
            }
        }
        return List.copyOf(first);
    }

    // ---- DistributionAware (Option B post-CBO enforcement pass) ----

    /**
     * A row-wise project imposes no partitioning requirement on its input (it neither needs nor breaks a
     * distribution) — returns {@code null} so the input keeps whatever distribution it derived.
     *
     * <p>A window-bearing project demands the partitioning its window frames need: {@code WORKER+HASH} on the
     * PARTITION BY keys when it has them, so the window runs distributed with each partition whole on one
     * worker; {@code COORDINATOR+SINGLETON} for an unpartitioned {@code OVER ()}, whose frame spans every row.
     * This is the same three-way choice Drill's {@code WindowPrule} and Flink's
     * {@code BatchPhysicalOverAggregateRule} make, and it mirrors the existing
     * {@link OpenSearchAggregate#requiredInputDistribution} which demands {@code hash(groupSet)} for a
     * grouped aggregate and SINGLETON for a global one.
     *
     * <p>A {@code pinAboveExchange} project still requires SINGLETON — it must stay in the coordinator
     * fragment next to the aggregate whose literal config arg it carries, regardless of any window.
     */
    @Override
    public OpenSearchDistribution requiredInputDistribution(int inputIndex, int partitionCount, OpenSearchDistributionTraitDef traitDef) {
        if (inputIndex != 0) {
            return null;
        }
        if (!containsOver() && !pinAboveExchange) {
            return null;
        }
        if (!pinAboveExchange) {
            List<Integer> partitionKeys = windowPartitionKeys();
            if (!partitionKeys.isEmpty()) {
                return traitDef.hash(partitionKeys, partitionCount);
            }
        }
        return traitDef.coordSingleton();
    }

    /**
     * A plain project passes the child's distribution through, REMAPPED to output columns: a hash key at
     * input column {@code k} moves to wherever the projection places {@code k} (and degrades to ANY if the
     * projection drops it) — exactly {@link OpenSearchDistribution#apply} over the project's
     * {@code getPartialMapping}.
     *
     * <p>A partitioned window that ran on hash-distributed input keeps that partitioning on its output — each
     * worker still holds whole partitions — so it remaps the child distribution like a plain project, letting
     * a co-partitioned parent (e.g. the {@code rn = 1} filter of {@code dedup}) avoid a needless gather. An
     * unpartitioned window, or a pinned project, gathered its input to SINGLETON, so its output is SINGLETON.
     * Returns {@code null} when the child distribution is unknown.
     */
    @Override
    public OpenSearchDistribution deriveOutputDistribution(
        List<OpenSearchDistribution> childDistributions,
        OpenSearchDistributionTraitDef traitDef
    ) {
        if (childDistributions.size() != 1 || childDistributions.get(0) == null) {
            return null;
        }
        OpenSearchDistribution childDist = childDistributions.get(0);
        if (pinAboveExchange) {
            return traitDef.coordSingleton();
        }
        if (containsOver() && !satisfiedByHash(childDist, windowPartitionKeys())) {
            return traitDef.coordSingleton();
        }
        return remapHashKeys(childDist, traitDef);
    }

    /**
     * Rewrites a HASH distribution's keys from input to output column ordinals by locating each key's
     * {@link RexInputRef} in this project's expressions, degrading to ANY when the projection drops a key —
     * Calcite's documented {@code RelDistribution.apply} contract.
     *
     * <p>Done by hand rather than through {@code childDist.apply(Project.getPartialMapping(...))}: the
     * {@code TargetMapping} that {@code getPartialMapping} returns does not implement
     * {@code getTargetOpt} for every projection shape and throws {@link UnsupportedOperationException}
     * (hit as soon as a window project — whose expressions include a non-{@code RexInputRef} window
     * column — asks to keep a hash distribution). Non-HASH distributions pass through unchanged.
     */
    private OpenSearchDistribution remapHashKeys(OpenSearchDistribution childDist, OpenSearchDistributionTraitDef traitDef) {
        if (childDist.getType() != RelDistribution.Type.HASH_DISTRIBUTED || childDist.getKeys().isEmpty()) {
            return childDist;
        }
        Map<Integer, Integer> inputToOutput = new LinkedHashMap<>();
        for (int out = 0; out < getProjects().size(); out++) {
            if (getProjects().get(out) instanceof RexInputRef ref) {
                inputToOutput.putIfAbsent(ref.getIndex(), out);
            }
        }
        List<Integer> remapped = new ArrayList<>(childDist.getKeys().size());
        for (int key : childDist.getKeys()) {
            Integer target = inputToOutput.get(key);
            if (target == null) {
                return traitDef.any();
            }
            remapped.add(target);
        }
        return childDist.withKeys(remapped);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends);
    }

    @Override
    public List<OperatorAnnotation> getAnnotations() {
        List<OperatorAnnotation> annotations = new ArrayList<>();
        for (RexNode expr : getProjects()) {
            if (expr instanceof AnnotatedProjectExpression annotation) {
                annotations.add(annotation);
            }
        }
        return annotations;
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        int annotationIndex = 0;
        List<RexNode> resolvedExprs = new ArrayList<>();
        for (RexNode expr : getProjects()) {
            if (expr instanceof AnnotatedProjectExpression) {
                resolvedExprs.add((RexNode) resolvedAnnotations.get(annotationIndex++));
            } else {
                // Plain expressions (field refs, literals, scalar calls) have no annotation — pass through.
                resolvedExprs.add(expr);
            }
        }
        return new OpenSearchProject(
            getCluster(),
            getTraitSet(),
            children.getFirst(),
            resolvedExprs,
            getRowType(),
            List.of(backend),
            pinAboveExchange
        );
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return stripAnnotations(strippedChildren, OperatorAnnotation::unwrap);
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren, Function<OperatorAnnotation, RexNode> annotationResolver) {
        // OpenSearchProjectRule.annotateExpr recurses into operands when validating viable
        // backends, so a top-level call like COALESCE(num0, CEIL(num1)) ends up with the inner
        // CEIL also wrapped. The supplied annotationResolver controls how each top-level
        // wrapper is unwrapped (defaults to OperatorAnnotation::unwrap, returning the original
        // RexNode); a RexShuttle then sweeps the resolver's result to strip any remaining
        // nested wrappers. Substrait conversion only recognizes the underlying RexCall shape,
        // so every wrapper at every depth must be removed before the plan is handed to a
        // backend's FragmentConvertor.
        //
        // Top-level baseline operators (BASELINE_SCALAR_OPS — COALESCE, CASE, CAST, arithmetic,
        // IS_NULL, …) bypass the AnnotatedProjectExpression wrap at the call site, but their
        // operands still go through annotation. The shuttle therefore runs on every project
        // expression — including plain ones — to catch annotated operands nested inside a
        // baseline-op root.
        RexShuttle nestedAnnotationStripper = new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                if (call instanceof AnnotatedProjectExpression nested) {
                    return nested.getOriginal().accept(this);
                }
                return super.visitCall(call);
            }
        };
        List<RexNode> strippedExprs = new ArrayList<>();
        for (RexNode expr : getProjects()) {
            if (expr instanceof AnnotatedProjectExpression annotated) {
                RexNode resolved = annotationResolver.apply(annotated);
                strippedExprs.add(resolved.accept(nestedAnnotationStripper));
            } else {
                // Pass-through expressions (RexInputRef, RexLiteral) have no annotation to
                // resolve. Running the shuttle is defensive and idempotent — atomic nodes
                // contain no nested AnnotatedProjectExpression to strip.
                strippedExprs.add(expr.accept(nestedAnnotationStripper));
            }
        }

        // Lift nested RexOver expressions out of scalar calls into a child LogicalProject.
        // PPL's `bin` command lowers `bins=N` / `minspan=N` / `start=… end=…` to a single
        // top-level scalar call whose operands embed RexOver: e.g.
        // width_bucket(f, N, MAX(f) OVER () - MIN(f) OVER (), MAX(f) OVER ())
        // DataFusion's substrait consumer auto-lifts *top-level* WindowFunction project
        // expressions into a LogicalWindow (datafusion-substrait
        // `from_project_rel`), but the nested RexOvers inside `width_bucket(...)` stay
        // where they are and reach DataFusion's physical planner — which then errors
        // with "Physical plan does not support logical expression WindowFunction(...)".
        //
        // Pre-substrait fix: walk every project expression, hoist each unique RexOver
        // into a child Project as its own top-level expression, and rewrite the original
        // expression to reference the hoisted column via RexInputRef. The child Project
        // becomes:
        // [input_field_0, input_field_1, ..., input_field_(n-1), MAX(f) OVER (), MIN(f) OVER ()]
        // and the outer Project's expressions reference those new columns by index.
        // DataFusion sees the WindowFunctions at the top level of the inner Project and
        // wraps them in a LogicalWindow as expected.
        Project lifted = liftNestedRexOver(strippedChildren.getFirst(), strippedExprs);
        if (lifted != null) {
            return lifted;
        }
        return LogicalProject.create(strippedChildren.getFirst(), List.of(), strippedExprs, getRowType());
    }

    /**
     * Hoists nested {@link RexOver} expressions out of {@code outerExprs} into a child
     * {@link LogicalProject} sitting on top of {@code input}. Returns {@code null} if no
     * RexOver was found (caller should emit a single-level Project as before).
     */
    private Project liftNestedRexOver(RelNode input, List<RexNode> outerExprs) {
        // Collect unique RexOvers from the expression trees. LinkedHashMap by digest so
        // the same RexOver from multiple expressions (e.g. MAX(f) OVER () appearing as
        // both data_range operand and max_value operand of width_bucket) is hoisted once
        // and shares a single column slot.
        LinkedHashMap<String, RexOver> uniqueOvers = new LinkedHashMap<>();
        RexShuttle collector = new RexShuttle() {
            @Override
            public RexNode visitOver(RexOver over) {
                uniqueOvers.putIfAbsent(over.toString(), over);
                return over;
            }
        };
        for (RexNode expr : outerExprs) {
            expr.accept(collector);
        }
        if (uniqueOvers.isEmpty()) {
            return null;
        }

        int inputFieldCount = input.getRowType().getFieldCount();
        RexBuilder rexBuilder = getCluster().getRexBuilder();

        // Build the lower-Project expressions: passthrough every input field as RexInputRef,
        // then append each unique RexOver as its own top-level expression. The lower-Project's
        // row type matches: input fields followed by appended window-output columns.
        List<RexNode> lowerExprs = new ArrayList<>(inputFieldCount + uniqueOvers.size());
        for (int i = 0; i < inputFieldCount; i++) {
            lowerExprs.add(rexBuilder.makeInputRef(input, i));
        }
        // overIndex maps "over digest" → its column index in the lower Project's output.
        Map<String, Integer> overIndex = new LinkedHashMap<>();
        int nextSlot = inputFieldCount;
        for (Map.Entry<String, RexOver> entry : uniqueOvers.entrySet()) {
            overIndex.put(entry.getKey(), nextSlot++);
            lowerExprs.add(entry.getValue());
        }
        Project lowerProject = LogicalProject.create(input, List.of(), lowerExprs, (List<String>) null);

        // Rewrite outer expressions: replace each RexOver with a RexInputRef into the
        // lower Project's output. Field names of the lower Project are anonymous (Calcite
        // auto-generates) — that's fine, we reference by index.
        RexShuttle rewriter = new RexShuttle() {
            @Override
            public RexNode visitOver(RexOver over) {
                Integer slot = overIndex.get(over.toString());
                if (slot == null) {
                    // Should not happen — collector found every RexOver.
                    return super.visitOver(over);
                }
                return rexBuilder.makeInputRef(lowerProject, slot);
            }
        };
        List<RexNode> rewrittenOuter = new ArrayList<>(outerExprs.size());
        for (RexNode expr : outerExprs) {
            rewrittenOuter.add(expr.accept(rewriter));
        }

        return LogicalProject.create(lowerProject, List.of(), rewrittenOuter, getRowType());
    }
}
