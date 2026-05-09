# Ralph Agent Instructions — OpenSearch Analytics Engine Weekend Gap Attack

The loop is driven by `./ralph.sh` at the repo root (on the `ralph/sql-setup` branch). It maintains a **single inner git worktree at `./work/`** (gitignored), switches that worktree's branch to the active tier each iteration, and drives you to implement + commit there.

Your scope is the analytics-engine side only: `sandbox/plugins/analytics-engine`, `sandbox/plugins/analytics-backend-datafusion`, `sandbox/libs/analytics-framework`, `sandbox/qa/analytics-engine-rest`. Sql-plugin (opensearch-project/sql) is out of scope.

## Per-Iteration Task

1. Read `prd.json` and `progress.txt` (both live next to this prompt file on the setup branch).
2. Check the **Codebase Patterns** and **Canonical E2E Test Patterns** sections of `progress.txt` FIRST.
3. Read any **Round Summary** or **Human Intervention** sections at the end of `progress.txt`.
4. If available, read the full weekend plan at `~/.kiro/plans/opensearch-sql/2026-05-08-weekend-ralph-loop-plan.md` for the active tier's details. If not present on this machine, rely on `prd.json` acceptance criteria only.
5. `ralph.sh` passes you the active `STORY_ID`, `BRANCH`, and absolute `WORK_DIR`. The inner worktree at `WORK_DIR` is already on `BRANCH` before you're invoked.
6. `cd "$WORK_DIR"` BEFORE any code edits, builds, tests, or commits. Confirm `git -C "$WORK_DIR" branch --show-current` equals `BRANCH`. If not, STOP and record the mismatch in the story's `notes`.
7. Implement the story inside `WORK_DIR` following the analytics-engine registration pattern:
   - Scalar: `ScalarFunction` enum → `STANDARD_PROJECT_OPS` → adapter (prefer `AbstractNameMappingAdapter`) → yaml signature only if needed.
   - Aggregate: aggregate enum + ops surface; custom UDAF only when DataFusion lacks a native equivalent.
   - Window / set-op / WITHIN GROUP / QUALIFY / FILTER: planner rules that propagate the RelNode shape to DataFusion.
8. Write an E2E test driving the RelNode through the pipeline, per story:
   - **internalClusterTest** in `sandbox/plugins/analytics-backend-datafusion/src/internalClusterTest/java/org/opensearch/be/datafusion/` following `BaseScalarFunctionIT`: PPL via `client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet()` → `CalciteRelNodeVisitor` produces target RelNode → pipeline runs → assert row.
   - **REST IT** in `sandbox/qa/analytics-engine-rest/src/test/java/org/opensearch/analytics/qa/` following `MathScalarFunctionsIT`: `POST /_analytics/ppl` → assert `rows[0][0]` (or full row set for set-ops, window, etc.).
   - Reference commits: `f7bd4f492e1` (MATH scalar sweep), `239f4d92373` (string scalars). Read their diffs before coding your first scalar/aggregate.
9. Run the commands from `prd.json` inside `WORK_DIR`: `buildCommand`, `testCommand`, `verifyCommand`.
10. If all checks pass, commit inside `WORK_DIR` — conventional commits, one per function/feature: `feat(analytics-engine): add <FUNC> <scalar|aggregate|window|...>`. **DO NOT `git push`** unless `RALPH_PUSH=1` was set. The branch diff against `main` must contain ONLY that tier's files.
11. Back in the setup-branch checkout (the directory containing this prompt file), update `prd.json` — set `passes: true` for the story only when ALL acceptance criteria are verifiably met. Otherwise keep `passes: false` and update `notes` with concrete next steps.
12. Append to `progress.txt` in the setup-branch checkout.
13. If you find reusable patterns, consolidate them into the `## Codebase Patterns` section at the top of `progress.txt`.

## Path Discipline

- Loop state files (`prd.json`, `progress.txt`, `ralph-prompt.md`, `ralph.sh`) are edited ONLY on the `ralph/sql-setup` branch checkout (same directory as this prompt).
- Code / tests / commits for a story happen ONLY in `WORK_DIR`.
- NEVER commit loop state to a tier branch. NEVER commit tier code to the setup branch.
- A diff in `WORK_DIR` on `BRANCH` must only show that tier's changes.

## Progress Report Format

APPEND to `progress.txt`:

```
## [Date/Time] - [Story ID] (branch: <branchName>)
- What was implemented
- Files changed
- Commit shas: git -C work log --oneline main..HEAD
- IQ delta (if measured)
- **Learnings**: patterns / gotchas / context
---
```

## Quality Requirements

- Every commit must pass build + tests.
- No broken code committed.
- Do NOT modify or delete existing tests; only add.
- Do NOT touch files owned by other tiers.
- Preserve existing comments, Javadoc, and logging.
- Every new function MUST have both an internalClusterTest case AND a REST IT case.

## Failure Handling

If build or tests fail after 3 attempts:
1. Record failure details in `notes` in `prd.json`.
2. Leave `passes: false`.
3. Append details to `progress.txt`.
4. Move on — another iteration will resume.

If a tier is too large for one iteration, commit per-function partial progress, keep `passes: false`, and record precisely which sub-functions are done vs remaining in `notes`.

## Stop Condition

When ALL stories have `passes: true`, output the exact text `RALPH_COMPLETE` on its own line.

## Important

- ONE story per iteration.
- Commit per function/feature.
- **DO NOT PUSH** (unless `RALPH_PUSH=1`).
- SKIP L-tier items (correlated subqueries, LATERAL, RECURSIVE CTEs, UNNEST-as-TVF, PIVOT, ROLLUP/CUBE/GROUPING SETS, INTERVAL arith, EXCLUDE).
- Read `## Codebase Patterns` BEFORE starting.
- Never attempt multiple stories in one iteration.
