# Ralph Loop — OpenSearch SQL Weekend Gap Attack

This branch (`ralph/sql-setup`) contains everything needed to drive the autonomous SQL gap-attack loop.

## Dev host pickup

```bash
git clone <your-fork-of-OpenSearch>
cd OpenSearch
git checkout ralph/sql-setup
./ralph.sh                          # that's it
```

`ralph.sh` maintains a single inner worktree at `./work/` (gitignored). For each iteration it selects the next unfinished story from `prd.json`, resolves that story's tier branch:

- if the branch exists locally → use it;
- else if it exists on `origin` → fetch it;
- else → create it **fresh from `origin/main` (or `main`)**.

Then the worktree is checked out to that branch and an agent implements + commits there. Loop state (`prd.json`, `progress.txt`) lives on this setup branch. **Only `ralph/sql-setup` needs to be pushed to the dev host in advance** — tier branches come into existence when the loop works on them.

## Flags

```
./ralph.sh --agent general          # change agent (default: sisyphus)
./ralph.sh --max 5                  # cap iterations
./ralph.sh --only US-T0             # force one story
touch STOP_RALPH                    # graceful stop
```

## Env

- `RALPH_WORK_DIR` — absolute path for the inner worktree (default: `./work`).
- `RALPH_PUSH=1` — push tier commits to origin after each iteration (default: DISABLED per weekend plan).

## Files on this branch

- `ralph.sh` — loop runner
- `prd.json` — 10 user stories (one per tier), with per-story `branchName`
- `ralph-prompt.md` — per-iteration instructions for the agent
- `progress.txt` — append-only learnings log
- `.gitignore` — inner worktree (`/work/`) and stop-file are ignored

## The 10 tier branches (lazily created from `main`)

Each tier branch is created from `main` (or `origin/main`) on demand the first time the loop works on its story. Each aims to become its own reviewable PR with an isolated diff:

- `ralph/sql-t0-sugar-aggregates` — COUNTIF, BIT_*, COVAR_*, EVERY, SOME, ANY_VALUE
- `ralph/sql-t1-simple-scalars` — INITCAP, OCTET_LENGTH, OVERLAY, LOCALTIME, LOCALTIMESTAMP, CARDINALITY
- `ralph/sql-t2-window-lag-lead` — LAG, LEAD, named WINDOW
- `ralph/sql-t3-string-arg-aggs` — LISTAGG, STRING_AGG, GROUP_CONCAT, ARG_MAX, ARG_MIN
- `ralph/sql-t4-qualify-filter` — QUALIFY, FILTER(WHERE)
- `ralph/sql-t5-unsigned-overflow` — UNSIGNED types + integer overflow
- `ralph/sql-t6-cast-format` — `CAST(x AS type FORMAT 'pattern')` (highest impact)
- `ralph/sql-t7-regression-aggs` — REGR_COUNT, REGR_SXX, REGR_SYY, REGR_SXY
- `ralph/sql-t8-set-ops` — INTERSECT / EXCEPT (ALL / DISTINCT)
- `ralph/sql-t9-within-group` — WITHIN GROUP / WITHIN DISTINCT

Scope is the analytics-engine side only (`sandbox/plugins/analytics-engine`, `sandbox/plugins/analytics-backend-datafusion`, `sandbox/libs/analytics-framework`, `sandbox/qa/analytics-engine-rest`). Reference PRs: `f7bd4f492e1` (MATH scalar sweep), `239f4d92373` (string scalars).
