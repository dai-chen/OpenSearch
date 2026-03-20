# Ralph Agent Instructions

You are an autonomous coding agent working on a software project.

## Your Task

1. Read the PRD at `prd.json`
2. Read the progress log at `progress.txt` (check **Codebase Patterns** and **GUARDRAILS** sections FIRST)
3. Check for any **Round Summary** or **Human Intervention** sections at the end of `progress.txt` — these describe changes made outside the loop that you must account for
4. Check you're on the correct branch from PRD `branchName`. If not, check it out or create from main.
5. Pick the **highest priority** user story where `passes: false`
6. Implement that single user story
7. Run build: `./gradlew :modules:query-languages:build`
8. Run tests: `./gradlew :modules:query-languages:test`
9. If a verification command is configured: `./gradlew :modules:query-languages:internalClusterTest`
10. If checks pass, commit and push ALL changes: `git add -A && git commit -m "feat: [{story.id}] {story.title}" && git push`
11. Update `prd.json` — set `passes: true` for the completed story
12. Append your progress to `progress.txt`
13. If you discover reusable patterns, consolidate them into the Codebase Patterns section at the top of `progress.txt`

## Project Context

This is a PoC to bring SQL/PPL as first-class query languages into OpenSearch core. The module `modules/query-languages/` registers `_sql` and `_ppl` REST endpoints that use the unified query API (`UnifiedQueryPlanner` → `UnifiedQueryCompiler` → `PreparedStatement` execution) with `OpenSearchIndex` as the Calcite Table implementation.

### Key References
- Design doc: `docs/plans/2026-03-19-sql-ppl-core-design.md`
- Unified query IT pattern: https://github.com/opensearch-project/sql/blob/main/integ-test/src/test/java/org/opensearch/sql/api/UnifiedQueryOpenSearchIT.java
- OpenSearch module source: https://github.com/opensearch-project/sql/tree/main/opensearch/src/main/java/org/opensearch/sql/opensearch
- PoC PR for reference: https://github.com/dai-chen/sql-1/pull/10
- Existing module examples: `modules/reindex/`, `modules/lang-painless/`
- Maven repo for unified-query-api: check `sandbox/plugins/analytics-engine/build.gradle` for exact URL and coordinates

### Critical Guardrails
- **ONLY modify files inside `modules/query-languages/`** (exception: `settings.gradle` for module registration in US-001 only)
- **Do NOT modify any existing module, plugin, server code, or sandbox code**
- **Do NOT modify `sandbox/plugins/analytics-engine/`** — it is unrelated
- **Do NOT delete or modify any existing tests**
- **Resolve jar conflicts ONLY in this module's build.gradle** — never touch other build.gradle files
- **Keep implementation minimal** — this is a PoC, not production code
- **Use hardcoded defaults** — no configurable plugin settings

## Progress Report Format

APPEND to progress.txt (never replace, always append):
```
## [Date/Time] - [Story ID]
- What was implemented
- Files changed
- **Learnings for future iterations:**
  - Patterns discovered (e.g., "this codebase uses X for Y")
  - Gotchas encountered (e.g., "don't forget to update Z when changing W")
  - Useful context (e.g., "the evaluation panel is in component X")
---
```

The learnings section is critical — it helps future iterations avoid repeating mistakes and understand the codebase better.

## Consolidate Patterns

If you discover a **reusable pattern** that future iterations should know, add it to the `## Codebase Patterns` section at the TOP of progress.txt (create it if it doesn't exist). This section should consolidate the most important learnings:

```
## Codebase Patterns
- Example: Use `sql<number>` template for aggregations
- Example: Always use `IF NOT EXISTS` for migrations
- Example: Export types from actions.ts for UI components
```

Only add patterns that are **general and reusable**, not story-specific details.

## Human Intervention Notes

If you see a `## Human Intervention` section in `progress.txt`, a human made changes between rounds. Read it carefully — it explains what was changed and why. Account for these changes in your implementation. Do not undo or conflict with human changes.

## Quality Requirements

- ALL commits must pass build and tests
- Do NOT commit broken code
- Keep changes focused and minimal
- Follow existing code patterns
- Never delete or skip tests to make build pass

## Failure Handling

If build or tests fail after 3 attempts at fixing:
1. Record the failure details in the story's `notes` field in prd.json
2. Leave `passes: false`
3. Append failure details to progress.txt with what was tried
4. Move on — another iteration (or a human) will address it

## Stop Condition

After completing a user story, check if ALL stories have `passes: true`.

If ALL stories are complete and passing, output the exact text `RALPH_COMPLETE` on its own line (no other text on that line).

If there are still stories with `passes: false`, end your response normally (another iteration will pick up the next story).

## Important

- Work on **ONE story per iteration**
- Commit frequently
- Keep CI green
- Read the Codebase Patterns and GUARDRAILS sections in progress.txt BEFORE starting
- Do NOT attempt multiple stories in one iteration
