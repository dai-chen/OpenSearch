# SQL/PPL to OpenSearch Core — PoC Design

## Goal

Prove that SQL/PPL can be a first-class query language in OpenSearch core, decoupled from the SQL plugin, by leveraging the unified query API.

## Motivation

Decouple the OpenSearch SQL/PPL language from the SQL plugin's OS core engine via a unified query API. This treats SQL and PPL as first-class query languages at the same level as OpenSearch DSL.

## Module

`modules/query-languages/`

A new self-contained OpenSearch module included in every build. The module owns REST handlers, transport actions, and the query execution pipeline.

## Architecture

```
POST _sql / _ppl
  → RestSqlAction / RestPplAction          [REST handlers, registered via ActionPlugin.getRestHandlers()]
  → TransportQueryLanguageAction           [transport action]
  → Build Calcite AbstractSchema           [lazy OpenSearchIndex from cluster state]
  → UnifiedQueryContext.builder()          [configure language, catalog, settings]
  → UnifiedQueryPlanner.plan(query)        [parse → RelNode]
  → UnifiedQueryCompiler.compile(relNode)  [RelNode → PreparedStatement]
  → statement.executeQuery()               [execute → ResultSet]
  → Format ResultSet → REST response
```

## Module Contents

### Plugin Class
- Implements `ActionPlugin`
- Registers REST handlers via `getRestHandlers()`
- Registers transport actions via `getActions()`

### REST Handlers
- `RestSqlAction` — registers `POST /_sql`, parses request body (query string), delegates to transport action
- `RestPplAction` — registers `POST /_ppl`, same pattern

### Transport Action
- `TransportQueryLanguageAction` — orchestrates the unified query pipeline:
  1. Builds Calcite `AbstractSchema` from cluster state (lazy `OpenSearchIndex` creation)
  2. Creates `UnifiedQueryContext` with language type, catalog, default settings
  3. Plans via `UnifiedQueryPlanner.plan(query)` → `RelNode`
  4. Compiles via `UnifiedQueryCompiler.compile(relNode)` → `PreparedStatement`
  5. Executes `statement.executeQuery()` → `ResultSet`
  6. Formats `ResultSet` into REST response

### Schema Builder
- `AbstractSchema` implementation that lazily creates `OpenSearchIndex` tables on demand
- Uses `OpenSearchNodeClient` + cluster state to resolve index metadata
- No `DataSourceService` or `StorageEngine` coupling (cleaned up from sql repo's opensearch module)

### OpenSearch Module Code (from opensearch-project/sql opensearch module)
- `OpenSearchIndex` — Calcite `Table` implementation enabling pushdown optimization
- `OpenSearchClient` / `OpenSearchNodeClient` — client abstractions
- Minimal dependency chain: request builders, response parsers, index mapping classes
- Cleaned up to remove `DataSourceService`/`StorageEngine` coupling

## Dependencies

### build.gradle
- `unified-query-api` — external Maven dependency from `ci.opensearch.org` snapshots repo
  - Provides: `UnifiedQueryPlanner`, `UnifiedQueryCompiler`, `UnifiedQueryContext`, `CalcitePlanContext`
  - Transitive: Calcite, ANTLR, PPL/SQL parsers, etc.
- OpenSearch server — provided (compileOnly)

### Jar Conflict Resolution
- `resolutionStrategy.force` for version alignment (Jackson, Guava, commons-codec, etc.)
- Exclusions in `bundlePlugin` for jars already on the server classpath
- Same pattern proven in the SQL plugin PoC (dai-chen/sql-1#10)

## Integration Tests

### SQL IT
```sql
SELECT firstname, age FROM <index> WHERE age > 30
```
- `POST _sql` with query body
- Verify response contains correct hits

### PPL IT
```
source = <index> | fields firstname, age | where age > 30
```
- `POST _ppl` with query body
- Verify response contains correct hits

## Out of Scope (PoC)

- Explain support (`_sql/_explain`, `_ppl/_explain`)
- Cursor/pagination
- Async query execution
- Settings management (hardcoded defaults for PoC)
- Error formatting (basic error responses only)
- SQL/PPL feature parity with the SQL plugin

## References

- Unified query API IT: https://github.com/opensearch-project/sql/blob/main/integ-test/src/test/java/org/opensearch/sql/api/UnifiedQueryOpenSearchIT.java
- PoC branch (SQL plugin ↔ analytics-engine): https://github.com/dai-chen/sql-1/pull/10
- OpenSearch module in sql repo: https://github.com/opensearch-project/sql/tree/main/opensearch
