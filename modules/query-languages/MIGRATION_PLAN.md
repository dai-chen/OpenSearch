# Migration Plan: Remove `unified-query-opensearch` Dependency from Core

## Goal

Replace the external `unified-query-opensearch` dependency in `modules/query-languages` with
core-native code that uses OpenSearch internal APIs directly. The SQL plugin's `opensearch`
module was designed for plugin use (REST client abstraction, classloader isolation). Running
inside core as a module, we have direct `NodeClient` access and should use it.

## Current State

`modules/query-languages/build.gradle` depends on four `unified-query-*` artifacts:

| Artifact | Scope | Keep? |
|----------|-------|-------|
| `unified-query-api` | implementation | **YES** — parser/planner/compiler APIs |
| `unified-query-core` | implementation | **YES** — core expression types, settings |
| `unified-query-ppl` | implementation | **YES** — PPL parser |
| `unified-query-opensearch` | implementation | **NO** — replace with core-native code |

`TransportQueryLanguageAction` imports exactly 2 classes from `unified-query-opensearch`:
- `org.opensearch.sql.opensearch.client.OpenSearchNodeClient`
- `org.opensearch.sql.opensearch.storage.OpenSearchIndex`

But these two classes pull in a deep dependency chain: `CalciteLogicalIndexScan`,
`CalciteEnumerableIndexScan`, `OpenSearchIndexEnumerator`, `OpenSearchRequestBuilder`,
`OpenSearchQueryRequest`, `OpenSearchResponse`, `PredicateAnalyzer`, `AggregateAnalyzer`,
`OpenSearchDataType`, `OpenSearchExprValueFactory`, `PushDownContext`, ~15 Calcite planner
rules, and script engines.

## Source Reference

All source code to migrate lives in the SQL plugin repo at:
`/Users/daichen/IdeaProjects/main-sql/opensearch/src/main/java/org/opensearch/sql/opensearch/`

---

## Phase 1: Minimal Viable Bridge

**Goal**: Run `SELECT * FROM idx WHERE x = y LIMIT n` and `source = idx | where x > y`
without `unified-query-opensearch`. Support filter push-down, project push-down, and limit
push-down.

### Files to Migrate

Migrate from `main-sql/opensearch/` into `modules/query-languages/` under package
`org.opensearch.querylanguages.opensearch.*`, adapting as needed:

| Source File | Target Package | Notes |
|-------------|---------------|-------|
| `client/OpenSearchClient.java` | `.client` | Interface — simplify to only methods used |
| `client/OpenSearchNodeClient.java` | `.client` | Direct `NodeClient` usage, drop REST path |
| `mapping/IndexMapping.java` | `.mapping` | Simple wrapper, copy as-is |
| `data/type/OpenSearchDataType.java` | `.data.type` | Core type mapping |
| `data/type/OpenSearchDateType.java` | `.data.type` | Date handling |
| `data/type/OpenSearchTextType.java` | `.data.type` | Text/keyword subfield logic |
| `data/type/OpenSearchGeoPointType.java` | `.data.type` | Geo type |
| `data/type/OpenSearchBinaryType.java` | `.data.type` | Binary type |
| `data/type/OpenSearchAliasType.java` | `.data.type` | Alias type |
| `data/value/OpenSearchExprValueFactory.java` | `.data.value` | Hit → ExprValue conversion |
| `data/value/OpenSearchExprBinaryValue.java` | `.data.value` | Binary value |
| `data/value/OpenSearchExprGeoPointValue.java` | `.data.value` | Geo value |
| `data/value/OpenSearchExprTextValue.java` | `.data.value` | Text value |
| `data/utils/Content.java` | `.data.utils` | Content interface |
| `data/utils/ObjectContent.java` | `.data.utils` | Object content |
| `data/utils/OpenSearchJsonContent.java` | `.data.utils` | JSON content |
| `storage/OpenSearchIndex.java` | `.storage` | Calcite Table — the heart |
| `storage/scan/AbstractCalciteIndexScan.java` | `.storage.scan` | Base scan node |
| `storage/scan/CalciteLogicalIndexScan.java` | `.storage.scan` | Logical scan + push-down |
| `storage/scan/CalciteEnumerableIndexScan.java` | `.storage.scan` | Physical scan + Enumerable |
| `storage/scan/OpenSearchIndexEnumerator.java` | `.storage.scan` | Data fetcher |
| `storage/scan/context/PushDownContext.java` | `.storage.scan.context` | Deferred actions |
| `storage/scan/context/PushDownType.java` | `.storage.scan.context` | Enum |
| `storage/scan/context/PushDownOperation.java` | `.storage.scan.context` | Record |
| `storage/scan/context/AbstractAction.java` | `.storage.scan.context` | Action interface |
| `storage/scan/context/OSRequestBuilderAction.java` | `.storage.scan.context` | Request action |
| `storage/scan/context/FilterDigest.java` | `.storage.scan.context` | Filter digest |
| `storage/scan/context/LimitDigest.java` | `.storage.scan.context` | Limit digest |
| `request/OpenSearchRequest.java` | `.request` | Request interface |
| `request/OpenSearchQueryRequest.java` | `.request` | Query request impl |
| `request/OpenSearchScrollRequest.java` | `.request` | Scroll request impl |
| `request/OpenSearchRequestBuilder.java` | `.request` | Builds SearchSourceBuilder |
| `request/PredicateAnalyzer.java` | `.request` | RexNode → QueryBuilder |
| `request/system/OpenSearchDescribeIndexRequest.java` | `.request.system` | Describe index |
| `request/system/OpenSearchSystemRequest.java` | `.request.system` | System request interface |
| `response/OpenSearchResponse.java` | `.response` | Wraps SearchResponse |
| `monitor/OpenSearchMemoryHealthy.java` | `.monitor` | Memory health check |
| `monitor/OpenSearchResourceMonitor.java` | `.monitor` | Resource monitor |
| `monitor/MemoryUsage.java` | `.monitor` | Memory usage interface |
| `monitor/RuntimeMemoryUsage.java` | `.monitor` | Runtime memory impl |
| `monitor/GCedMemoryUsage.java` | `.monitor` | GC memory impl |
| `planner/rules/EnumerableIndexScanRule.java` | `.planner.rules` | Logical → physical |
| `planner/rules/FilterIndexScanRule.java` | `.planner.rules` | Filter push-down |
| `planner/rules/ProjectIndexScanRule.java` | `.planner.rules` | Project push-down |
| `planner/rules/LimitIndexScanRule.java` | `.planner.rules` | Limit push-down |
| `planner/rules/OpenSearchIndexRules.java` | `.planner.rules` | Rule registry |
| `util/JdbcOpenSearchDataTypeConvertor.java` | `.util` | Type conversion |

### Build Changes

- Remove `unified-query-opensearch` from `build.gradle` dependencies
- Add `jackson-databind` as implementation dep (needed by `OpenSearchExprValueFactory`)
- Keep `unified-query-api`, `unified-query-core`, `unified-query-ppl`
- Remove `unified-query-opensearch` license/SHA files

### Verification

- Existing ITs pass: `QueryLanguagesPplIT`, `QueryLanguagesSqlIT`
- `./gradlew :modules:query-languages:check` passes

---

## Phase 2: Aggregation Push-Down

**Goal**: Support `GROUP BY`, `stats`, `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` with push-down
to OpenSearch aggregation API.

### Files to Migrate

| Source File | Target Package | Notes |
|-------------|---------------|-------|
| `request/AggregateAnalyzer.java` | `.request` | Calcite Aggregate → AggregationBuilder |
| `request/AggregateFilterAnalyzer.java` | `.request` | Aggregate filter analysis |
| `request/CaseRangeAnalyzer.java` | `.request` | Case/range analysis |
| `planner/rules/AggregateIndexScanRule.java` | `.planner.rules` | Aggregate push-down rule |
| `storage/scan/context/AggPushDownAction.java` | `.storage.scan.context` | Agg action |
| `storage/scan/context/AggregationBuilderAction.java` | `.storage.scan.context` | Agg builder action |
| `response/agg/*.java` (~12 files) | `.response.agg` | Aggregation response parsers |
| `storage/script/aggregation/*.java` | `.storage.script.aggregation` | Script aggregation |
| `storage/script/aggregation/dsl/*.java` | `.storage.script.aggregation.dsl` | Agg DSL builders |

### Build Changes

- None expected beyond Phase 1

### Verification

- New ITs for `SELECT COUNT(*), AVG(age) FROM idx GROUP BY field`
- New ITs for `source = idx | stats count() by field`

---

## Phase 3: Sort, Collapse, Script, and Remaining Rules

**Goal**: Full feature parity with `unified-query-opensearch` for the Calcite engine path.

### Files to Migrate

| Source File | Target Package | Notes |
|-------------|---------------|-------|
| `planner/rules/SortIndexScanRule.java` | `.planner.rules` | Sort push-down |
| `planner/rules/SortProjectExprTransposeRule.java` | `.planner.rules` | Sort+project |
| `planner/rules/SortAggregateMeasureRule.java` | `.planner.rules` | Sort+agg |
| `planner/rules/DedupPushdownRule.java` | `.planner.rules` | Dedup/collapse |
| `planner/rules/RareTopPushdownRule.java` | `.planner.rules` | Rare/top push-down |
| `planner/rules/RelevanceFunctionPushdownRule.java` | `.planner.rules` | match(), match_phrase() |
| `planner/rules/ExpandCollationOnProjectExprRule.java` | `.planner.rules` | Collation expansion |
| `planner/rules/EnumerableSystemIndexScanRule.java` | `.planner.rules` | System index scan |
| `storage/scan/context/RareTopDigest.java` | `.storage.scan.context` | Rare/top digest |
| `storage/script/filter/*.java` | `.storage.script.filter` | Filter scripts |
| `storage/script/sort/SortQueryBuilder.java` | `.storage.script.sort` | Sort scripts |
| `storage/script/core/*.java` | `.storage.script.core` | Script core |
| `storage/script/*ScriptEngine.java` | `.storage.script` | Script engines |
| `storage/system/*.java` | `.storage.system` | System index support |
| `storage/serde/*.java` | `.storage.serde` | Cursor serialization |
| `response/error/*.java` | `.response.error` | Error messages |
| `security/SecurityAccess.java` | `.security` | Security context |
| `setting/OpenSearchSettings.java` | `.setting` | Dynamic settings |

### Build Changes

- Replace `HardcodedSettings` with proper `OpenSearchSettings` wired to cluster settings
- Register script engines in plugin

### Verification

- ITs for `ORDER BY`, `LIMIT offset`, `dedup`, `rare`, `top`
- ITs for relevance functions: `match()`, `match_phrase()`
- ITs for `SHOW TABLES`, `DESCRIBE table`
- Full `./gradlew :modules:query-languages:check` passes

---

## Success Criteria

- `unified-query-opensearch` completely removed from `build.gradle`
- All existing ITs continue to pass
- No runtime dependency on external SQL plugin opensearch module
- `unified-query-api`, `unified-query-core`, `unified-query-ppl` remain (language-level concerns)
