# PoC: running the analytics engine on a plain Lucene index

Status: proof of concept, measured on a live 1-node cluster. Not production-ready — see
[Limits](#limits).

## Problem

The analytics engine (AE) could only read indices backed by the composite engine. On an ordinary
OpenSearch index every PPL query failed, and the reason went deeper than a missing reader: AE's
operator rules intersect each operator's capability set with **its child's** viable backends
(`OpenSearchSortRule:56-62` and twelve sibling rules do this). A scan that resolved to Lucene alone
therefore narrowed everything above it to Lucene, so sort, window functions, `CAST` and aggregation
had nowhere to run even though DataFusion was sitting one stage above.

Before this work, on a plain index:

| PPL | via `/_analytics/ppl` | via `/_plugins/_ppl` |
| --- | --- | --- |
| `fields name` | fail | fail |
| `where name = 'x'` | fail | fail |
| `stats count()` | fail | fail |
| `sort - id \| head 10` | fail | fail |
| `dedup name` | fail | fail |

Representative errors: `No backend supports SORT capability among [lucene]`,
`COORDINATOR_REDUCE stage 1 expected exactly one plan alternative, got 0`,
`Function [CAST] is not currently supported as a scalar function`, and — at the very bottom —
`EngineBackedIndexer.acquireReader` threw `UnsupportedOperationException`.

## Approach

One index concept, and divergence driven by the **data format id** alone:

- `lucene` — Lucene as an inverted index. On a composite index with a non-Lucene primary this is the
  secondary segment: postings only, no doc values, no indexed numerics.
- `lucene_doc_values` — Lucene as a source of row values, on an index that never opted into a
  pluggable data format.

Two roles for Lucene, chosen per fragment:

1. **Driver** — Lucene executes the shard fragment itself. Used for metadata counts, postings
   filters and plain projections.
2. **Value source** — Lucene implements `ValueScanProducer` and feeds Arrow batches into
   DataFusion's scan leaf, registered as a streaming table under the same name the fragment's
   Substrait `NamedTable` references. Every operator above the leaf is then ordinary DataFusion
   execution.

Role 2 is what removes the narrowing: DataFusion declares `lucene_doc_values` among its capability
formats, so the scan reports `viableBackends=[[lucene, datafusion]]` and DataFusion survives to the
top of the plan.

## Commits

| Commit | What |
| --- | --- |
| `e86d724` | Plain shard hands out a reader; `lucene_doc_values` format id; Lucene doc-values → Arrow; composite plugin stops stamping its primary format onto every index |
| `4fa0d0f` | Refactor: format id becomes the single discriminator at the reader seam; one format-keyed adapter; static reader cache removed |
| `2f0999f` | Window projects require hash distribution on their `PARTITION BY` keys (Drill / Flink / Ignite three-way choice) |
| `af508ec` | `ValueScanProducer` SPI; DataFusion runs a shard fragment over a Lucene-fed leaf |
| `642e17d` | Rank-limited windows pushed below the exchange (Spark `InsertWindowGroupLimit` / Flink local-global `Rank`) |

## Test setup

```bash
curl -s -X PUT "localhost:9200/logs" -H 'Content-Type: application/json' -d '{
  "settings": { "number_of_shards": 3, "number_of_replicas": 0 },
  "mappings": { "properties": {
      "user":         { "type": "keyword" },
      "severityText": { "type": "keyword" },
      "latency":      { "type": "long"    } } }
}'
```

3000 docs, 20 distinct users at 150 docs each; `severityText=ERROR` for the 16 users whose id is not
a multiple of 5 (2400 ERROR docs). A `users` index holds 20 rows mapping `user` → `team{id%4}`.
A composite twin (`parquet` primary + `lucene` secondary) with identical data is used as the
correctness oracle.

The index is plain — `index.pluggable.dataformat.enabled: false` and no
`index.composite.primary_data_format`.

## Results

### What runs, and where

`rows out` is `rows_processed` on the shard fragment: what crosses the network.

| PPL | shard backend | rows out | result |
| --- | --- | --- | --- |
| `fields user` | lucene | 3000 | 3000 rows |
| `fields user, latency` | lucene | 3000 | 3000 rows |
| `where user = 'user3'` | lucene | 150 | 150 rows |
| `where latency > 500` | datafusion | 1497 | 1497 rows |
| `sort - latency \| head 10` | **datafusion** | **30** | 10 rows |
| `stats count() as c by user` | **datafusion** | **60** | 20 rows |
| `stats count() as c by user \| sort - c \| head 5` | **datafusion** | **60** | 5 rows |
| `where … \| eventstats count() by user \| sort - c \| head 10` | lucene | 2400 | 10 rows |
| `join users \| stats count() by team` | lucene | 3000 | 4 rows |

The two rows that matter most are `sort … | head` and `stats … by`: **30 and 60 rows leave the
shards instead of 3000**, because DataFusion now runs a shard-local `SortExec: TopK(fetch=10)` and
`AggregateExec: mode=Partial` directly over Lucene doc values. Neither shape could be planned at all
before.

### Rank-limited window pushdown

`642e17d` pushes a rank-like window and its rank bound below the exchange.

| PPL | rows out before | rows out after | result |
| --- | --- | --- | --- |
| `dedup user` (plain) | 3000 | **60** | 20 rows |
| `dedup 2 user` (plain) | 3000 | **120** | 40 rows |
| `dedup user` (composite) | 3000 | **60** | 20 rows |
| `eventstats count() by user \| sort - c \| head 5` | 3000 | 3000 (unchanged) | 5 rows |

60 = 20 partitions × 3 shards, each shard emitting at most one row per partition — a 50× reduction.
Results are identical to the pre-change plan in every case.

Request and response:

```bash
curl -s -X POST "localhost:9200/_analytics/ppl" \
  -H 'Content-Type: application/json' \
  -d '{"query": "source=logs | dedup user | fields user"}'
```

```json
{"columns": ["user"],
 "rows": [["user0"],["user1"],["user14"],["user19"],["user9"],["user11"],["user16"],
          ["user18"],["user2"],["user3"],["user8"],["user13"],["user15"],["user4"],
          ["user5"],["user10"],["user12"],["user17"],["user6"],["user7"]]}
```

The plan carries the window twice — the pushed copy is the `_local_rank_` one below the exchange:

```
OpenSearchProject(user=[$0])
  OpenSearchFilter(ANNOTATED_PREDICATE(id=1, <=($1, 1)))                       <- coordinator authority
    OpenSearchProject(user=[$2], _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY $2)])
      OpenSearchExchangeReducer(SINGLETON)
        OpenSearchProject(latency, severityText, user)                          <- drops local rank
          OpenSearchFilter(<=($3, 1))                                           <- pushed rank bound
            OpenSearchProject(..., _local_rank_=[ROW_NUMBER() OVER (PARTITION BY $2)])  <- pushed window
              OpenSearchFilter(IS NOT NULL($2))
                OpenSearchTableScan(logs, viableBackends=[[lucene, datafusion]])
```

Shard fragment — 60 rows out of 3000, window evaluated locally over a Lucene-fed leaf:

```
stage 0  SHARD_FRAGMENT  backend=datafusion  rows_processed=60  tasks=3

FilterExec: row_number() PARTITION BY [logs.user] … @3 <= 1, projection=[latency@0, severityText@1, user@2]
  BoundedWindowAggExec: wdw=[row_number() PARTITION BY [logs.user] …], mode=[Sorted]
    SortExec: expr=[user@2 ASC NULLS LAST], preserve_partitioning=[true]
      RepartitionExec: partitioning=Hash([user@2], 4), input_partitions=4
        FilterExec: user@2 IS NOT NULL
          RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            StreamingTableExec: partition_sizes=1, projection=[latency, severityText, user]
```

Coordinator re-runs the window as the final authority:

```
stage 1  COORDINATOR_REDUCE  backend=datafusion  tasks=1

FilterExec: row_number() PARTITION BY [input-0.user] … @1 <= 1, projection=[user@0]
  BoundedWindowAggExec: wdw=[row_number() PARTITION BY [input-0.user] …], mode=[Sorted]
```

**Soundness.** For a rank-like function the local rank of a row never exceeds its global rank:
adding other shards' rows to a partition can only push a row further down. So every row satisfying
`global_rank <= N` also satisfies `local_rank <= N`, and the shard filter discards only rows the
coordinator would discard anyway. The same rule appears in Spark (`InsertWindowGroupLimit`, for
`RowNumber`/`Rank`/`DenseRank` under a rank predicate) and Flink (batch `Rank` planned as local rank
before the exchange, global rank after).

**Why `eventstats` is excluded.** Its predicate is a row-level Top-N over an *additive* measure. A
shard's partial `COUNT` is not the global count, so a locally-failing row may pass globally —
filtering locally would change the answer. Spark draws the same line. Note also that `eventstats`
and `stats` are not interchangeable: `stats count() by user | sort - c | head 5` returns **5 distinct
users**, while `eventstats count() by user | sort - c | head 5` returns **5 rows of the same user**,
because `eventstats` annotates every row and the Top-N is over rows.

### Hybrid split: `where` + `join` + `stats`

```bash
curl -s -X POST "localhost:9200/_analytics/ppl" -H 'Content-Type: application/json' -d '{
  "query": "source=logs | where severityText='"'"'ERROR'"'"' | join left=L right=U on L.user = U.user users | stats count() as c by U.team"}'
```

```json
{"columns": ["c", "U.team"],
 "rows": [[600, "team1"], [600, "team0"], [600, "team3"], [600, "team2"]]}
```

Verifiable by arithmetic: 16 ERROR users × 150 docs = 2400 joined rows; 4 ERROR users per team × 150
= 600 each.

```
stage 0  SHARD_FRAGMENT      backend=lucene      rows_processed=2400  tasks=3
           Project(severityText, user) <- Filter(severityText='ERROR') <- TableScan(logs)

stage 1  SHARD_FRAGMENT      backend=lucene      rows_processed=20    tasks=3
           TableScan(users)

stage 2  COORDINATOR_REDUCE  backend=datafusion  tasks=1
           ProjectionExec
             AggregateExec: mode=FinalPartitioned, gby=[team@0]
               RepartitionExec: partitioning=Hash([team@0], 4)
                 AggregateExec: mode=Partial, gby=[team@0]
                   HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(user@1, user@0)]
```

Lucene applies the `where` on the `logs` side (2400 of 3000 rows leave the shards) and does an
unfiltered doc-value scan on the `users` side; DataFusion owns the join and the aggregation. This is
the intended division of labour: Lucene as the storage-and-postings layer, DataFusion as the query
engine.

## Limits

- **The join runs on the coordinator.** `HashJoinExec` sits in `COORDINATOR_REDUCE` with
  `mode=CollectLeft`. AE only places a join on worker nodes through the MPP hash-shuffle worker
  tier, which is off by default.
- **Window distribution needs MPP explicitly enabled.** `2f0999f` makes a partitioned window request
  hash distribution, but `DistributionEnforcementPass` is inert unless `analytics.mpp.enabled=true`,
  `analytics.mpp.distribute.min_rows` is lowered from its 1,000,000 default, and
  `analytics.mpp.shuffle.partitions` yields `partitionCount > 1`. Even then the window executes on a
  single node: worker promotion in `GeneralShuffleDAGRewriter` is driven entirely by
  `JoinShuffleInfo`'s two named inputs, so a unary shuffle consumer never becomes a
  `WORKER_FRAGMENT`.
- **QTF / late materialization is rejected on a producer-fed shard**, not mis-executed: row ids are a
  parquet concept and a producer's rows carry no such identity.
- **Types**: `keyword` and `long` only. `text` has no doc values; `date` and floating-point types are
  unimplemented.
- **Dead scaffolding remains.** The Lucene driver value-scan path is still selected for projection and
  filter fragments, so `LuceneFragmentKind.VALUE_SCAN`, `VALUE_SCAN_SCORE`, the cross-backend gather
  fallback and the derived-column filter exclusion are all still load-bearing. Removing them requires
  handling filter delegation on a producer-fed shard first — `ShardScanWithDelegationHandler` needs a
  native reader that such a shard does not have.

## Verification

- **2277 unit tests, 0 failures** — analytics-framework 120, analytics-engine 1040,
  analytics-backend-lucene 324, analytics-backend-datafusion 793.
- `spotlessJavaCheck` and `forbiddenApisMain` clean for server and all sandbox modules.
- Results match the composite twin for every deterministic query. Queries with unstable output
  (`head` without `sort`, or `sort` on a column where all values tie) differ run to run on the *same*
  index and are compared using a deterministic variant instead.
- **Not verified: the `analytics-engine-rest` IT suites.** Installing the cached
  `opensearch-sql-plugin` snapshot fails with jackson jar hell before any test executes — the distro
  ships both `jackson-core-2.22.1.jar` and `jackson-core-3.2.1.jar` in `lib/` while the plugin zip
  bundles `2.22.1`. It fails identically with these changes stashed, so it is version skew between
  snapshots rather than a regression, but it means `integTest` (132 classes / 1274 tests) and
  `integTestPlanShape` (86 tests) are unconfirmed for the last two commits.

## Routing note

No SQL-plugin change is needed to reach AE from `/_plugins/_ppl`.
`RestUnifiedQueryAction.isAnalyticsIndex` short-circuits its per-index composite check when
`cluster.pluggable.dataformat=composite`, returning true for every target except the system catalog
and the `rest` command's reserved source. `gradle/run.gradle` already sets that value, and its
comment notes it does not make indices composite — an ordinary index stays a plain Lucene index.
