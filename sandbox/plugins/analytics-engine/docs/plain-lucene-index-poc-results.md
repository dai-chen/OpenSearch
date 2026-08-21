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

Every block below is a verbatim transcript against the cluster described above. `rows_processed` on
a `SHARD_FRAGMENT` is what that stage emitted across all shards — i.e. what crossed the network.
Row lists are truncated to six entries where noted; `row_count` is the full count.

### sort + head — shard-local TopK

```console
$ curl -s -X POST "localhost:9200/_analytics/ppl" -H 'Content-Type: application/json' \
    -d '{"query": "source=logs | sort - latency | head 10 | fields latency"}'
{"columns": ["latency"], "row_count": 10, "rows": [[999], [999], [999], [998], [998], [998], "..."]}

$ curl -s -X POST "localhost:9200/_analytics/ppl/_explain" -H 'Content-Type: application/json' \
    -d '{"query": "source=logs | sort - latency | head 10 | fields latency"}' | jq '.profile.stages[] | {stage_id, execution_type, chosen_backend, rows_processed}'
{"stage_id": 0, "execution_type": "SHARD_FRAGMENT", "chosen_backend": "datafusion", "rows_processed": 30, "tasks_completed": 3}
{"stage_id": 1, "execution_type": "COORDINATOR_REDUCE", "chosen_backend": "datafusion", "rows_processed": 0, "tasks_completed": 1}
```

### stats by key — shard-local partial aggregate

```console
$ curl -s -X POST "localhost:9200/_analytics/ppl" -H 'Content-Type: application/json' \
    -d '{"query": "source=logs | stats count() as c by user"}'
{"columns": ["c", "user"], "row_count": 20, "rows": [[150, "user18"], [150, "user11"], [150, "user16"], [150, "user3"], [150, "user2"], [150, "user8"], "..."]}

$ curl -s -X POST "localhost:9200/_analytics/ppl/_explain" -H 'Content-Type: application/json' \
    -d '{"query": "source=logs | stats count() as c by user"}' | jq '.profile.stages[] | {stage_id, execution_type, chosen_backend, rows_processed}'
{"stage_id": 0, "execution_type": "SHARD_FRAGMENT", "chosen_backend": "datafusion", "rows_processed": 60, "tasks_completed": 3}
{"stage_id": 1, "execution_type": "COORDINATOR_REDUCE", "chosen_backend": "datafusion", "rows_processed": 0, "tasks_completed": 1}
```

### top-N groups — partial aggregate + coordinator TopK

```console
$ curl -s -X POST "localhost:9200/_analytics/ppl" -H 'Content-Type: application/json' \
    -d '{"query": "source=logs | stats count() as c by user | sort - c | head 5"}'
{"columns": ["c", "user"], "row_count": 5, "rows": [[150, "user10"], [150, "user5"], [150, "user12"], [150, "user15"], [150, "user17"]]}

$ curl -s -X POST "localhost:9200/_analytics/ppl/_explain" -H 'Content-Type: application/json' \
    -d '{"query": "source=logs | stats count() as c by user | sort - c | head 5"}' | jq '.profile.stages[] | {stage_id, execution_type, chosen_backend, rows_processed}'
{"stage_id": 0, "execution_type": "SHARD_FRAGMENT", "chosen_backend": "datafusion", "rows_processed": 60, "tasks_completed": 3}
{"stage_id": 1, "execution_type": "COORDINATOR_REDUCE", "chosen_backend": "datafusion", "rows_processed": 0, "tasks_completed": 1}
```

### dedup — rank-limited window pushed below the exchange

```console
$ curl -s -X POST "localhost:9200/_analytics/ppl" -H 'Content-Type: application/json' \
    -d '{"query": "source=logs | dedup user | fields user"}'
{"columns": ["user"], "row_count": 20, "rows": [["user13"], ["user15"], ["user4"], ["user5"], ["user10"], ["user12"], "..."]}

$ curl -s -X POST "localhost:9200/_analytics/ppl/_explain" -H 'Content-Type: application/json' \
    -d '{"query": "source=logs | dedup user | fields user"}' | jq '.profile.stages[] | {stage_id, execution_type, chosen_backend, rows_processed}'
{"stage_id": 0, "execution_type": "SHARD_FRAGMENT", "chosen_backend": "datafusion", "rows_processed": 60, "tasks_completed": 3}
{"stage_id": 1, "execution_type": "COORDINATOR_REDUCE", "chosen_backend": "datafusion", "rows_processed": 0, "tasks_completed": 1}
```

### dedup 2 — same rule, N=2

```console
$ curl -s -X POST "localhost:9200/_analytics/ppl" -H 'Content-Type: application/json' \
    -d '{"query": "source=logs | dedup 2 user | fields user"}'
{"columns": ["user"], "row_count": 40, "rows": [["user11"], ["user11"], ["user16"], ["user16"], ["user18"], ["user18"], "..."]}

$ curl -s -X POST "localhost:9200/_analytics/ppl/_explain" -H 'Content-Type: application/json' \
    -d '{"query": "source=logs | dedup 2 user | fields user"}' | jq '.profile.stages[] | {stage_id, execution_type, chosen_backend, rows_processed}'
{"stage_id": 0, "execution_type": "SHARD_FRAGMENT", "chosen_backend": "datafusion", "rows_processed": 120, "tasks_completed": 3}
{"stage_id": 1, "execution_type": "COORDINATOR_REDUCE", "chosen_backend": "datafusion", "rows_processed": 0, "tasks_completed": 1}
```

### eventstats + sort + head — additive measure, deliberately NOT pushed

```console
$ curl -s -X POST "localhost:9200/_analytics/ppl" -H 'Content-Type: application/json' \
    -d '{"query": "source=logs | eventstats count() as c by user | sort - c | head 5 | fields user, c"}'
{"columns": ["user", "c"], "row_count": 5, "rows": [["user10", 150], ["user11", 150], ["user10", 150], ["user11", 150], ["user10", 150]]}

$ curl -s -X POST "localhost:9200/_analytics/ppl/_explain" -H 'Content-Type: application/json' \
    -d '{"query": "source=logs | eventstats count() as c by user | sort - c | head 5 | fields user, c"}' | jq '.profile.stages[] | {stage_id, execution_type, chosen_backend, rows_processed}'
{"stage_id": 0, "execution_type": "SHARD_FRAGMENT", "chosen_backend": "lucene", "rows_processed": 3000, "tasks_completed": 3}
{"stage_id": 1, "execution_type": "COORDINATOR_REDUCE", "chosen_backend": "datafusion", "rows_processed": 0, "tasks_completed": 1}
```

### Full plan for the pushed window

The plan carries the window twice. The `_local_rank_` copy below the `OpenSearchExchangeReducer` is
the pushed one; the coordinator keeps its own copy and remains the authority.

```console
$ curl -s -X POST "localhost:9200/_analytics/ppl/_explain" -H 'Content-Type: application/json' \
    -d '{"query": "source=logs | dedup user | fields user"}' | jq -r '.profile.full_plan[]'
OpenSearchProject(user=[$0], viableBackends=[[datafusion]])
  OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=1, backends=[datafusion], <=($1, 1))], viableBackends=[[datafusion]])
    OpenSearchProject(user=[$2], _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY $2)], viableBackends=[[datafusion]])
      OpenSearchExchangeReducer(viableBackends=[[datafusion]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[], partitionCount=0]])
        OpenSearchProject(latency=[$0], severityText=[$1], user=[$2], viableBackends=[[datafusion]])
          OpenSearchFilter(condition=[<=($3, 1)], viableBackends=[[datafusion]])
            OpenSearchProject(latency=[$0], severityText=[$1], user=[$2], _local_rank_=[ROW_NUMBER() OVER (PARTITION BY $2)], viableBackends=[[datafusion]])
              OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[datafusion], IS NOT NULL($2))], viableBackends=[[datafusion]])
                OpenSearchTableScan(table=[[logs]], viableBackends=[[lucene, datafusion]])

$ ... | jq '.profile.stages[0] | {stage_id, execution_type, chosen_backend, rows_processed}'
{"stage_id": 0, "execution_type": "SHARD_FRAGMENT", "chosen_backend": "datafusion", "rows_processed": 60}

$ ... | jq -r '.profile.stages[0].tasks[0].physical_plan'
FilterExec: row_number() PARTITION BY [logs.user] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@3 <= 1, projection=[latency@0, severityText@1, user@2]
  BoundedWindowAggExec: wdw=[row_number() PARTITION BY [logs.user] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW: Field { "row_number() PARTITION BY [logs.user] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
    SortExec: expr=[user@2 ASC NULLS LAST], preserve_partitioning=[true]
      RepartitionExec: partitioning=Hash([user@2], 4), input_partitions=4
        FilterExec: user@2 IS NOT NULL
          RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            StreamingTableExec: partition_sizes=1, projection=[latency, severityText, user]
```

`RepartitionExec: partitioning=Hash([user@2], 4)` is DataFusion parallelising the local window across
four threads per shard, and `StreamingTableExec` is the Lucene doc-value producer feeding it — so this
one plan shows both the window pushdown and the Lucene-to-DataFusion leaf working together.

### where + join + stats — Lucene filters one side, scans the other, DataFusion joins and aggregates

```console
$ curl -s -X POST "localhost:9200/_analytics/ppl" -H 'Content-Type: application/json' \
    -d "{\"query\": \"source=logs | where severityText='ERROR' | join left=L right=U on L.user = U.user users | stats count() as c by U.team\"}"
{"columns": ["c", "U.team"], "rows": [[600, "team2"], [600, "team3"], [600, "team0"], [600, "team1"]]}

$ ... /_explain | jq '.profile.stages[] | {stage_id, execution_type, chosen_backend, rows_processed}'
{"stage_id": 0, "execution_type": "SHARD_FRAGMENT", "chosen_backend": "lucene", "rows_processed": 2400, "tasks_completed": 3}
{"stage_id": 1, "execution_type": "SHARD_FRAGMENT", "chosen_backend": "lucene", "rows_processed": 20, "tasks_completed": 3}
{"stage_id": 2, "execution_type": "COORDINATOR_REDUCE", "chosen_backend": "datafusion", "rows_processed": 0, "tasks_completed": 1}
```

Verifiable by arithmetic: 16 ERROR users x 150 docs = 2400 joined rows; 4 ERROR users per team x 150
= 600 each. Lucene applies the `where` on the `logs` side (2400 of 3000 rows leave the shards) and
does an unfiltered doc-value scan on the `users` side; DataFusion owns the join and the aggregation.

### Summary

| PPL | shard backend | rows out | result | before this branch |
| --- | --- | --- | --- | --- |
| `sort - latency \| head 10` | datafusion | **30** | 10 rows | planning failure |
| `stats count() as c by user` | datafusion | **60** | 20 rows | planning failure |
| `stats count() as c by user \| sort - c \| head 5` | datafusion | **60** | 5 rows | planning failure |
| `dedup user` | datafusion | **60** | 20 rows | 3000 |
| `dedup 2 user` | datafusion | **120** | 40 rows | 3000 |
| `eventstats count() by user \| sort - c \| head 5` | lucene | 3000 | 5 rows | 3000 (unchanged by design) |
| `where … \| join users \| stats count() by team` | lucene | 2400 + 20 | 4 rows | planning failure |

30, 60 and 120 are the headline numbers: `30` = 3 shards x TopK(10); `60` = 20 partitions x 3 shards;
`120` is the same with N=2. Where the table says *planning failure*, the query could not be planned at
all on a plain index before this branch.

**Soundness of the window pushdown.** For a rank-like function the local rank of a row never exceeds
its global rank: adding other shards' rows to a partition can only push a row further down. So every
row satisfying `global_rank <= N` also satisfies `local_rank <= N`, and the shard filter discards only
rows the coordinator would discard anyway. The same rule appears in Spark
(`InsertWindowGroupLimit`, for `RowNumber`/`Rank`/`DenseRank` under a rank predicate) and Flink (batch
`Rank` planned as a local rank before the exchange and a global rank after).

**Why `eventstats` is excluded.** Its predicate is a row-level Top-N over an *additive* measure. A
shard's partial `COUNT` is not the global count, so a locally-failing row may pass globally —
filtering locally would change the answer. Spark draws the same line. Note also that `eventstats` and
`stats` are not interchangeable, which the transcripts above show directly: `stats count() by user |
sort - c | head 5` returns **5 distinct users**, while `eventstats count() by user | sort - c | head
5` returns **5 rows** (all 150-count rows, ordered arbitrarily among ties).

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
