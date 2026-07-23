# PoC test & results — `json` field type (Design C) in OpenSearch core

**Date:** 2026-07-22
**Branch:** `poc/json-field-type` (OpenSearch core, `/home/daichen/os-ws/OpenSearch`)
**Cluster:** `./gradlew run` → `localhost:9200`, opensearch `3.8.0-SNAPSHOT` (our build), Lucene 10.5.0
**Changes (uncommitted):**
- NEW `server/src/main/java/org/opensearch/index/mapper/JsonFieldMapper.java`
- MODIFIED `server/src/main/java/org/opensearch/indices/IndicesModule.java` (registered `json` type)

**Goal:** prove the Fidelity pattern in core — dynamic subfields, no mapping explosion,
native `count() by body.<field>`, and typed numeric `avg`/range — the operations that fail on
`flat_object`.

> Note: `./gradlew run` starts **core only** (no SQL/PPL plugin), so tests use the DSL `_search`
> API, which is exactly what PPL compiles `stats … by body.status` into.

---

## Setup

### 1. Create index (`body` = json type; `duration_ms` declared as `long`)
```json
PUT /json-poc
{"mappings":{"properties":{
  "@timestamp":{"type":"date"},
  "body":{"type":"json","typed_paths":{"duration_ms":"long"}}}}}
```
Result:
```json
{"acknowledged":true,"shards_acknowledged":true,"index":"json-poc"}
```

### 2. Index 3 docs (`body` is a JSON object with 4–5 subfields each)
```
{"body":{"method":"GET", "status":"200","duration_ms":2853,"uri":"/a","client_ip":"1.1.1.1"}}
{"body":{"method":"POST","status":"200","duration_ms":100, "uri":"/b"}}
{"body":{"method":"GET", "status":"404","duration_ms":50,  "uri":"/c"}}
```
Result: bulk `"errors":false`, 3 docs created.

---

## Tests & results

### 3. `GET /json-poc/_mapping` — no subfield explosion
Result:
```json
{"json-poc":{"mappings":{"properties":{
  "@timestamp":{"type":"date"},
  "body":{"type":"json","typed_paths":{"duration_ms":"long"}}}}}}
```
✅ Only `body` appears — no `body.status`/`body.method`/`body.uri`/`body.client_ip` entries,
despite all being queryable.

### 4. Filter `term body.method=GET`
```json
Query:  {"query":{"term":{"body.method":"GET"}}}
Result: {"hits":{"total":{"value":2},"hits":[{"_id":"0"},{"_id":"2"}]}}
```
✅ 2 hits (the two GET docs).

### 5. Native aggregation — `count() by body.status` (fails on flat_object)
```json
Query:  {"size":0,"aggs":{"by_status":{"terms":{"field":"body.status","size":10}}}}
Result: {"aggregations":{"by_status":{"buckets":[
          {"key":"200","doc_count":2},
          {"key":"404","doc_count":1}]}}}
```
✅ Native terms agg — no `Unknown NamedWriteable [DocValueFormat][flat_object]` error.

### 6. Typed numeric aggregation — `avg(body.duration_ms)`
```json
Query:  {"size":0,"aggs":{"avg_dur":{"avg":{"field":"body.duration_ms"}}}}
Result: {"aggregations":{"avg_dur":{"value":1001.0}}}
```
✅ (2853+100+50)/3 = 1001.0 — numeric typing works (impossible on flat_object).

### 7. Filtered group-by — `where body.method=GET | stats count() by body.status`
```json
Query:  {"size":0,"query":{"term":{"body.method":"GET"}},
         "aggs":{"by_status":{"terms":{"field":"body.status","size":10}}}}
Result: {"aggregations":{"by_status":{"buckets":[
          {"key":"200","doc_count":1},
          {"key":"404","doc_count":1}]}}}
```
✅ Matches the exact Fidelity query shape.

### 8. Typed numeric range — `body.duration_ms > 100`
```json
Query:  {"query":{"range":{"body.duration_ms":{"gt":100}}}}
Result: {"hits":{"total":{"value":1},"hits":[{"_id":"0"}]}}
```
✅ Only doc 0 (duration 2853).

### 9. `max_dynamic_paths` cap (index `json-cap`, `max_dynamic_paths:2`)
Doc: `{"body":{"method":"GET","status":"200","uri":"/x","client_ip":"9.9.9.9"}}`
```
body.method=GET     -> {"hits":{"total":{"value":1}}}   materialized
body.status=200     -> {"hits":{"total":{"value":1}}}   materialized
body.uri=/x         -> {"hits":{"total":{"value":0}}}   dropped (over cap)
body.client_ip=...  -> {"hits":{"total":{"value":0}}}   dropped (over cap)
mapping             -> {"body":{"type":"json","max_dynamic_paths":2}}
```
✅ First 2 distinct paths get columns; overflow dropped; mapping stays flat.

---

## Summary

| # | Test | Expected | Actual | Pass |
|---|---|---|---|---|
| 3 | mapping after indexing | only `body:json` | only `body:json` | ✅ |
| 4 | filter `body.method=GET` | 2 hits | 2 hits | ✅ |
| 5 | `count() by body.status` | 200:2, 404:1 | 200:2, 404:1 | ✅ |
| 6 | `avg(body.duration_ms)` | 1001.0 | 1001.0 | ✅ |
| 7 | filtered by status | 200:1, 404:1 | 200:1, 404:1 | ✅ |
| 8 | range `duration_ms>100` | 1 hit | 1 hit | ✅ |
| 9 | `max_dynamic_paths=2` | 2 kept, 2 dropped | 2 kept, 2 dropped | ✅ |

---

## Scope / caveats
- **PPL not tested end-to-end** — core-only `./gradlew run`; verified via DSL `_search` (what PPL compiles to).
- **Type inference is *declared*** via `typed_paths` (not auto-inferred). True per-value inference needs
  per-segment schema + `field_caps`; declaration is the minimal robust form and covers Fidelity's known hot fields.
- **`max_dynamic_paths` overflow drops** excess columns (unqueryable) rather than routing to a shared
  keyword tail. Tail-with-query-resolution needs persisted per-segment schema (deferred).
- No commit made; changes live in the working tree on branch `poc/json-field-type`.

## How to reproduce
```
cd /home/daichen/os-ws/OpenSearch
git checkout poc/json-field-type
./gradlew run            # starts localhost:9200 (core only)
# then run the curl commands above
```
Stop the cluster: `pkill -f 'gradlew run'`.
