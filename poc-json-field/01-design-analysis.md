# Semi-structured field designs in OpenSearch — analysis

Context: Fidelity stores logs with a top-level `body` field containing many sub-/nested
fields. Queries extract subfields (`status`, `method`, `level`, …) and use them mainly as
low-cardinality `stats … by <field>` group-by keys. Today this is done with `rex` (query-time
extraction). This doc compares the solution space and the concrete Lucene data structures.

---

## Part 1 — Solution space by problem (with historical origin)

| Use case / problem | Solution (design) | Pros / Cons | Reference implementation | Original idea comes from |
|---|---|---|---|---|
| Retain a blob, never query it | **Stored-only** (`_source`, no index) | + zero cost / − inert | OS `object enabled:false` | **Store-vs-index separation / BLOB storage** — early DBMS distinction between stored and indexed data; the document `_source` idea |
| Bounded, known field set, full typed queries | **Explicit typed mapping** (declare each field) | + full typed ops / − explodes as count grows | OS/ES `object`, Vespa struct | **Relational model / fixed schema** (Codd, 1970) — schema-on-write, predefined typed columns |
| Many/unknown subfields, exact-match filtering only | **Single dynamic keyword blob** (path encoded into the term) | + no explosion, dynamic / − keyword-only, no agg | OS `flat_object`, ES `flattened` | **EAV / "turn schema into data"** (sparse-schema clinical DBs, 1970s–80s) → RDF triples → Bigtable column qualifiers (2006) |
| One field whose *value type* varies | **Tagged union** (discriminator + per-type subcolumn) | + clean mixed types / − not a path solution | ClickHouse `Variant` / `Dynamic` | **Variant records / discriminated unions** — ALGOL 68 united modes, Pascal variant records (1970s), ML/Haskell sum types |
| ⭐ Unbounded/unknown subfields needing **typed** range+agg, no explosion *(Fidelity's would-be fit)* | **Per-segment local schema + typed columnar subcolumns + residual tail** | + typed on any path, no explosion / − complex core work, backfill hard | ClickHouse `JSON`, Snowflake VARIANT, BigQuery JSON, **Druid `COMPLEX<json>`**, SingleStore, Parquet/Iceberg Variant shredding | **Dremel record shredding / column striping** (Google, 2010 → Parquet) + **schema-on-read** (Hadoop era) + per-part local schema inference |
| Many dynamic subfields, fast **filter** on any path | **Index-everything** (inverted/B-tree/GIN on all path→value) | + any-path filter now / − write amp, no columnar agg | Rockset, MongoDB wildcard index, Postgres JSONB + GIN | **The inverted index itself** (IR, 1960s–70s) generalized to all attributes → Postgres **GIN** (Generalized Inverted iNdex, ~2006) |

**Where the three designs below sit:**
- **Design A (postings)** = squeezing aggregation out of the `flat_object` EAV blob (row 3).
- **Design B (shared doc-values column)** = `flat_object`'s current doc-values path (row 3, broken at cross-shard reduce today).
- **Design C (per-path columns)** = the **ClickHouse `JSON` / Druid / Parquet-shredding category** (row 5). The user-facing query is unchanged; only the on-disk structure differs.

---

## Part 2 — Test data & query

Three documents; `body` is mapped as `flat_object`:

```
doc0: body = { "method": "GET",  "status": "200" }
doc1: body = { "method": "POST", "status": "200" }
doc2: body = { "method": "GET",  "status": "404" }
```

PPL query (same intent in all designs):

```
source=flat-logs | where body.method = 'GET' | stats count() by body.status
```

Expected result: `{ 200: 1, 404: 1 }`  (doc0→200, doc2→404; doc1 excluded by `method=POST`).

### What `flat_object` actually stores today

From `FlatObjectFieldMapper.parseToken(...)`, each leaf value is written into **four** structures —
**two postings** (inverted index) and **two shared SortedSet doc-values** columns. Doc-values terms
are prefixed with `body.` (`getDVPrefix`), which is why `_valueAndPath` DV terms read `body.body.…`.

```
POSTINGS  field: body._value                 POSTINGS  field: body._valueAndPath
  GET  → [doc0, doc2]                           body.method=GET   → [doc0, doc2]
  POST → [doc1]                                 body.method=POST  → [doc1]
  200  → [doc0, doc1]                           body.status=200   → [doc0, doc1]
  404  → [doc2]                                 body.status=404   → [doc2]

DOCVALUES field: body._value  (SortedSet)     DOCVALUES field: body._valueAndPath (SortedSet)
  dict: 0=body.200 1=body.404                    dict: 0=body.body.method=GET  1=…method=POST
        2=body.GET  3=body.POST                        2=body.body.status=200  3=…status=404
  doc0 → {0(200), 2(GET)}                        doc0 → {0, 2}
  doc1 → {0(200), 3(POST)}                       doc1 → {1, 2}
  doc2 → {1(404), 2(GET)}                        doc2 → {0, 3}
```

Notes on the current type (grounded in code):
- `FlatObjectFieldType.isAggregatable()` returns **false**.
- `FlatObjectDocValueFormat.getWriteableName()` = `"flat_object"` but it is **not registered** in
  `SearchModule.registerValueFormat(...)`, and its `writeTo` is empty with no reader → cross-shard
  aggregation fails with `Unknown NamedWriteable [DocValueFormat][flat_object]`.
- Design A uses the **postings**; Design B uses the **shared SortedSet doc values**; Design C proposes
  an **additional dedicated per-path column**.

---

## Part 3 — The three designs

### Design A — inverted index (postings)

**Internal data structure** (`term → postings list`, one shared field):
```
field: body._valueAndPath   (postings)
  body.method=GET   → [doc0, doc2]
  body.method=POST  → [doc1]
  body.status=200   → [doc0, doc1]
  body.status=404   → [doc2]
```

**DSL** (filters agg = one term query per value; a prefix-`TermsEnum` aggregator would auto-discover values):
```json
{
  "size": 0,
  "query": { "bool": { "filter": [ { "term": { "body.method": "GET" } } ] } },
  "aggs": { "by_status": { "filters": { "filters": {
    "200": { "term": { "body.status": "200" } },
    "404": { "term": { "body.status": "404" } }
  } } } }
}
```

**How it executes** (postings only, no doc values):
```
term body.method=GET → [doc0,doc2]                       ← filter set
"200": body.status=200 [doc0,doc1] ∩ [doc0,doc2] = [doc0]  → 1
"404": body.status=404 [doc2]      ∩ [doc0,doc2] = [doc2]  → 1
```
→ `{200:1, 404:1}`

- Works today. Uses only the inverted index.
- Limitation: high cardinality = one postings walk per distinct term. The stock `filters` agg needs
  values enumerated; a purpose-built prefix-`TermsEnum` aggregator removes that.

---

### Design B — one shared SortedSet doc-values column

**Internal data structure** (all subpaths mixed into a single column):
```
field: body._valueAndPath   (SortedSetDocValues)
  global dictionary            per-doc ordinals
  ord  term                    doc0 → {0, 2}
  0    body.method=GET         doc1 → {1, 2}
  1    body.method=POST        doc2 → {0, 3}
  2    body.status=200
  3    body.status=404
```

**DSL**:
```json
{
  "size": 0,
  "query": { "bool": { "filter": [ { "term": { "body.method": "GET" } } ] } },
  "aggs": { "by_status": { "terms": { "field": "body.status", "size": 100 } } }
}
```

**How it executes** (shared column + `FlatObjectDocValueFormat`, prefix `body.status=`):
```
filter (postings) → matched docs {doc0, doc2}
doc0 → {0,2}: ord0 body.method=GET → not prefix → NO_MATCH (skip)
              ord2 body.status=200 → strip → "200" → +1
doc2 → {0,3}: ord0 → skip
              ord3 body.status=404 → strip → "404" → +1
```
→ `{200:1, 404:1}`

- Reads `method` ordinals and discards them (scans all subpaths per doc).
- Cross-shard reduce must serialize the `flat_object` `DocValueFormat` → **currently fails**
  (`Unknown NamedWriteable`). Fix needs: make the format static, serialize its `prefix`, register the
  NamedWriteable, and flip `isAggregatable()` to true for subfields.

---

### Design C — one dedicated doc-values column per path  (= ClickHouse `JSON` category)

**Internal data structure** (each path is its own column with its own small dictionary):
```
field: body.method  (SortedSetDocValues)      field: body.status  (SortedSetDocValues)
  ord  term    per-doc ords                     ord  term    per-doc ords
  0    GET     doc0 → {0}                        0    200     doc0 → {0}
  1    POST    doc1 → {1}                        1    404     doc1 → {0}
              doc2 → {0}                                     doc2 → {1}
(+ shared _valueAndPath tail blob for non-shredded paths)
```
Schema (which paths are shredded) lives in **per-segment Lucene FieldInfos**, decided at flush/merge —
**not** in cluster-state mapping — so subfield cardinality never explodes cluster metadata. Bounded by a
`max_dynamic_paths`-style cap; overflow paths stay in the shared blob (served by Design A).

**DSL** — identical to Design B (query never changes; only the internal structure does):
```json
{
  "size": 0,
  "query": { "bool": { "filter": [ { "term": { "body.method": "GET" } } ] } },
  "aggs": { "by_status": { "terms": { "field": "body.status", "size": 100 } } }
}
```

**How it executes** (dedicated `body.status` column, own ordinals):
```
filter (postings) → matched docs {doc0, doc2}
doc0 → {0} → ord0 (=200) → +1
doc2 → {1} → ord1 (=404) → +1
```
→ `{200:1, 404:1}`

- No prefix, no NO_MATCH, no `method` values scanned — a plain keyword terms aggregation on its own
  ordinals. Serializes natively; scales to any cardinality; supports numeric metrics if the column is typed.

---

## Part 4 — Summary

| Aspect | A (postings) | B (shared DV) | C (per-path columns) |
|---|---|---|---|
| Structure | `term → docs` | one shared `doc → values` column | one `doc → values` column **per path** |
| DSL | filters / prefix-terms | standard `terms` | standard `terms` (same as B) |
| Scans other subfields? | no | yes (discards via NO_MATCH) | no |
| High cardinality | weak (postings walk/term) | ok but scans all paths | strong (own global ordinals) |
| Cluster-state entries | 1 (`body`) | 1 (`body`) | 1 (`body`); columns in per-segment FieldInfos |
| Cross-shard serialization | plain int counts ✅ | needs `DocValueFormat` fix ❌ | native keyword agg ✅ |
| Numeric metrics (`avg`) | ❌ | ❌ (untyped) | ✅ if typed |
| Category (Part 1) | row 3 (EAV) | row 3 (EAV) | **row 5 (ClickHouse JSON / Druid / Parquet shredding)** |
| Best for | low-card `count() by` | — (inefficient middle) | any cardinality, dynamic paths, future metrics |

**Recommendation for Fidelity:** their hot set is *known and bounded* (`status`, `method`, `level`, …),
so the best near-term solution is **ingest-time extraction into dedicated keyword/numeric fields**
(Design C done manually for a known set — works today, most efficient, no core code). Build the dynamic
per-segment shredded `variant`/`flat_object` (Design C automatic) only when efficient aggregation is needed
on *unknown/unbounded* subpaths.
