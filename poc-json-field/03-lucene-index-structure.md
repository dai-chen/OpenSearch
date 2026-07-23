# Generated Lucene index structure (`json` field type)

Read directly from the live segment produced by `./gradlew run` (Lucene 10.5.0), by dumping
`FieldInfos` and per-column contents with a small Lucene program. This shows what
`JsonFieldMapper` actually writes to disk.

Index `json-poc` — 3 docs, mapping `body: {type: json, typed_paths: {duration_ms: long}}`:
```
doc0: body = {"method":"GET", "status":"200","duration_ms":2853,"uri":"/a","client_ip":"1.1.1.1"}
doc1: body = {"method":"POST","status":"200","duration_ms":100, "uri":"/b"}
doc2: body = {"method":"GET", "status":"404","duration_ms":50,  "uri":"/c"}
```

## FieldInfos — one dedicated Lucene column per JSON leaf path
```
FIELD                postings       docValues        points
body.method          DOCS           SORTED_SET       0(0B)      <- keyword column
body.status          DOCS           SORTED_SET       0(0B)      <- keyword column
body.uri             DOCS           SORTED_SET       0(0B)      <- keyword column
body.client_ip       DOCS           SORTED_SET       0(0B)      <- keyword column
body.duration_ms     NONE           SORTED_NUMERIC   1(8B)      <- NUMERIC column (declared long)
@timestamp           NONE           SORTED_NUMERIC   1(8B)      (normal date field)
_id / _source / _seq_no / _primary_term / _version            (internal)
```

Observations:
- **Each subfield is its own Lucene column** (`body.method`, `body.status`, `body.uri`,
  `body.client_ip`, `body.duration_ms`). There is **NO shared `body._value` / `body._valueAndPath`
  EAV blob** — that is what `flat_object` produces instead.
- **Keyword paths** → `postings (DOCS)` + `SORTED_SET` doc values ⇒ native `term` filter AND native
  `terms` aggregation.
- **Declared-numeric path** (`duration_ms`) → **`points (1 dim, 8B)` + `SORTED_NUMERIC` doc values,
  `postings = NONE`** ⇒ numeric range (points) + `avg` (numeric doc values), exactly like a real
  `long` field. Type inference materializes a different physical structure.

## Per-column contents (the columnar dictionaries)
```
POSTINGS  body.method     terms: GET(df=2) POST(df=1)
POSTINGS  body.status     terms: 200(df=2) 404(df=1)
POSTINGS  body.uri        terms: /a(df=1) /b(df=1) /c(df=1)
POSTINGS  body.client_ip  terms: 1.1.1.1(df=1)
DOCVALUES body.duration_ms  numeric: doc0=2853 doc1=100 doc2=50
```
- `body.status` has its OWN dictionary `{200, 404}` with per-term doc frequencies — this is how
  `count() by body.status -> 200:2, 404:1` is answered natively (no shared-column scan).
- `body.duration_ms` is a real numeric column `{2853,100,50}` — how `avg -> 1001.0` works.

This is the **Design C** layout (ClickHouse-JSON / Druid per-path columns). Contrast **Design B**
(`flat_object`), which would instead show a single `body._valueAndPath` SORTED_SET mixing
`body.status=200`, `body.method=GET`, ... in one shared column.

## `max_dynamic_paths` cap — proven at the storage layer
Index `json-cap` (`max_dynamic_paths: 2`), one doc with 4 subfields `{method,status,uri,client_ip}`
produced **only two columns**:
```
body.method          DOCS   SORTED_SET
body.status          DOCS   SORTED_SET
(no body.uri, no body.client_ip)
```
The cap stopped at 2 distinct columns; overflow paths were never written to Lucene — matching the
query result (0 hits on `body.uri` / `body.client_ip`).

## How this was produced
```
# locate the live segment dir
build/testclusters/runTask-0/data/nodes/0/indices/<uuid>/0/index
# open with Lucene 10.5.0 and print FieldInfos + terms + numeric docvalues (Java 21)
```
Net: on disk the mapper produces **N dedicated per-path Lucene columns** (keyword or numeric per
`typed_paths`), not the shared EAV encoding of `flat_object` — the concrete realization of the
ClickHouse-JSON / Druid per-path-column model, and the reason native aggregation and typed numeric
ops work while cluster-state mappings stay flat.
