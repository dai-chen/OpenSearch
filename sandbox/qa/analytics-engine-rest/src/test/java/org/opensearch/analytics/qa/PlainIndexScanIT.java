/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * End-to-end proof that the analytics engine can scan an <b>ordinary</b> index — one created with no
 * {@code index.pluggable.dataformat} settings, no composite engine, no parquet — and return correct
 * row values through PPL.
 *
 * <p>Two changes make this work, and this test is the evidence for both:
 *
 * <ol>
 *   <li>{@code EngineBackedIndexer.acquireReader()} — a plain shard now hands out an
 *       {@code IndexReaderProvider.Reader} instead of throwing, so
 *       {@code AnalyticsSearchService.startFragment} can reach its segments.</li>
 *   <li>The Lucene backend declares {@code ScanCapability.DocValues} over the {@code lucene}
 *       doc-value format and implements a doc-values row scan, so the planner both picks it and
 *       can get values out of it. On a plain index every field reports
 *       {@code docValueFormats=["lucene"]}; on a composite index they report {@code ["parquet"]},
 *       which is what keeps this path off composite indices.</li>
 * </ol>
 *
 * <p>Queries go through the {@code test-ppl-frontend} shim ({@code POST /_analytics/ppl}) so the
 * opensearch-sql plugin — whose {@code RestUnifiedQueryAction} routing is out of scope here — is
 * entirely out of the picture.
 *
 * <p>Run with:
 * {@code ./gradlew :sandbox:qa:analytics-engine-rest:integTest --tests "*.PlainIndexScanIT" -Dsandbox.enabled=true}
 */
public class PlainIndexScanIT extends AnalyticsRestTestCase {

    private static final String SINGLE_SHARD_INDEX = "poc_plain";
    private static final String MULTI_SHARD_INDEX = "poc_plain_multi";

    /** Phase 3 fixture: 1 shard, 3 docs. */
    private static final List<Doc> THREE_DOCS = List.of(new Doc(1, "alpha"), new Doc(2, "beta"), new Doc(3, "gamma"));

    /** Phase 4 fixture: 3 shards, 10 docs — proves the gather, not a single-shard special case. */
    private static final List<Doc> TEN_DOCS = List.of(
        new Doc(1, "alpha"),
        new Doc(2, "beta"),
        new Doc(3, "gamma"),
        new Doc(4, "delta"),
        new Doc(5, "epsilon"),
        new Doc(6, "zeta"),
        new Doc(7, "eta"),
        new Doc(8, "theta"),
        new Doc(9, "iota"),
        new Doc(10, "kappa")
    );

    // ── Phase 3: the single-shard proof ──────────────────────────────────────

    /**
     * The headline assertion: {@code source=poc_plain | fields id, name} over a plain index returns
     * exactly the three indexed rows with the right values, and the shard fragment was driven by the
     * Lucene backend (not DataFusion, which has no doc values for this index).
     */
    public void testPlainIndexProjectionReturnsRowValues() throws Exception {
        createPlainIndex(SINGLE_SHARD_INDEX, 1);
        bulkIndex(SINGLE_SHARD_INDEX, THREE_DOCS);

        List<Doc> rows = queryDocs(SINGLE_SHARD_INDEX, "fields id, name");
        assertEquals("expected exactly the three indexed rows, got " + rows, THREE_DOCS, rows);

        Map<String, Object> explain = explain("source = " + SINGLE_SHARD_INDEX + " | fields id, name");
        assertStageChoseBackend(explain, "SHARD_FRAGMENT", "lucene");
    }

    /** Column order must follow the projection, not the mapping. */
    public void testProjectionColumnOrderIsRespected() throws Exception {
        createPlainIndex(SINGLE_SHARD_INDEX, 1);
        bulkIndex(SINGLE_SHARD_INDEX, THREE_DOCS);

        Map<String, Object> response = executePplViaShim("source = " + SINGLE_SHARD_INDEX + " | fields name, id");
        assertEquals(List.of("name", "id"), columnNames(response));

        List<Doc> rows = new ArrayList<>();
        for (List<Object> row : datarows(response)) {
            rows.add(new Doc(((Number) row.get(1)).longValue(), (String) row.get(0)));
        }
        rows.sort(Comparator.comparingLong(Doc::id));
        assertEquals(THREE_DOCS, rows);
    }

    /** Multiple segments — the scan sums per-leaf iteration rather than assuming one leaf. */
    public void testMultipleSegments() throws Exception {
        createPlainIndex(SINGLE_SHARD_INDEX, 1);
        bulkIndex(SINGLE_SHARD_INDEX, THREE_DOCS.subList(0, 1));
        flush(SINGLE_SHARD_INDEX);
        bulkIndex(SINGLE_SHARD_INDEX, THREE_DOCS.subList(1, 2));
        flush(SINGLE_SHARD_INDEX);
        bulkIndex(SINGLE_SHARD_INDEX, THREE_DOCS.subList(2, 3));
        flush(SINGLE_SHARD_INDEX);

        assertEquals(THREE_DOCS, queryDocs(SINGLE_SHARD_INDEX, "fields id, name"));
    }

    // ── Phase 4: multi-shard gather + filter composition ────────────────────

    /** 3 shards, 10 docs → all 10 rows come back, proving the cross-shard gather. */
    public void testMultiShardGatherReturnsAllRows() throws Exception {
        createPlainIndex(MULTI_SHARD_INDEX, 3);
        bulkIndex(MULTI_SHARD_INDEX, TEN_DOCS);

        List<Doc> rows = queryDocs(MULTI_SHARD_INDEX, "fields id, name");
        assertEquals("all 10 rows across 3 shards, got " + rows, TEN_DOCS, rows);
    }

    /** Filter composition: the backend's existing predicate pushdown over a value-producing scan. */
    public void testNumericFilterOnPlainIndex() throws Exception {
        createPlainIndex(SINGLE_SHARD_INDEX, 1);
        bulkIndex(SINGLE_SHARD_INDEX, THREE_DOCS);

        List<Doc> rows = queryDocs(SINGLE_SHARD_INDEX, "where id > 1 | fields id, name");
        assertEquals(List.of(new Doc(2, "beta"), new Doc(3, "gamma")), rows);
    }

    /** Keyword equality — the predicate shape the Lucene backend has always supported. */
    public void testKeywordFilterOnPlainIndex() throws Exception {
        createPlainIndex(SINGLE_SHARD_INDEX, 1);
        bulkIndex(SINGLE_SHARD_INDEX, THREE_DOCS);

        assertEquals(List.of(new Doc(2, "beta")), queryDocs(SINGLE_SHARD_INDEX, "where name = 'beta' | fields id, name"));
    }

    /** Filter over the multi-shard index: gather plus pushdown together. */
    public void testMultiShardFilter() throws Exception {
        createPlainIndex(MULTI_SHARD_INDEX, 3);
        bulkIndex(MULTI_SHARD_INDEX, TEN_DOCS);

        List<Doc> expected = TEN_DOCS.stream().filter(d -> d.id() > 5).toList();
        assertEquals(expected, queryDocs(MULTI_SHARD_INDEX, "where id > 5 | fields id, name"));
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    /** Runs the query and returns its rows as {@link Doc}s sorted by id, so shard order can't flake. */
    private List<Doc> queryDocs(String index, String pplSuffix) throws IOException {
        String ppl = "source = " + index + " | " + pplSuffix;
        Map<String, Object> response = executePplViaShim(ppl);
        List<String> columns = columnNames(response);
        int idIdx = columns.indexOf("id");
        int nameIdx = columns.indexOf("name");
        assertTrue("response is missing the id/name columns: " + columns, idIdx >= 0 && nameIdx >= 0);

        List<Doc> docs = new ArrayList<>();
        for (List<Object> row : datarows(response)) {
            Object id = row.get(idIdx);
            assertNotNull("null id in row " + row + " for: " + ppl, id);
            docs.add(new Doc(((Number) id).longValue(), (String) row.get(nameIdx)));
        }
        docs.sort(Comparator.comparingLong(Doc::id));
        return docs;
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> datarows(Map<String, Object> response) {
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("response has no datarows: " + response, rows);
        return rows;
    }

    /** The shim returns {@code columns}; the sql plugin returns {@code schema}. Handle both. */
    private static List<String> columnNames(Map<String, Object> response) {
        Object columns = response.get("columns");
        if (columns instanceof List<?> list) {
            return list.stream().map(String::valueOf).toList();
        }
        return extractColumnNames(response);
    }

    private Map<String, Object> explain(String ppl) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl/_explain");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "EXPLAIN: " + ppl);
    }

    @SuppressWarnings("unchecked")
    private static void assertStageChoseBackend(Map<String, Object> explain, String executionType, String expectedBackend) {
        Map<String, Object> profile = (Map<String, Object>) explain.get("profile");
        assertNotNull("profile present in " + explain, profile);
        List<Map<String, Object>> stages = (List<Map<String, Object>>) profile.get("stages");
        assertNotNull("stages present in " + profile, stages);
        for (Map<String, Object> stage : stages) {
            if (executionType.equals(stage.get("execution_type"))) {
                assertEquals(
                    executionType + " chose unexpected backend (full stage: " + stage + ")",
                    expectedBackend,
                    stage.get("chosen_backend")
                );
                return;
            }
        }
        fail("No " + executionType + " stage in profile: " + stages);
    }

    /**
     * Creates an ordinary index: only shard/replica counts and the two field mappings. Explicitly
     * <em>no</em> {@code index.pluggable.dataformat*} or {@code index.composite.*} settings — that
     * absence is the whole point of this test.
     */
    private void createPlainIndex(String index, int numberOfShards) throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {
            // index may not exist yet
        }

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": "
            + numberOfShards
            + ","
            + "  \"number_of_replicas\": 0"
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"id\": { \"type\": \"long\" },"
            + "    \"name\": { \"type\": \"keyword\" }"
            + "  }"
            + "}"
            + "}";
        Request request = new Request("PUT", "/" + index);
        request.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(request), "Create index " + index);
        assertEquals(true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + index);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    private void bulkIndex(String index, List<Doc> docs) throws Exception {
        StringBuilder ndjson = new StringBuilder();
        for (Doc doc : docs) {
            ndjson.append("{\"index\": {}}\n").append(doc.toJson()).append('\n');
        }
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.setJsonEntity(ndjson.toString());
        request.addParameter("refresh", "true");
        request.setOptions(request.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> response = assertOkAndParse(client().performRequest(request), "Bulk index " + index);
        assertEquals("bulk indexing should have no errors", false, response.get("errors"));
    }

    private void flush(String index) throws Exception {
        client().performRequest(new Request("POST", "/" + index + "/_flush?force=true"));
    }

    private record Doc(long id, String name) {
        String toJson() {
            return "{\"id\": " + id + ", \"name\": \"" + name + "\"}";
        }
    }
}
