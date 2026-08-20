/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Phase-2 evidence in isolation: drive {@link LuceneSearchExecEngine} in
 * {@link LuceneFragmentKind#VALUE_SCAN} mode over a real Lucene index and assert the Arrow batch
 * contents equal the indexed values.
 *
 * <p>Complements {@code PlainIndexScanIT}, which proves the same path end-to-end through Flight;
 * here the doc-value reading itself is pinned, including the multi-value and missing-value shapes an
 * IT with clean data would not exercise.
 */
public class LuceneValueScanTests extends OpenSearchTestCase {

    private Directory directory;
    private IndexWriter writer;
    private DirectoryReader reader;
    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = new ByteBuffersDirectory();
        writer = new IndexWriter(directory, new IndexWriterConfig());
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        if (reader != null) {
            reader.close();
        }
        writer.close();
        directory.close();
        allocator.close();
        super.tearDown();
    }

    /** Two columns, three rows — the shape {@code source=idx | fields id, name} produces. */
    public void testValueScanReadsBothColumns() throws Exception {
        addDoc(1, "alpha");
        addDoc(2, "beta");
        addDoc(3, "gamma");
        openReader();

        List<List<Object>> rows = runValueScan(List.of("id", "name"), new MatchAllDocsQuery());

        assertEquals(3, rows.size());
        assertEquals(List.of(1L, "alpha"), rows.get(0));
        assertEquals(List.of(2L, "beta"), rows.get(1));
        assertEquals(List.of(3L, "gamma"), rows.get(2));
    }

    /** Column order follows the requested projection, not the index. */
    public void testValueScanRespectsColumnOrder() throws Exception {
        addDoc(7, "eta");
        openReader();

        List<List<Object>> rows = runValueScan(List.of("name", "id"), new MatchAllDocsQuery());

        assertEquals(1, rows.size());
        assertEquals(List.of("eta", 7L), rows.get(0));
    }

    /** Only matching docs are materialised — the filter query drives the iteration. */
    public void testValueScanAppliesFilterQuery() throws Exception {
        addDoc(1, "alpha");
        addDoc(2, "beta");
        addDoc(3, "gamma");
        openReader();

        Query greaterThanOne = LongPoint.newRangeQuery("id", 2L, Long.MAX_VALUE);
        List<List<Object>> rows = runValueScan(List.of("id", "name"), greaterThanOne);

        assertEquals(2, rows.size());
        assertEquals(List.of(2L, "beta"), rows.get(0));
        assertEquals(List.of(3L, "gamma"), rows.get(1));
    }

    /** Docs spread across several segments are all read (one cursor per leaf). */
    public void testValueScanSpansMultipleSegments() throws Exception {
        addDoc(1, "alpha");
        writer.commit();
        addDoc(2, "beta");
        writer.commit();
        addDoc(3, "gamma");
        writer.commit();
        openReader();
        assertTrue("expected more than one leaf, got " + reader.leaves().size(), reader.leaves().size() > 1);

        List<List<Object>> rows = runValueScan(List.of("id", "name"), new MatchAllDocsQuery());
        assertEquals(3, rows.size());
        assertEquals(List.of(1L, "alpha"), rows.get(0));
        assertEquals(List.of(3L, "gamma"), rows.get(2));
    }

    /** A doc missing a column's doc values yields null rather than a bogus value or an exception. */
    public void testValueScanEmitsNullForMissingValues() throws Exception {
        addDoc(1, "alpha");
        Document partial = new Document();
        partial.add(new SortedNumericDocValuesField("id", 2L));
        partial.add(new LongPoint("id", 2L));
        writer.addDocument(partial); // no "name"
        openReader();

        List<List<Object>> rows = runValueScan(List.of("id", "name"), new MatchAllDocsQuery());

        assertEquals(2, rows.size());
        assertEquals(List.of(1L, "alpha"), rows.get(0));
        assertEquals(2L, rows.get(1).get(0));
        assertNull("missing keyword doc value must read back as null", rows.get(1).get(1));
    }

    /** Multi-valued fields take the first (lowest) doc value — documented simplification. */
    public void testValueScanTakesFirstOfMultiValuedField() throws Exception {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("id", 5L));
        doc.add(new SortedNumericDocValuesField("id", 9L));
        doc.add(new LongPoint("id", 5L));
        doc.add(new SortedSetDocValuesField("name", new BytesRef("aaa")));
        doc.add(new SortedSetDocValuesField("name", new BytesRef("zzz")));
        writer.addDocument(doc);
        openReader();

        List<List<Object>> rows = runValueScan(List.of("id", "name"), new MatchAllDocsQuery());
        assertEquals(1, rows.size());
        assertEquals(List.of(5L, "aaa"), rows.get(0));
    }

    /** A column the mapping doesn't know about fails loudly rather than emitting silent nulls. */
    public void testUnmappedColumnFailsLoudly() throws Exception {
        addDoc(1, "alpha");
        openReader();

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> runValueScan(List.of("id", "nope"), new MatchAllDocsQuery())
        );
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("nope"));
    }

    // ── Harness ─────────────────────────────────────────────────────────────

    private void addDoc(long id, String name) throws IOException {
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("id", id));
        doc.add(new LongPoint("id", id));
        doc.add(new SortedSetDocValuesField("name", new BytesRef(name)));
        writer.addDocument(doc);
    }

    private void openReader() throws IOException {
        writer.commit();
        reader = DirectoryReader.open(directory);
    }

    /**
     * Runs the exec engine over the open reader and returns the emitted batch as rows of Java values.
     * Mirrors what {@code LuceneScanInstructionHandler} assembles on the data node.
     */
    private List<List<Object>> runValueScan(List<String> columns, Query filter) throws IOException {
        LuceneSearcherState state = new LuceneSearcherState(
            new IndexSearcher(reader),
            filter,
            columns,
            LuceneFragmentKind.VALUE_SCAN
        );
        ShardScanExecutionContext context = new ShardScanExecutionContext("test_index", null, null);
        context.setAllocator(allocator);
        context.setMapperService(mapperService());

        LuceneSearchExecEngine engine = new LuceneSearchExecEngine(state);
        engine.prepare(context);

        List<List<Object>> rows = new ArrayList<>();
        try (EngineResultStream stream = engine.execute(context)) {
            Iterator<EngineResultBatch> batches = stream.iterator();
            while (batches.hasNext()) {
                EngineResultBatch batch = batches.next();
                try (VectorSchemaRoot root = batch.getArrowRoot()) {
                    assertEquals("batch schema must match the requested projection", columns, batch.getFieldNames());
                    for (int row = 0; row < batch.getRowCount(); row++) {
                        List<Object> values = new ArrayList<>(columns.size());
                        for (String column : columns) {
                            Object value = batch.getFieldValue(column, row);
                            // Arrow returns Utf8 as byte[]/Text depending on vector type; normalise.
                            values.add(value instanceof byte[] bytes ? new String(bytes, java.nio.charset.StandardCharsets.UTF_8) : value);
                        }
                        rows.add(values);
                    }
                }
            }
        }
        return rows;
    }

    /** MapperService stub resolving {@code id} to a long and {@code name} to a keyword. */
    private static MapperService mapperService() {
        // Build the field-type mocks first: creating a mock inside a when(...) argument is nested
        // stubbing, which Mockito rejects with UnfinishedStubbingException.
        MappedFieldType idType = fieldType("long");
        MappedFieldType nameType = fieldType("keyword");
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType("id")).thenReturn(idType);
        when(mapperService.fieldType("name")).thenReturn(nameType);
        return mapperService;
    }

    private static MappedFieldType fieldType(String typeName) {
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(fieldType.typeName()).thenReturn(typeName);
        when(fieldType.hasDocValues()).thenReturn(true);
        return fieldType;
    }
}
