/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.ValueScanProducer;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.IndexReaderProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Lucene's {@link ValueScanProducer}: materialises a shard's columns from its own doc values.
 *
 * <p>Accepts a reader only when it publishes {@link
 * org.opensearch.index.engine.dataformat.DataFormatNames#LUCENE_DOC_VALUES} — real doc values on an
 * index with no columnar primary. A composite index's Lucene secondary is postings-only and its
 * values live in the primary, so {@link #supports} returns false there and the consumer keeps reading
 * the primary directly.
 *
 * <p>Holds the doc-values iteration that {@link LuceneSearchExecEngine} also uses for its own
 * driver-mode value scan, so the two cannot diverge.
 *
 * @opensearch.internal
 */
final class LuceneValueScanProducer implements ValueScanProducer {

    private static final Logger LOGGER = LogManager.getLogger(LuceneValueScanProducer.class);

    private final LucenePlugin plugin;

    LuceneValueScanProducer(LucenePlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public boolean supports(IndexReaderProvider.Reader reader) {
        LuceneReaderAdapter.Resolved resolved = plugin.readerAdapter().resolve(reader);
        return resolved != null && resolved.hasDocValues();
    }

    @Override
    public Schema schema(List<String> columns, ShardScanExecutionContext context) {
        return new Schema(arrowFields(readers(columns, context)));
    }

    @Override
    public void produce(List<String> columns, ShardScanExecutionContext context, BatchSink sink) throws IOException {
        LuceneReaderAdapter.Resolved resolved = plugin.readerAdapter().resolve(context.getReader());
        if (resolved == null) {
            throw new IllegalStateException("Lucene value scan dispatched to a shard with no LuceneReader");
        }
        // Shared per-reader searcher (see LuceneReader#searcher) — the filter and the doc values must
        // come from one point-in-time reader or the doc ids would index different segments.
        IndexSearcher searcher = resolved.reader().searcher(context.getQueryCache(), context.getQueryCachingPolicy());
        List<LuceneDocValuesReader> readers = readers(columns, context);
        Schema schema = new Schema(arrowFields(readers));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, context.getAllocator())) {
            int rows = scanInto(searcher, new MatchAllDocsQuery(), readers, root, context.getShardId());
            LOGGER.debug("[lucene-value-producer] shardId={} columns={} rows={}", context.getShardId(), columns, rows);
            sink.accept(root);
        }
    }

    /** Resolves one doc-values reader per column, in output order. */
    static List<LuceneDocValuesReader> readers(List<String> columns, ShardScanExecutionContext context) {
        if (columns.isEmpty()) {
            throw new IllegalStateException("Lucene value scan dispatched with no output columns");
        }
        List<LuceneDocValuesReader> readers = new ArrayList<>(columns.size());
        for (String columnName : columns) {
            readers.add(LuceneDocValuesReader.forField(columnName, context.getMapperService()));
        }
        return readers;
    }

    /** The Arrow fields those readers write into, in the same order. */
    static List<Field> arrowFields(List<LuceneDocValuesReader> readers) {
        List<Field> fields = new ArrayList<>(readers.size());
        for (LuceneDocValuesReader reader : readers) {
            fields.add(reader.arrowField());
        }
        return fields;
    }

    /**
     * Fills {@code root} with every document matching {@code filter} and returns the row count.
     *
     * <p>Iteration is per leaf over the filter query's scorer, intersected with {@code liveDocs} so
     * deleted documents are skipped ({@code Weight.scorer} does not apply deletions). Doc-value
     * cursors require ascending doc ids within a leaf, which the scorer's {@link DocIdSetIterator}
     * guarantees.
     *
     * <p>All columns are emitted nullable, matching the {@code NULLABILITY_NULLABLE} the coordinator's
     * Substrait stub declares in {@code LuceneFragmentConvertor.convertSchemaOnlyRead}. A mismatch
     * there stalls the coordinator's partition stream rather than failing, so the two must agree.
     */
    static int scanInto(IndexSearcher searcher, Query filter, List<LuceneDocValuesReader> readers, VectorSchemaRoot root, ShardId shardId)
        throws IOException {
        root.allocateNew();
        Weight weight = searcher.createWeight(searcher.rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        int rowIndex = 0;
        for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            Bits liveDocs = leaf.reader().getLiveDocs();
            List<LuceneDocValuesReader.LeafCursor> cursors = new ArrayList<>(readers.size());
            for (LuceneDocValuesReader reader : readers) {
                cursors.add(reader.open(leaf.reader()));
            }
            DocIdSetIterator docs = scorer.iterator();
            for (int docId = docs.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = docs.nextDoc()) {
                if (liveDocs != null && liveDocs.get(docId) == false) {
                    continue;
                }
                if (rowIndex >= LuceneSearchExecEngine.MAX_VALUE_SCAN_ROWS) {
                    throw new IllegalStateException(
                        "Lucene value scan on shard "
                            + shardId
                            + " exceeded the single-batch limit of "
                            + LuceneSearchExecEngine.MAX_VALUE_SCAN_ROWS
                            + " rows"
                    );
                }
                for (int col = 0; col < cursors.size(); col++) {
                    cursors.get(col).append(docId, rowIndex, root.getVector(col));
                }
                rowIndex++;
            }
        }
        root.setRowCount(rowIndex);
        return rowIndex;
    }
}
