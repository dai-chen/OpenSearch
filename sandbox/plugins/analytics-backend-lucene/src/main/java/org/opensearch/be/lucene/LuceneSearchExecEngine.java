/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.backend.ShardScanExecutionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Lucene-side {@link SearchExecEngine}. Mirrors {@code DatafusionSearchExecEngine}'s role
 * for the Lucene backend: takes the {@link LuceneSearcherState} produced upstream by the
 * instruction handler, executes the operation, and returns an {@link EngineResultStream}
 * the framework drains into the Flight transport.
 *
 * <p>Two operations, selected by {@link LuceneSearcherState#kind()}:
 *
 * <ul>
 *   <li>{@link LuceneFragmentKind#COUNT} — the metadata-only fast path,
 *       {@link org.apache.lucene.search.IndexSearcher#count(org.apache.lucene.search.Query)}.
 *       No deletes gate needed: {@code IndexSearcher.count} is self-healing, since per-leaf
 *       {@code Weight.count(leaf)} returns -1 on dirty leaves and falls back to full iteration.</li>
 *   <li>{@link LuceneFragmentKind#VALUE_SCAN} — row values read out of Lucene doc values for the
 *       fragment's output columns. This is what lets the analytics engine scan a plain
 *       (non-composite) index; on a composite index the same columns' doc values live in the
 *       parquet primary, and {@code LuceneFragmentConvertor.isValueScanFastPath} keeps the planner
 *       from choosing this path there.</li>
 * </ul>
 *
 * <p>Both results are exported through the Arrow C-Data interface so the result VSR has the same
 * foreign-allocation-managed buffer layout DataFusion's result stream produces. Pure-Java
 * {@code setSafe}-built VSRs don't survive Flight's {@code VectorTransfer.transferRoot}; see
 * {@link LuceneResultStream} for the detailed comparison.
 *
 * @opensearch.internal
 */
final class LuceneSearchExecEngine implements SearchExecEngine<ShardScanExecutionContext, EngineResultStream> {

    private static final Logger LOGGER = LogManager.getLogger(LuceneSearchExecEngine.class);

    /**
     * Upper bound on rows a single value-scan fragment may return per shard. {@link LuceneResultStream}
     * carries exactly one {@link ArrowArray}, so the scan cannot yet chunk into multiple batches;
     * failing loudly beats silently truncating. Lifting this means teaching the result stream to hold
     * a queue of exported arrays.
     */
    static final int MAX_VALUE_SCAN_ROWS = 1_000_000;

    private final LuceneSearcherState state;

    LuceneSearchExecEngine(LuceneSearcherState state) {
        this.state = state;
    }

    @Override
    public void prepare(ShardScanExecutionContext context) {
        // No preparation needed — the LuceneSearcherState was fully built by the instruction
        // handler. {@code prepare} is part of the SearchExecEngine contract for backends that
        // need to assemble plans from the context (e.g. DataFusion); Lucene has nothing to do.
    }

    @Override
    public EngineResultStream execute(ShardScanExecutionContext context) throws IOException {
        return switch (state.kind()) {
            case COUNT -> executeCount(context);
            case VALUE_SCAN -> executeValueScan(context);
        };
    }

    // ── count fast path ──────────────────────────────────────────────────────

    private EngineResultStream executeCount(ShardScanExecutionContext context) throws IOException {
        long count = state.searcher().count(state.filterQuery());
        LOGGER.debug(
            "[lucene-count] shardId={} query={} count={} columns={}",
            context.getShardId(),
            state.filterQuery(),
            count,
            state.outputColumnNames()
        );
        Schema schema = buildCountSchema(state.outputColumnNames());
        return export(context.getAllocator(), schema, (root, allocator) -> {
            root.allocateNew();
            for (int i = 0; i < state.outputColumnNames().size(); i++) {
                ((BigIntVector) root.getVector(i)).setSafe(0, count);
            }
            root.setRowCount(1);
        });
    }

    private static Schema buildCountSchema(List<String> columnNames) {
        FieldType int64Nullable = new FieldType(true, new ArrowType.Int(64, true), null);
        List<Field> fields = new ArrayList<>(columnNames.size());
        for (String name : columnNames) {
            fields.add(new Field(name, int64Nullable, null));
        }
        return new Schema(fields);
    }

    // ── doc-values value scan ────────────────────────────────────────────────

    /**
     * Materialises the fragment's output columns from Lucene doc values.
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
    private EngineResultStream executeValueScan(ShardScanExecutionContext context) throws IOException {
        List<String> columnNames = state.outputColumnNames();
        List<LuceneDocValuesReader> readers = LuceneValueScanProducer.readers(columnNames, context);
        Schema schema = new Schema(LuceneValueScanProducer.arrowFields(readers));

        EngineResultStream stream = export(context.getAllocator(), schema, (root, allocator) -> {
            int rowIndex = LuceneValueScanProducer.scanInto(state.searcher(), state.filterQuery(), readers, root, context.getShardId());
            LOGGER.debug(
                "[lucene-value-scan] shardId={} query={} columns={} rows={}",
                context.getShardId(),
                state.filterQuery(),
                columnNames,
                rowIndex
            );
        });
        return stream;
    }

    // ── Arrow C-Data export ──────────────────────────────────────────────────

    /** Fills a scratch {@link VectorSchemaRoot}; may throw {@link IOException} while reading Lucene. */
    private interface BatchPopulator {
        void populate(VectorSchemaRoot root, BufferAllocator allocator) throws IOException;
    }

    /**
     * Builds a scratch VSR for {@code schema}, hands it to {@code populator}, exports it through the
     * Arrow C-Data interface and wraps the result in a {@link LuceneResultStream}. Mirrors the export
     * side of {@code DatafusionResultStream}'s contract: the populated {@link ArrowArray} is what the
     * stream re-imports — the same call shape DataFusion uses for native batches.
     */
    private static EngineResultStream export(BufferAllocator allocator, Schema schema, BatchPopulator populator) throws IOException {
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        boolean transferred = false;
        try {
            VectorSchemaRoot scratch = VectorSchemaRoot.create(schema, allocator);
            try {
                populator.populate(scratch, allocator);
                try (CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()) {
                    Data.exportVectorSchemaRoot(allocator, scratch, dictProvider, array, arrowSchema);
                }
            } finally {
                scratch.close();
            }
            LuceneResultStream stream = new LuceneResultStream(array, arrowSchema, allocator);
            transferred = true;
            return stream;
        } finally {
            if (transferred == false) {
                try {
                    array.close();
                } finally {
                    arrowSchema.close();
                }
            }
        }
    }
}
