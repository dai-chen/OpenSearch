/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;

/**
 * Lucene-side shard-scan instruction handler. Reads a {@link ShardScanInstructionNode}
 * produced for a Lucene {@code StagePlan}, acquires the shard's Lucene reader, deserialises
 * the filter {@link QueryBuilder} from {@code ShardScanExecutionContext.getFragmentBytes()},
 * compiles it to a Lucene {@link Query}, and returns a {@link LuceneSearcherState} for
 * {@link LuceneSearchExecEngine} to execute.
 *
 * <p>Empty {@code fragmentBytes} → {@link MatchAllDocsQuery} (count(*) over the whole shard).
 *
 * @opensearch.internal
 */
final class LuceneScanInstructionHandler implements FragmentInstructionHandler<ShardScanInstructionNode> {

    private static final Logger LOGGER = LogManager.getLogger(LuceneScanInstructionHandler.class);

    private final LucenePlugin plugin;

    LuceneScanInstructionHandler(LucenePlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public BackendExecutionContext apply(
        ShardScanInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        ShardScanExecutionContext shardCtx = (ShardScanExecutionContext) commonContext;
        IndexReaderProvider.Reader reader = shardCtx.getReader();
        LuceneReaderAdapter.Resolved resolved = plugin.readerAdapter().resolve(reader);
        if (resolved == null) {
            throw new IllegalStateException("Lucene-driver fragment dispatched to a shard with no LuceneReader");
        }
        LuceneReader luceneReader = resolved.reader();
        // Shared per-reader searcher (see LuceneReader#searcher).
        IndexSearcher searcher = luceneReader.searcher(shardCtx.getQueryCache(), shardCtx.getQueryCachingPolicy());
        Decoded decoded = decodeFragmentBytes(shardCtx, searcher, resolved.hasDocValues());
        LOGGER.debug(
            "[lucene-driver] shardId={} kind={} filterQuery={} columnNames={}",
            shardCtx.getShardId(),
            decoded.kind,
            decoded.filterQuery,
            decoded.columnNames
        );
        return new LuceneSearcherState(searcher, decoded.filterQuery, decoded.columnNames, decoded.kind);
    }

    /**
     * Deserializes the wire format produced by {@link LuceneFragmentConvertor#convertFragment}:
     * {@code [kind VInt] [columnNames String[]] [hasFilter boolean] [QueryBuilder NamedWriteable]?}.
     * Empty bytes → count over MatchAllDocs with no column names (legacy/defensive fallback that
     * shouldn't happen on the Lucene-driver path but stays safe if the wire shape ever drifts).
     */
    private Decoded decodeFragmentBytes(ShardScanExecutionContext shardCtx, IndexSearcher searcher, boolean hasDocValues) {
        byte[] bytes = shardCtx.getFragmentBytes();
        if (bytes == null || bytes.length == 0) {
            return new Decoded(LuceneFragmentKind.COUNT, new MatchAllDocsQuery(), java.util.List.of());
        }
        try (StreamInput rawInput = StreamInput.wrap(bytes)) {
            StreamInput input = new NamedWriteableAwareStreamInput(rawInput, shardCtx.getNamedWriteableRegistry());
            LuceneFragmentKind kind = LuceneFragmentKind.fromOrdinal(input.readVInt());
            java.util.List<String> columnNames = input.readStringList();
            boolean hasFilter = input.readBoolean();
            Query filterQuery;
            if (hasFilter) {
                QueryShardContext qsc = LuceneAnalyticsBackendPlugin.buildMinimalQueryShardContext(shardCtx, searcher);
                QueryBuilder queryBuilder = input.readNamedWriteable(QueryBuilder.class);
                Query compiled = queryBuilder.toQuery(qsc);
                // Rewrite FieldExistsQuery → postings-only equivalent for the doc-values-less
                // lucene-secondary segment. A format that carries real doc values (and points) needs
                // no rewrite, and it would be wrong there — a numeric FieldExistsQuery turned into a
                // TermRangeQuery matches nothing against a points-indexed field. The decision comes
                // from the format id that published the reader, so it cannot disagree with the format
                // the planner resolved capabilities against.
                filterQuery = hasDocValues ? compiled : LuceneQueryConversionUtils.rewriteFieldExistsForSecondary(compiled);
            } else {
                filterQuery = new MatchAllDocsQuery();
            }
            return new Decoded(kind, filterQuery, columnNames);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to deserialize Lucene-driver fragment bytes", e);
        }
    }

    private record Decoded(LuceneFragmentKind kind, Query filterQuery, java.util.List<String> columnNames) {
    }
}
