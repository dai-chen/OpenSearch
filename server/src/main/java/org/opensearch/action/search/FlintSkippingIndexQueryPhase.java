/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import static org.opensearch.action.search.FlintSkippingIndexQueryPhase.FlintSkippingIndexResponse;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.transport.Transport;

/**
 * Query phase that check if any Flint skipping index can accelerate the query.
 */
class FlintSkippingIndexQueryPhase extends AbstractSearchAsyncAction<FlintSkippingIndexResponse> {

    /** Next query phase */
    private final Function<GroupShardsIterator<SearchShardIterator>, SearchPhase> phaseFactory;

    /**
     * All shards in the index
     */
    private final GroupShardsIterator<SearchShardIterator> shardsIts;

    private final ClusterState clusterState;

    FlintSkippingIndexQueryPhase(
        Logger logger,
        SearchTransportService searchTransportService,
        BiFunction<String, String, Transport.Connection> nodeIdToConnection,
        Map<String, AliasFilter> aliasFilter,
        Map<String, Float> concreteIndexBoosts,
        Map<String, Set<String>> indexRoutings,
        Executor executor,
        SearchRequest request,
        ActionListener<SearchResponse> listener,
        GroupShardsIterator<SearchShardIterator> shardsIts,
        TransportSearchAction.SearchTimeProvider timeProvider,
        ClusterState clusterState,
        SearchTask task,
        Function<GroupShardsIterator<SearchShardIterator>, SearchPhase> phaseFactory,
        SearchResponse.Clusters clusters
    ) {
        super(
            "flint_skipping_index",
            logger,
            searchTransportService,
            nodeIdToConnection,
            aliasFilter,
            concreteIndexBoosts,
            indexRoutings,
            executor,
            request,
            listener,
            shardsIts,
            timeProvider,
            clusterState,
            task,
            new ArraySearchPhaseResults<>(shardsIts.size()),
            shardsIts.size(),
            clusters
        );
        this.phaseFactory = phaseFactory;
        this.shardsIts = shardsIts;

        this.clusterState = clusterState;
    }

    // Remove unused constructor params and extends from QueryPhase directly if this works
    /*
     logic: assume all filtering conditions concat by AND
        1) Check if alb_logs_skipping_index exists
        2) Get skipping index meta info from _meta in mapping
        3) Generate query plan tree (QueryBuilder?)
        4) Traverse tree and find if any field present in skipping index meta info
        5) If any filtering condition matched, send query to skipping index
        output: Return shard ID list
     */
    @SuppressWarnings("unchecked")
    public void test() {
        String indexName = "flint_test_skipping_index";
        Metadata metadata = clusterState.metadata();

        // Check if skipping index exists
        if (metadata.hasIndex(indexName)) {
            System.out.println("Skipping index exists");

            // Get indexed columns from skipping index meta field
            IndexMetadata indexMetadata = metadata.index(indexName);
            Map<String, Object> meta = (Map<String, Object>) indexMetadata.mapping().sourceAsMap().get("_meta");
            List<String> indexCols = (List<String>) meta.get("indexedColumns");

            // Traverse query plan tree to collect filtering conditions that can be pushed down
            QueryBuilder sourceQuery = getRequest().source().query();
            QueryBuilder indexQuery = pushDownFilterToSkippingIndex(sourceQuery, new HashSet<>(indexCols));
            if (indexQuery != null) {
                SearchRequest indexRequest = new SearchRequest();
                indexRequest.source().query(indexQuery);
                // getSearchTransport().sendExecuteQuery();

                System.out.println("Pushdown query: " + indexQuery);
            }
        }
    }

    private QueryBuilder pushDownFilterToSkippingIndex(QueryBuilder sourceQuery, Set<String> indexedColumns) {
        if (sourceQuery instanceof TermQueryBuilder) {
            TermQueryBuilder filter = (TermQueryBuilder) sourceQuery;
            if (indexedColumns.contains(filter.fieldName())) {
                return QueryBuilders.termQuery(filter.fieldName(), filter.value());
            }
        }

        if (sourceQuery instanceof BoolQueryBuilder) {
            BoolQueryBuilder boolQuery = (BoolQueryBuilder) sourceQuery;
            BoolQueryBuilder indexQuery = QueryBuilders.boolQuery();

            for (QueryBuilder childQuery : boolQuery.must()) {
                QueryBuilder newChildQuery = pushDownFilterToSkippingIndex(childQuery, indexedColumns);
                if (newChildQuery != null) {
                    indexQuery.must(newChildQuery);
                }
            }
            return indexQuery.must().isEmpty() ? null : indexQuery;
        }

        // Query cannot be supported now
        return null;
    }

    /*
    private static QueryBuilder filterQueryByColumns(QueryBuilder originalQuery, List<String> columnList) {
        if (originalQuery instanceof QueryBuilders.BoolQueryBuilder) {
            QueryBuilders.BoolQueryBuilder boolQuery = (QueryBuilders.BoolQueryBuilder) originalQuery;
            QueryBuilders.BoolQueryBuilder filteredBoolQuery = QueryBuilders.boolQuery();

            for (QueryBuilder innerQuery : boolQuery.filter()) {
                QueryBuilder filteredInnerQuery = filterQueryByColumns(innerQuery, columnList);
                if (filteredInnerQuery != null) {
                    filteredBoolQuery.filter(filteredInnerQuery);
                }
            }

            return filteredBoolQuery.hasClauses() ? filteredBoolQuery : null;
        }

        if (originalQuery instanceof QueryBuilders.TermQueryBuilder) {
            QueryBuilders.TermQueryBuilder termQuery = (QueryBuilders.TermQueryBuilder) originalQuery;
            String fieldName = termQuery.fieldName();
            if (columnList.contains(fieldName)) {
                return originalQuery;
            }
        }

        return null;
    }
    */

    @Override
    protected void executePhaseOnShard(SearchShardIterator shardIt, SearchShardTarget shard, SearchActionListener<FlintSkippingIndexResponse> listener) {
        test();

        listener.onResponse(new FlintSkippingIndexResponse(new int[]{1}));
    }

    @Override
    protected SearchPhase getNextPhase(SearchPhaseResults<FlintSkippingIndexResponse> results, SearchPhaseContext context) {
        return phaseFactory.apply(markSkippedShards());
    }

    private GroupShardsIterator<SearchShardIterator> markSkippedShards() {
        for (SearchShardIterator shard : shardsIts) {
            if (shard.shardId().getId() == 1) { // assume only shard 1 may match
                shard.reset();
            } else {
                shard.resetAndSkip();
            }
        }
        return shardsIts;
    }

    public static final class FlintSkippingIndexResponse extends SearchPhaseResult {

        /**
         * Shards that may have match for the query.
         */
        private int[] shardIds;

        public FlintSkippingIndexResponse(StreamInput in) throws IOException {
            this.shardIds = in.readIntArray();
        }

        public FlintSkippingIndexResponse(int[] shardIds) {
            this.shardIds = shardIds;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeIntArray(shardIds);
        }

        public int[] getShardIds() {
            return shardIds;
        }
    }
}
