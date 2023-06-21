/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

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
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.transport.Transport;

/**
 * Query phase that check if any Flint skipping index can accelerate the query.
 */
class FlintSkippingIndexQueryPhase extends AbstractSearchAsyncAction<SearchPhaseResult> {

    /**
     * Next query phase
     */
    private final Function<GroupShardsIterator<SearchShardIterator>, SearchPhase> phaseFactory;

    /**
     * All shards in the index
     */
    private final GroupShardsIterator<SearchShardIterator> shardsIts;

    /**
     * Node client that sends high level query request
     */
    private final NodeClient client;


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
        SearchResponse.Clusters clusters,
        NodeClient client
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
        this.client = client;
    }

    @Override
    public void run() {
        if (skippingIndexExists()) {
            List<String> indexedColumns = getIndexedColumnsFromMetadata();
            QueryBuilder skippingIndexQuery = pushDownFilterToSkippingIndex(indexedColumns);

            if (skippingIndexQuery == null) {
                executeNextPhase();
            } else {
                getShardIdFromSkippingIndex(skippingIndexQuery, new SkippingIndexResponseListener() {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        Set<Integer> shardIds = parseShardIdFromSearchResponse(searchResponse);
                        markSkippedShards(shardIds);
                        executeNextPhase();
                    }
                });
            }
        } else {
            executeNextPhase();
        }
    }

    @Override
    protected void executePhaseOnShard(SearchShardIterator shardIt, SearchShardTarget shard, SearchActionListener<SearchPhaseResult> listener) {
        // No need to implement because we override run()
    }

    @Override
    protected SearchPhase getNextPhase(SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
        return null; // No need to implement because we override run()
    }

    private void executeNextPhase() {
        try {
            phaseFactory.apply(shardsIts).run();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String skippingIndexName() {
        String indexName = getRequest().indices()[0]; // Assume only 1 index being queried
        return "flint_" + indexName + "_skipping_index";
    }

    private boolean skippingIndexExists() {
        return clusterState.metadata().hasIndex(skippingIndexName());
    }

    // Parse indexed column list out of skipping index metadata
    @SuppressWarnings("unchecked")
    private List<String> getIndexedColumnsFromMetadata() {
        IndexMetadata indexMetadata = clusterState.metadata().index(skippingIndexName());
        Map<String, Object> meta = (Map<String, Object>) indexMetadata.mapping().sourceAsMap().get("_meta");
        return (List<String>) meta.get("indexedColumns");
    }

    // Traverse query plan tree to collect filtering conditions that can be pushed down
    private QueryBuilder pushDownFilterToSkippingIndex(List<String> indexedColumns) {
        QueryBuilder sourceQuery = getRequest().source().query();
        QueryBuilder skippingIndexQuery = doTraverseSourceQuery(sourceQuery, new HashSet<>(indexedColumns));

        if (skippingIndexQuery != null) {
            System.out.println("Push down query: " + skippingIndexQuery);
        }
        return skippingIndexQuery;
    }

    private QueryBuilder doTraverseSourceQuery(QueryBuilder sourceQuery, Set<String> indexedColumns) {
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
                QueryBuilder newChildQuery = doTraverseSourceQuery(childQuery, indexedColumns);
                if (newChildQuery != null) {
                    indexQuery.must(newChildQuery);
                }
            }
            return indexQuery.must().isEmpty() ? null : indexQuery;
        }

        // Query cannot be supported now
        return null;
    }

    private void getShardIdFromSkippingIndex(QueryBuilder indexQuery, SkippingIndexResponseListener listener) {
        SearchRequest indexRequest = new SearchRequest();
        indexRequest.indices(skippingIndexName());
        indexRequest.source(new SearchSourceBuilder());
        indexRequest.source().query(indexQuery);

        client.search(indexRequest, listener);
    }

    private Set<Integer> parseShardIdFromSearchResponse(SearchResponse searchResponse) {
        Set<Integer> shardIds = new HashSet<>();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        for (SearchHit hit : searchHits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            Integer shardId = (Integer) sourceAsMap.get("shardId");
            shardIds.add(shardId);
        }
        return shardIds;
    }

    private void markSkippedShards(Set<Integer> shardIds) {
        System.out.println("Shards to scan: " + shardIds);

        for (SearchShardIterator shard : shardsIts) {
            if (shardIds.contains(shard.shardId().getId())) {
                shard.reset();
            } else {
                shard.resetAndSkip();
            }
        }
    }

    // Custom class for readability
    private static abstract class SkippingIndexResponseListener implements ActionListener<SearchResponse> {
        @Override
        public void onFailure(Exception e) {
            throw new RuntimeException("Failed to query skipping index", e);
        }
    }
}
