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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchRequest;
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
    }



    @Override
    protected void executePhaseOnShard(SearchShardIterator shardIt, SearchShardTarget shard, SearchActionListener<FlintSkippingIndexResponse> listener) {
        listener.onResponse(new FlintSkippingIndexResponse(new int[]{1}));

        // getSearchTransport().sendExecuteQuery(getConnection(shard.getClusterAlias(), shard.getNodeId()), request, getTask(), listener);
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
