/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.request;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.querylanguages.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.querylanguages.opensearch.response.OpenSearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.opensearch.querylanguages.opensearch.storage.OpenSearchIndex.METADATA_FIELD_ID;
import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;

/**
 * OpenSearch search request. This has to be stateful because it needs to:
 *
 * <p>1) Accumulate search source builder when visiting logical plan to push down operation. 2)
 * Indicate the search already done.
 */
public class OpenSearchQueryRequest implements OpenSearchRequest {

    /** {@link OpenSearchRequest.IndexName}. */
    private final IndexName indexName;

    /** Search request source builder. */
    private SearchSourceBuilder sourceBuilder;

    /** OpenSearchExprValueFactory. */
    private final OpenSearchExprValueFactory exprValueFactory;

    /** List of includes expected in the response. */
    private final List<String> includes;

    private boolean needClean = true;

    /** Indicate the search already done. */
    private boolean searchDone = false;

    private String pitId;

    private TimeValue cursorKeepAlive;

    private Object[] searchAfter;

    private SearchResponse searchResponse = null;

    /** Constructor of OpenSearchQueryRequest. */
    public OpenSearchQueryRequest(String indexName, int size, OpenSearchExprValueFactory factory, List<String> includes) {
        this(new IndexName(indexName), size, factory, includes);
    }

    /** Constructor of OpenSearchQueryRequest. */
    public OpenSearchQueryRequest(IndexName indexName, int size, OpenSearchExprValueFactory factory, List<String> includes) {
        this.indexName = indexName;
        this.sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(0);
        sourceBuilder.size(size);
        sourceBuilder.timeout(DEFAULT_QUERY_TIMEOUT);
        this.exprValueFactory = factory;
        this.includes = includes;
    }

    /** Constructor of OpenSearchQueryRequest. */
    public OpenSearchQueryRequest(
        IndexName indexName,
        SearchSourceBuilder sourceBuilder,
        OpenSearchExprValueFactory factory,
        List<String> includes
    ) {
        this.indexName = indexName;
        this.sourceBuilder = sourceBuilder;
        this.exprValueFactory = factory;
        this.includes = includes;
    }

    /** Constructor of OpenSearchQueryRequest with PIT support. */
    public OpenSearchQueryRequest(
        IndexName indexName,
        SearchSourceBuilder sourceBuilder,
        OpenSearchExprValueFactory factory,
        List<String> includes,
        TimeValue cursorKeepAlive,
        String pitId
    ) {
        this.indexName = indexName;
        this.sourceBuilder = sourceBuilder;
        this.exprValueFactory = factory;
        this.includes = includes;
        this.cursorKeepAlive = cursorKeepAlive;
        this.pitId = pitId;
    }

    @Override
    public OpenSearchExprValueFactory getExprValueFactory() {
        return exprValueFactory;
    }

    /** true if the request is a count aggregation request. */
    public boolean isCountAggRequest() {
        return !searchDone
            && sourceBuilder.size() == 0
            && sourceBuilder.trackTotalHitsUpTo() != null // only set in v3
            && sourceBuilder.trackTotalHitsUpTo() == Integer.MAX_VALUE;
    }

    /**
     * Constructs OpenSearchQueryRequest from serialized representation.
     *
     * @param in stream to read data from.
     * @param engine OpenSearchSqlEngine to get node-specific context.
     * @throws IOException thrown if reading from input {@code in} fails.
     */
    public OpenSearchQueryRequest(StreamInput in, Object engine) throws IOException {
        // TODO: Cursor/pagination deserialization not yet supported in core
        throw new UnsupportedOperationException("OpenSearchQueryRequest deserialization not yet supported in core");
    }

    @Override
    public OpenSearchResponse search(
        Function<SearchRequest, SearchResponse> searchAction,
        Function<SearchScrollRequest, SearchResponse> scrollAction
    ) {
        if (this.pitId == null) {
            // When SearchRequest doesn't contain PitId, fetch single page request
            if (searchDone) {
                return new OpenSearchResponse(SearchHits.empty(), exprValueFactory, includes, isCountAggRequest());
            } else {
                // get the value before set searchDone = true
                boolean isCountAggRequest = isCountAggRequest();
                searchDone = true;
                return new OpenSearchResponse(
                    searchAction.apply(new SearchRequest().indices(indexName.getIndexNames()).source(sourceBuilder)),
                    exprValueFactory,
                    includes,
                    isCountAggRequest
                );
            }
        } else {
            // Search with PIT instead of scroll API
            return searchWithPIT(searchAction);
        }
    }

    public OpenSearchResponse searchWithPIT(Function<SearchRequest, SearchResponse> searchAction) {
        OpenSearchResponse openSearchResponse;
        if (searchDone) {
            openSearchResponse = new OpenSearchResponse(SearchHits.empty(), exprValueFactory, includes, isCountAggRequest());
        } else {
            this.sourceBuilder.pointInTimeBuilder(new PointInTimeBuilder(this.pitId));
            this.sourceBuilder.timeout(cursorKeepAlive);
            // check for search after
            if (searchAfter != null) {
                this.sourceBuilder.searchAfter(searchAfter);
            }
            // Set sort field for search_after
            if (this.sourceBuilder.sorts() == null) {
                this.sourceBuilder.sort(DOC_FIELD_NAME, ASC);
                // Workaround to preserve sort location more exactly,
                // see https://github.com/opensearch-project/sql/pull/3061
                this.sourceBuilder.sort(METADATA_FIELD_ID, ASC);
            }
            SearchRequest searchRequest = new SearchRequest().indices(indexName.getIndexNames()).source(this.sourceBuilder);
            this.searchResponse = searchAction.apply(searchRequest);

            openSearchResponse = new OpenSearchResponse(this.searchResponse, exprValueFactory, includes, isCountAggRequest());

            needClean = openSearchResponse.isEmpty();
            searchDone = openSearchResponse.isEmpty();
            SearchHit[] searchHits = this.searchResponse.getHits().getHits();
            if (searchHits != null && searchHits.length > 0) {
                searchAfter = searchHits[searchHits.length - 1].getSortValues();
                this.sourceBuilder.searchAfter(searchAfter);
            }
        }
        return openSearchResponse;
    }

    @Override
    public void clean(Consumer<String> cleanAction) {
        try {
            // clean on the last page only, to prevent deleting the PitId in the middle of paging.
            if (this.pitId != null && needClean) {
                cleanAction.accept(this.pitId);
                searchDone = true;
            }
        } finally {
            this.pitId = null;
        }
    }

    @Override
    public void forceClean(Consumer<String> cleanAction) {
        try {
            if (this.pitId != null) {
                cleanAction.accept(this.pitId);
                searchDone = true;
            }
        } finally {
            this.pitId = null;
        }
    }

    @Override
    public boolean hasAnotherBatch() {
        if (this.pitId != null) {
            return !needClean;
        }
        return false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (this.pitId != null) {
            // Convert SearchSourceBuilder to XContent and write it as a string
            out.writeString(sourceBuilder.toString());

            out.writeTimeValue(sourceBuilder.timeout());
            out.writeString(sourceBuilder.pointInTimeBuilder().getId());
            out.writeStringCollection(includes);
            indexName.writeTo(out);

            // Serialize the searchAfter array
            if (searchAfter != null) {
                out.writeVInt(searchAfter.length);
                for (Object obj : searchAfter) {
                    out.writeGenericValue(obj);
                }
            }
        } else {
            // OpenSearch Query request without PIT for single page requests
            throw new UnsupportedOperationException("OpenSearchQueryRequest serialization is not implemented.");
        }
    }
}
