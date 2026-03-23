/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.response.agg;

import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;

import java.util.List;
import java.util.Map;

/** OpenSearch Aggregation Response Parser. */
public interface OpenSearchAggregationResponseParser {

    /**
     * Parse the OpenSearch Aggregation Response.
     *
     * @param aggregations Aggregations.
     * @return aggregation result.
     */
    List<Map<String, Object>> parse(Aggregations aggregations);

    /**
     * Parse the search hits response.
     *
     * @param hit the search hits
     * @return parsed result
     */
    List<Map<String, Object>> parse(SearchHits hit);
}
