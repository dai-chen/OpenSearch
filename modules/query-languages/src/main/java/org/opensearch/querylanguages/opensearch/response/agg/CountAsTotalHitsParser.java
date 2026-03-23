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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CountAsTotalHitsParser implements OpenSearchAggregationResponseParser {

    // countAggNameList dedicated the list of count aggregations which are filled by hits.total.value
    private final List<String> countAggNameList;

    public CountAsTotalHitsParser(List<String> countAggNameList) {
        this.countAggNameList = countAggNameList;
    }

    @Override
    public List<Map<String, Object>> parse(Aggregations aggregations) {
        throw new UnsupportedOperationException("CountAsTotalHitsParser doesn't support parse(Aggregations)");
    }

    @Override
    public List<Map<String, Object>> parse(SearchHits hits) {
        Map<String, Object> resultMap = new HashMap<>();
        countAggNameList.forEach(name -> resultMap.put(name, hits.getTotalHits().value()));
        return Collections.singletonList(resultMap);
    }
}
