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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** No Bucket Aggregation Parser which include only metric parsers. */
public class NoBucketAggregationParser implements OpenSearchAggregationResponseParser {

    private final MetricParserHelper metricsParser;

    /** Constructor. */
    public NoBucketAggregationParser(MetricParserHelper metricsParser) {
        this.metricsParser = metricsParser;
    }

    public NoBucketAggregationParser(MetricParser... metricParserList) {
        metricsParser = new MetricParserHelper(Arrays.asList(metricParserList));
    }

    public NoBucketAggregationParser(List<MetricParser> metricParserList) {
        metricsParser = new MetricParserHelper(metricParserList);
    }

    @Override
    public List<Map<String, Object>> parse(Aggregations aggregations) {
        return Collections.singletonList(metricsParser.parse(aggregations));
    }

    @Override
    public List<Map<String, Object>> parse(SearchHits hits) {
        throw new UnsupportedOperationException("NoBucketAggregationParser doesn't support parse(SearchHits)");
    }
}
