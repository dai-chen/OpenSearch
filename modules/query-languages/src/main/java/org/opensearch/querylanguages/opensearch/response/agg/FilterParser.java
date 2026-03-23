/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.response.agg;

import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.bucket.filter.Filter;

import java.util.Map;

/**
 * {@link Filter} Parser. The current use case is filter aggregation, e.g. avg(age)
 * filter(balance>0). The filter parser do nothing and return the result from metricsParser.
 */
public class FilterParser implements MetricParser {

    private final MetricParser metricsParser;

    private final String name;

    @Override
    public String getName() {
        return name;
    }

    /** Constructor. */
    public FilterParser(MetricParser metricsParser, String name) {
        this.metricsParser = metricsParser;
        this.name = name;
    }

    @Override
    public Map<String, Object> parse(Aggregation aggregations) {
        return metricsParser.parse(((Filter) aggregations).getAggregations().asList().get(0));
    }
}
