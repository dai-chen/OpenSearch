/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.response.agg;

import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregation;

import java.util.Collections;
import java.util.Map;

import static org.opensearch.querylanguages.opensearch.response.agg.Utils.handleNanInfValue;

/** {@link NumericMetricsAggregation.SingleValue} metric parser. */
public class SingleValueParser implements MetricParser {

    private final String name;

    @Override
    public String getName() {
        return name;
    }

    /** Constructor. */
    public SingleValueParser(String name) {
        this.name = name;
    }

    @Override
    public Map<String, Object> parse(Aggregation agg) {
        return Collections.singletonMap(agg.getName(), handleNanInfValue(((NumericMetricsAggregation.SingleValue) agg).value()));
    }
}
