/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.response.agg;

import com.google.common.collect.Streams;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.Percentiles;

import java.util.Collections;
import java.util.Map;

public class SinglePercentileParser implements MetricParser {

    private final String name;

    @Override
    public String getName() {
        return name;
    }

    /** Constructor. */
    public SinglePercentileParser(String name) {
        this.name = name;
    }

    @Override
    public Map<String, Object> parse(Aggregation agg) {
        return Collections.singletonMap(
            agg.getName(),
            // TODO `Percentiles` implements interface
            // `org.opensearch.search.aggregations.metrics.MultiValue`, but there is not
            // method `values()` available in this interface. So we
            Streams.stream(((Percentiles) agg).iterator()).findFirst().get().getValue()
        );
    }
}
