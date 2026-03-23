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
import org.opensearch.search.aggregations.metrics.Percentile;
import org.opensearch.search.aggregations.metrics.Percentiles;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class PercentilesParser implements MetricParser {

    private final String name;

    @Override
    public String getName() {
        return name;
    }

    /** Constructor. */
    public PercentilesParser(String name) {
        this.name = name;
    }

    @Override
    public Map<String, Object> parse(Aggregation agg) {
        return Collections.singletonMap(
            agg.getName(),
            // TODO a better implementation here is providing a class `MultiValueParser`
            // similar to `SingleValueParser`. However, there is no method `values()` available
            // in `org.opensearch.search.aggregations.metrics.MultiValue`.
            Streams.stream(((Percentiles) agg).iterator()).map(Percentile::getValue).collect(Collectors.toList())
        );
    }
}
