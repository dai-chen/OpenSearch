/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.response.agg;

import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.TopHits;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/** {@link TopHits} metric parser. */
public class TopHitsParser implements MetricParser {

    private final String name;
    private final boolean returnSingleValue;

    public TopHitsParser(String name) {
        this.name = name;
        this.returnSingleValue = false;
    }

    public TopHitsParser(String name, boolean returnSingleValue) {
        this.name = name;
        this.returnSingleValue = returnSingleValue;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Map<String, Object> parse(Aggregation agg) {
        TopHits topHits = (TopHits) agg;
        SearchHit[] hits = topHits.getHits().getHits();

        if (hits.length == 0) {
            return Collections.singletonMap(agg.getName(), null);
        }

        if (returnSingleValue) {
            // Extract the single value from the first (and only) hit from fields (fetchField)
            if (hits[0].getFields() != null && !hits[0].getFields().isEmpty()) {
                Object value = hits[0].getFields().values().iterator().next().getValue();
                return Collections.singletonMap(agg.getName(), value);
            }
            return Collections.singletonMap(agg.getName(), null);
        } else {
            // Return all values as a list from fields (fetchField)
            if (hits[0].getFields() != null && !hits[0].getFields().isEmpty()) {
                return Collections.singletonMap(
                    agg.getName(),
                    Arrays.stream(hits)
                        .flatMap(h -> h.getFields().values().stream())
                        .map(f -> f.getValue())
                        .filter(v -> v != null) // Filter out null values
                        .collect(Collectors.toList())
                );
            }
            return Collections.singletonMap(agg.getName(), Collections.emptyList());
        }
    }
}
