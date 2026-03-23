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

import java.util.Collections;
import java.util.Map;

/** {@link TopHits} metric parser for ARG_MAX/ARG_MIN aggregations. */
public class ArgMaxMinParser implements MetricParser {

    private final String name;

    /** Constructor. */
    public ArgMaxMinParser(String name) {
        this.name = name;
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

        if (hits[0].getFields() != null && !hits[0].getFields().isEmpty()) {
            Object value = hits[0].getFields().values().iterator().next().getValue();
            return Collections.singletonMap(agg.getName(), value);
        }

        return Collections.singletonMap(agg.getName(), null);
    }
}
