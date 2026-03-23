/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.response.agg;

import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.ExtendedStats;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.querylanguages.opensearch.response.agg.Utils.handleNanInfValue;

/** {@link ExtendedStats} metric parser. */
public class StatsParser implements MetricParser {

    private final Function<ExtendedStats, Double> valueExtractor;

    private final String name;

    @Override
    public String getName() {
        return name;
    }

    /** Constructor. */
    public StatsParser(Function<ExtendedStats, Double> valueExtractor, String name) {
        this.valueExtractor = valueExtractor;
        this.name = name;
    }

    @Override
    public Map<String, Object> parse(Aggregation agg) {
        return Collections.singletonMap(agg.getName(), handleNanInfValue(valueExtractor.apply((ExtendedStats) agg)));
    }
}
