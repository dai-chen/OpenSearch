/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.response.agg;

import org.opensearch.search.aggregations.Aggregation;

import java.util.Map;

/** Metric Aggregation Parser. */
public interface MetricParser {

    /** Get the name of metric parser. */
    String getName();

    /**
     * Parse the {@link Aggregation}.
     *
     * @param aggregation {@link Aggregation}
     * @return the map between metric name and metric value.
     */
    Map<String, Object> parse(Aggregation aggregation);
}
