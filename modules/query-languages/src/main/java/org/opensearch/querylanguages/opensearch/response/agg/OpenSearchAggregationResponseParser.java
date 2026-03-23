/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.response.agg;

import java.util.List;
import java.util.Map;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;

/** OpenSearch Aggregation Response Parser. */
public interface OpenSearchAggregationResponseParser {

  /**
   * Parse the OpenSearch Aggregation Response.
   *
   * @param aggregations Aggregations.
   * @return aggregation result.
   */
  List<Map<String, Object>> parse(Aggregations aggregations);

  List<Map<String, Object>> parse(SearchHits hit);
}
