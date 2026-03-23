/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.storage.scan.context;

/** Push down types. */
public enum PushDownType {
  FILTER,
  PROJECT,
  AGGREGATION,
  SORT,
  LIMIT,
  SCRIPT,
  COLLAPSE,
  SORT_AGG_METRICS, // convert composite aggregate to terms or multi-terms bucket aggregate
  RARE_TOP, // convert composite aggregate to nested aggregate
  // HIGHLIGHT,
  // NESTED
}
