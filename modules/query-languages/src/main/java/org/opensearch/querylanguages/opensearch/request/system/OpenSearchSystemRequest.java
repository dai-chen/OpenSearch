/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.request.system;

import java.util.List;
import org.opensearch.sql.data.model.ExprValue;

/** OpenSearch system request query against the system index. */
public interface OpenSearchSystemRequest {

  /**
   * Search.
   *
   * @return list of ExprValue.
   */
  List<ExprValue> search();
}
