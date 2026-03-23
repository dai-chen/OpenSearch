/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.data.value;

import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.querylanguages.opensearch.data.type.OpenSearchTextType;

/** Expression Text Value, it is a extension of the ExprValue by OpenSearch. */
public class OpenSearchExprTextValue extends ExprStringValue {
  public OpenSearchExprTextValue(String value) {
    super(value);
  }

  @Override
  public ExprType type() {
    return OpenSearchTextType.of();
  }
}
