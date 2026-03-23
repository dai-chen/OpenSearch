/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.data.value;

import org.opensearch.querylanguages.opensearch.data.type.OpenSearchBinaryType;
import org.opensearch.sql.data.model.AbstractExprValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;

/**
 * OpenSearch BinaryValue. Todo, add this to avoid the unknown value type exception, the
 * implementation will be changed.
 */
public class OpenSearchExprBinaryValue extends AbstractExprValue {
    private final String encodedString;

    public OpenSearchExprBinaryValue(String encodedString) {
        this.encodedString = encodedString;
    }

    @Override
    public int compare(ExprValue other) {
        return encodedString.compareTo((String) other.value());
    }

    @Override
    public boolean equal(ExprValue other) {
        return encodedString.equals(other.value());
    }

    @Override
    public Object value() {
        return encodedString;
    }

    @Override
    public ExprType type() {
        return OpenSearchBinaryType.of();
    }
}
