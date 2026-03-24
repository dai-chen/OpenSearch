/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.storage.serde;

import org.opensearch.sql.expression.Expression;

/**
 * Default expression serializer stub.
 * Cursor serialization is not yet supported in the PoC.
 *
 * @opensearch.internal
 */
public class DefaultExpressionSerializer implements ExpressionSerializer {

    @Override
    public String serialize(Expression expr) {
        throw new UnsupportedOperationException("Expression serialization not yet supported in core PoC");
    }

    @Override
    public Expression deserialize(String code) {
        throw new UnsupportedOperationException("Expression deserialization not yet supported in core PoC");
    }
}
