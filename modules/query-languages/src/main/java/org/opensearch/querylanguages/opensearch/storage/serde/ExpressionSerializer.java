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
 * Expression serializer interface.
 *
 * @opensearch.internal
 */
public interface ExpressionSerializer {

    /**
     * Serialize an expression.
     * @param expr expression
     * @return serialized string
     */
    String serialize(Expression expr);

    /**
     * Deserialize an expression.
     * @param code serialized code
     * @return original expression object
     */
    Expression deserialize(String code);
}
