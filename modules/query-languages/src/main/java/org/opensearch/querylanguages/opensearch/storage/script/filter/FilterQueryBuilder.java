/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.storage.script.filter;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.querylanguages.opensearch.storage.serde.ExpressionSerializer;
import org.opensearch.sql.expression.Expression;

/**
 * Builds OpenSearch filter queries from expressions.
 * TODO: Phase 3 — full implementation with Lucene query support.
 *
 * @opensearch.internal
 */
public class FilterQueryBuilder {

    /**
     * Creates a new FilterQueryBuilder.
     * @param serializer the expression serializer
     */
    public FilterQueryBuilder(ExpressionSerializer serializer) {}

    /**
     * Build a QueryBuilder from an expression.
     * @param expr the filter expression
     * @return the query builder
     */
    public QueryBuilder build(Expression expr) {
        throw new UnsupportedOperationException("Script-based filter not yet supported in core. Use Calcite push-down path.");
    }
}
