/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.sql.executor.QueryType;

import java.io.IOException;

/**
 * Request for SQL/PPL query execution.
 *
 * @opensearch.internal
 */
public class QueryLanguageRequest extends ActionRequest {

    private final String query;
    private final QueryType queryType;

    /**
     * Creates a new request.
     *
     * @param query the query string
     * @param queryType the query language type (SQL or PPL)
     */
    public QueryLanguageRequest(String query, QueryType queryType) {
        this.query = query;
        this.queryType = queryType;
    }

    /**
     * Creates a new request from a stream.
     *
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public QueryLanguageRequest(StreamInput in) throws IOException {
        super(in);
        this.query = in.readString();
        this.queryType = in.readEnum(QueryType.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(query);
        out.writeEnum(queryType);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (query == null || query.isEmpty()) {
            ActionRequestValidationException e = new ActionRequestValidationException();
            e.addValidationError("query is required");
            return e;
        }
        return null;
    }

    /**
     * Returns the query string.
     *
     * @return the query
     */
    public String getQuery() {
        return query;
    }

    /**
     * Returns the query type.
     *
     * @return the query type
     */
    public QueryType getQueryType() {
        return queryType;
    }
}
