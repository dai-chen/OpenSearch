/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for POST /_ppl endpoint.
 *
 * @opensearch.internal
 */
public class RestPplAction extends BaseRestHandler {

    /** Creates a new RestPplAction. */
    public RestPplAction() {}

    @Override
    public String getName() {
        return "query_languages_ppl_action";
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_ppl"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String query;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            parser.nextToken(); // START_OBJECT
            String fieldName = null;
            query = null;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if ("query".equals(fieldName)) {
                    query = parser.text();
                }
            }
        }
        if (query == null || query.isEmpty()) {
            throw new IllegalArgumentException("request body must contain a 'query' field");
        }
        QueryLanguageRequest langRequest = new QueryLanguageRequest(query, QueryType.PPL);
        return channel -> client.execute(QueryLanguageAction.INSTANCE, langRequest, new RestToXContentListener<>(channel));
    }
}
