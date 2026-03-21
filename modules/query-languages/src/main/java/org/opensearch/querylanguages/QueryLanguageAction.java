/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages;

import org.opensearch.action.ActionType;

/**
 * Action type for SQL/PPL query execution.
 *
 * @opensearch.internal
 */
public class QueryLanguageAction extends ActionType<QueryLanguageResponse> {

    /** Singleton instance. */
    public static final QueryLanguageAction INSTANCE = new QueryLanguageAction();

    /** Action name. */
    public static final String NAME = "indices:data/read/query_language";

    private QueryLanguageAction() {
        super(NAME, QueryLanguageResponse::new);
    }
}
