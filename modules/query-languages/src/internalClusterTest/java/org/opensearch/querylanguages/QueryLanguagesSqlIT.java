/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages;

import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;

/**
 * Integration test for the SQL endpoint.
 */
public class QueryLanguagesSqlIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(QueryLanguagesPlugin.class);
    }

    /**
     * Test SQL query: index documents, query via transport action, verify results.
     *
     * @throws Exception if query execution fails
     */
    public void testSqlQuery() throws Exception {
        String index = "test_sql";
        // Index sample documents
        for (int i = 1; i <= 3; i++) {
            XContentBuilder doc = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
            doc.startObject().field("name", "user" + i).field("age", 20 + i).endObject();
            client().prepareIndex(index).setId(String.valueOf(i)).setSource(doc).get();
        }
        client().admin().indices().prepareRefresh(index).get();

        // Execute SQL query via transport action
        QueryLanguageRequest request = new QueryLanguageRequest("SELECT name, age FROM " + index + " WHERE age > 21", QueryType.SQL);
        QueryLanguageResponse response = client().execute(QueryLanguageAction.INSTANCE, request).actionGet();

        // Verify response
        XContentBuilder builder = MediaTypeRegistry.contentBuilder(MediaTypeRegistry.JSON);
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        // Should have schema with name and age columns
        assertTrue("Response should contain schema", json.contains("\"schema\""));
        assertTrue("Response should contain datarows", json.contains("\"datarows\""));
        // age > 21 means age 22 (user2) and 23 (user3) — 2 rows
        assertTrue("Response should contain 2 rows", json.contains("\"total\":2"));
    }
}
