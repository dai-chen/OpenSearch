/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/** Basic tests for {@link QueryLanguagesPlugin}. */
public class QueryLanguagesPluginTests extends OpenSearchTestCase {

    /** Verify plugin can be instantiated and registers handlers. */
    public void testPluginCreation() throws IOException {
        try (QueryLanguagesPlugin plugin = new QueryLanguagesPlugin()) {
            assertNotNull(plugin);
            assertEquals(1, plugin.getRestHandlers(null, null, null, null, null, null, null).size());
            assertEquals(1, plugin.getActions().size());
        }
    }
}
