/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages;

import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;

/** Integration test verifying the query-languages module loads correctly. */
public class QueryLanguagesPluginIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(QueryLanguagesPlugin.class);
    }

    /** Verify the cluster starts with the plugin loaded. */
    public void testPluginLoaded() {
        ensureGreen();
        assertFalse(client().admin().cluster().prepareNodesInfo().get().getNodes().isEmpty());
    }
}
