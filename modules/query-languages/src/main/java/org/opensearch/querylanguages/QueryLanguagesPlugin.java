/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages;

import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Plugin that registers SQL and PPL query language endpoints.
 *
 * @opensearch.internal
 */
public class QueryLanguagesPlugin extends Plugin implements ActionPlugin {

    /** Creates a new QueryLanguagesPlugin. */
    public QueryLanguagesPlugin() {}

    /**
     * Returns REST handlers for SQL and PPL endpoints.
     *
     * @param settings the node settings
     * @param restController the REST controller
     * @param clusterSettings the cluster settings
     * @param indexScopedSettings the index-scoped settings
     * @param settingsFilter the settings filter
     * @param indexNameExpressionResolver the index name expression resolver
     * @param nodesInCluster supplier of discovery nodes in the cluster
     * @return list of REST handlers
     */
    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return Arrays.asList(new RestPplAction(), new RestSqlAction());
    }

    /**
     * Returns transport actions for SQL and PPL query execution.
     *
     * @return list of action handlers
     */
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Collections.singletonList(new ActionHandler<>(QueryLanguageAction.INSTANCE, TransportQueryLanguageAction.class));
    }
}
