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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Arrays;
import java.util.Collection;
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

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        OpenSearchSettings openSearchSettings = new OpenSearchSettings(clusterService.getClusterSettings());
        return Collections.singletonList(openSearchSettings);
    }

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

    /**
     * Returns executor builders for thread pools used by the query pipeline.
     *
     * @param settings the node settings
     * @return list of executor builders
     */
    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return Collections.singletonList(
            new ScalingExecutorBuilder("sql_background_io", 1, 4, org.opensearch.common.unit.TimeValue.timeValueMinutes(5))
        );
    }

    /**
     * Returns settings registered by this plugin.
     *
     * @return list of settings
     */
    @Override
    public List<Setting<?>> getSettings() {
        return OpenSearchSettings.pluginSettings();
    }
}
