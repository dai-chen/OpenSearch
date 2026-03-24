/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages;

import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.querylanguages.opensearch.client.OpenSearchNodeClient;
import org.opensearch.querylanguages.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transport action that executes SQL/PPL queries via the unified query pipeline.
 *
 * @opensearch.internal
 */
public class TransportQueryLanguageAction extends HandledTransportAction<QueryLanguageRequest, QueryLanguageResponse> {

    private final NodeClient nodeClient;
    private final OpenSearchSettings openSearchSettings;

    /**
     * Creates a new transport action.
     *
     * @param transportService the transport service
     * @param actionFilters the action filters
     * @param nodeClient the node client for OpenSearch operations
     * @param openSearchSettings the cluster-settings-wired settings
     */
    @Inject
    public TransportQueryLanguageAction(
        TransportService transportService,
        ActionFilters actionFilters,
        NodeClient nodeClient,
        OpenSearchSettings openSearchSettings
    ) {
        super(QueryLanguageAction.NAME, transportService, actionFilters, QueryLanguageRequest::new);
        this.nodeClient = nodeClient;
        this.openSearchSettings = openSearchSettings;
    }

    @Override
    protected void doExecute(Task task, QueryLanguageRequest request, ActionListener<QueryLanguageResponse> listener) {
        nodeClient.threadPool().executor("sql_background_io").execute(() -> doExecuteOnWorker(request, listener));
    }

    private void doExecuteOnWorker(QueryLanguageRequest request, ActionListener<QueryLanguageResponse> listener) {
        try {
            OpenSearchNodeClient osClient = new OpenSearchNodeClient(nodeClient);
            Settings settings = openSearchSettings;

            Schema schema = new AbstractSchema() {
                @Override
                protected Map<String, Table> getTableMap() {
                    Map<String, Table> tables = new HashMap<>();
                    for (String index : osClient.indices()) {
                        tables.put(index, new OpenSearchIndex(osClient, settings, index));
                    }
                    return tables;
                }
            };

            try (
                UnifiedQueryContext context = UnifiedQueryContext.builder()
                    .language(request.getQueryType())
                    .catalog("opensearch", schema)
                    .defaultNamespace("opensearch")
                    .setting("plugins.calcite.enabled", true)
                    .setting("plugins.calcite.pushdown.enabled", true)
                    .setting("plugins.query.size_limit", 200)
                    .build()
            ) {
                RelNode plan;
                if (request.getQueryType() == QueryType.SQL) {
                    FrameworkConfig baseConfig = context.getPlanContext().config;
                    FrameworkConfig sqlConfig = Frameworks.newConfigBuilder(baseConfig)
                        .parserConfig(SqlParser.config().withLex(Lex.JAVA))
                        .build();
                    Planner calcitePlanner = Frameworks.getPlanner(sqlConfig);
                    SqlNode parsed = calcitePlanner.parse(request.getQuery());
                    SqlNode validated = calcitePlanner.validate(parsed);
                    plan = calcitePlanner.rel(validated).rel;
                } else {
                    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
                    plan = planner.plan(request.getQuery());
                }

                UnifiedQueryCompiler compiler = new UnifiedQueryCompiler(context);
                PreparedStatement stmt = compiler.compile(plan);

                try (ResultSet rs = stmt.executeQuery()) {
                    ResultSetMetaData meta = rs.getMetaData();
                    int colCount = meta.getColumnCount();

                    List<String> columns = new ArrayList<>(colCount);
                    for (int i = 1; i <= colCount; i++) {
                        columns.add(meta.getColumnName(i));
                    }

                    List<List<Object>> dataRows = new ArrayList<>();
                    while (rs.next()) {
                        List<Object> row = new ArrayList<>(colCount);
                        for (int i = 1; i <= colCount; i++) {
                            row.add(rs.getObject(i));
                        }
                        dataRows.add(row);
                    }

                    listener.onResponse(new QueryLanguageResponse(columns, dataRows));
                } finally {
                    stmt.close();
                }
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
