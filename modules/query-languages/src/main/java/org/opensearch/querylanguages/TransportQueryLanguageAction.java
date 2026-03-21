/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.client.OpenSearchNodeClient;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collections;
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

    /**
     * Creates a new transport action.
     *
     * @param transportService the transport service
     * @param actionFilters the action filters
     * @param nodeClient the node client for OpenSearch operations
     */
    @Inject
    public TransportQueryLanguageAction(TransportService transportService, ActionFilters actionFilters, NodeClient nodeClient) {
        super(QueryLanguageAction.NAME, transportService, actionFilters, QueryLanguageRequest::new);
        this.nodeClient = nodeClient;
    }

    @Override
    protected void doExecute(Task task, QueryLanguageRequest request, ActionListener<QueryLanguageResponse> listener) {
        try {
            OpenSearchNodeClient osClient = new OpenSearchNodeClient(nodeClient);
            Settings settings = new HardcodedSettings();

            Schema schema = new AbstractSchema() {
                @Override
                protected Map<String, Table> getTableMap() {
                    // Lazy: tables resolved by Calcite during planning
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
                    .setting("calcite.engine.enabled", true)
                    .setting("calcite.pushdown.enabled", true)
                    .setting("query.size_limit", 200)
                    .build()
            ) {
                UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
                RelNode plan = planner.plan(request.getQuery());

                UnifiedQueryCompiler compiler = new UnifiedQueryCompiler(context);
                PreparedStatement stmt = compiler.compile(plan);

                ResultSet rs = stmt.executeQuery();
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
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Hardcoded settings for the PoC — no configurable plugin settings.
     */
    static class HardcodedSettings extends Settings {
        private static final Map<Key, Object> DEFAULTS;

        static {
            Map<Key, Object> m = new HashMap<>();
            m.put(Key.CALCITE_ENGINE_ENABLED, true);
            m.put(Key.CALCITE_PUSHDOWN_ENABLED, true);
            m.put(Key.QUERY_SIZE_LIMIT, 200);
            m.put(Key.QUERY_BUCKET_SIZE, 1000);
            m.put(Key.SEARCH_MAX_BUCKETS, 65535);
            m.put(Key.QUERY_MEMORY_LIMIT, "85%");
            m.put(Key.FIELD_TYPE_TOLERANCE, false);
            m.put(Key.SQL_ENABLED, true);
            m.put(Key.PPL_ENABLED, true);
            m.put(Key.CALCITE_FALLBACK_ALLOWED, false);
            DEFAULTS = Collections.unmodifiableMap(m);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T getSettingValue(Key key) {
            return (T) DEFAULTS.get(key);
        }

        @Override
        public List<?> getSettings() {
            return Collections.emptyList();
        }
    }
}
