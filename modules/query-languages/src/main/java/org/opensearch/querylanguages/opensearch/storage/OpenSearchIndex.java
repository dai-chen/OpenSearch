/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.storage;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.CompositeMap;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.querylanguages.opensearch.client.OpenSearchClient;
import org.opensearch.querylanguages.opensearch.data.type.OpenSearchDataType;
import org.opensearch.querylanguages.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.querylanguages.opensearch.monitor.OpenSearchMemoryHealthy;
import org.opensearch.querylanguages.opensearch.monitor.OpenSearchResourceMonitor;
import org.opensearch.querylanguages.opensearch.request.OpenSearchRequest;
import org.opensearch.querylanguages.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.querylanguages.opensearch.request.system.OpenSearchDescribeIndexRequest;
import org.opensearch.querylanguages.opensearch.storage.scan.CalciteLogicalIndexScan;
import org.opensearch.sql.calcite.plan.AbstractOpenSearchTable;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/** OpenSearch table (index) implementation. */
public class OpenSearchIndex extends AbstractOpenSearchTable {

    public static final String METADATA_FIELD_ID = "_id";
    public static final String METADATA_FIELD_INDEX = "_index";
    public static final String METADATA_FIELD_SCORE = "_score";
    public static final String METADATA_FIELD_MAXSCORE = "_maxscore";
    public static final String METADATA_FIELD_SORT = "_sort";

    public static final String METADATA_FIELD_ROUTING = "_routing";

    public static final java.util.Map<String, ExprType> METADATAFIELD_TYPE_MAP = new LinkedHashMap<>() {
        {
            put(METADATA_FIELD_ID, ExprCoreType.STRING);
            put(METADATA_FIELD_INDEX, ExprCoreType.STRING);
            put(METADATA_FIELD_SCORE, ExprCoreType.FLOAT);
            put(METADATA_FIELD_MAXSCORE, ExprCoreType.FLOAT);
            put(METADATA_FIELD_SORT, ExprCoreType.LONG);
            put(METADATA_FIELD_ROUTING, ExprCoreType.STRING);
        }
    };

    /** OpenSearch client connection. */
    private final OpenSearchClient client;

    private final Settings settings;

    /** {@link OpenSearchRequest.IndexName}. */
    private final OpenSearchRequest.IndexName indexName;

    /** The cached mapping of field and type in index. */
    private Map<String, OpenSearchDataType> cachedFieldOpenSearchTypes = null;

    /** The cached ExprType of fields. */
    private Map<String, ExprType> cachedFieldTypes = null;

    /** The cached mapping of alias type field to its original path. */
    private Map<String, String> aliasMapping = null;

    /** The cached max result window setting of index. */
    private Integer cachedMaxResultWindow = null;

    /** Constructor. */
    public OpenSearchIndex(OpenSearchClient client, Settings settings, String indexName) {
        this.client = client;
        this.settings = settings;
        this.indexName = new OpenSearchRequest.IndexName(indexName);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        final RelOptCluster cluster = context.getCluster();
        return new CalciteLogicalIndexScan(cluster, relOptTable, this);
    }

    @Override
    public boolean exists() {
        return client.exists(indexName.toString());
    }

    @Override
    public void create(Map<String, ExprType> schema) {
        Map<String, Object> mappings = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        mappings.put("properties", properties);

        for (Map.Entry<String, ExprType> colType : schema.entrySet()) {
            properties.put(colType.getKey(), colType.getValue().legacyTypeName().toLowerCase(java.util.Locale.ROOT));
        }
        client.createIndex(indexName.toString(), mappings);
    }

    /*
     * TODO: Assume indexName doesn't have wildcard.
     *  Need to either handle field name conflicts
     *   or lazy evaluate when query engine pulls field type.
     */
    /**
     * Get simplified parsed mapping info. Unlike {@link #getFieldOpenSearchTypes()} it returns a
     * flattened map.
     *
     * @return A map between field names and matching `ExprCoreType`s.
     */
    @Override
    public Map<String, ExprType> getFieldTypes() {
        if (cachedFieldOpenSearchTypes == null) {
            cachedFieldOpenSearchTypes = new OpenSearchDescribeIndexRequest(client, indexName).getFieldTypes();
        }
        if (cachedFieldTypes == null) {
            cachedFieldTypes = OpenSearchDataType.traverseAndFlatten(cachedFieldOpenSearchTypes)
                .entrySet()
                .stream()
                .collect(LinkedHashMap::new, (map, item) -> map.put(item.getKey(), item.getValue().getExprType()), Map::putAll);
        }
        return cachedFieldTypes;
    }

    @Override
    public Map<String, ExprType> getReservedFieldTypes() {
        return METADATAFIELD_TYPE_MAP;
    }

    // Return all field types including reserved fields
    public Map<String, ExprType> getAllFieldTypes() {
        return CompositeMap.of(getFieldTypes(), getReservedFieldTypes());
    }

    public Map<String, String> getAliasMapping() {
        if (cachedFieldOpenSearchTypes == null) {
            cachedFieldOpenSearchTypes = new OpenSearchDescribeIndexRequest(client, indexName).getFieldTypes();
        }
        if (aliasMapping == null) {
            aliasMapping = OpenSearchDataType.traverseAndFlatten(cachedFieldOpenSearchTypes)
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().getOriginalPath().isPresent())
                .collect(Collectors.toUnmodifiableMap(Entry::getKey, entry -> entry.getValue().getOriginalPath().get()));
        }
        return aliasMapping;
    }

    /**
     * Get parsed mapping info.
     *
     * @return A complete map between field names and their types.
     */
    public Map<String, OpenSearchDataType> getFieldOpenSearchTypes() {
        if (cachedFieldOpenSearchTypes == null) {
            cachedFieldOpenSearchTypes = new OpenSearchDescribeIndexRequest(client, indexName).getFieldTypes();
        }
        return cachedFieldOpenSearchTypes;
    }

    /** Get the max result window setting of the table. */
    public Integer getMaxResultWindow() {
        if (cachedMaxResultWindow == null) {
            cachedMaxResultWindow = new OpenSearchDescribeIndexRequest(client, indexName).getMaxResultWindow();
        }
        return cachedMaxResultWindow;
    }

    public Integer getBucketSize() {
        return Math.min(
            settings.getSettingValue(Settings.Key.QUERY_BUCKET_SIZE),
            settings.getSettingValue(Settings.Key.SEARCH_MAX_BUCKETS)
        );
    }

    @Override
    public PhysicalPlan implement(LogicalPlan plan) {
        // TODO: Legacy V1 engine implementation not yet supported in core
        throw new UnsupportedOperationException("Legacy engine implement() not supported in core");
    }

    private OpenSearchExprValueFactory createExprValueFactory() {
        Map<String, OpenSearchDataType> allFields = new HashMap<>();
        getReservedFieldTypes().forEach((k, v) -> allFields.put(k, OpenSearchDataType.of(v)));
        allFields.putAll(getFieldOpenSearchTypes());
        return new OpenSearchExprValueFactory(allFields, settings.getSettingValue(Settings.Key.FIELD_TYPE_TOLERANCE));
    }

    public boolean isFieldTypeTolerance() {
        return settings.getSettingValue(Settings.Key.FIELD_TYPE_TOLERANCE);
    }

    public Settings getSettings() {
        return settings;
    }

    public OpenSearchClient getClient() {
        return client;
    }

    public OpenSearchRequestBuilder createRequestBuilder() {
        return new OpenSearchRequestBuilder(createExprValueFactory(), getMaxResultWindow(), settings);
    }

    public OpenSearchResourceMonitor createOpenSearchResourceMonitor() {
        return new OpenSearchResourceMonitor(settings, new OpenSearchMemoryHealthy(settings));
    }

    public OpenSearchRequest buildRequest(OpenSearchRequestBuilder requestBuilder) {
        final TimeValue cursorKeepAlive = settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE);
        return requestBuilder.build(indexName, cursorKeepAlive, client, cachedFieldOpenSearchTypes.isEmpty());
    }
}
