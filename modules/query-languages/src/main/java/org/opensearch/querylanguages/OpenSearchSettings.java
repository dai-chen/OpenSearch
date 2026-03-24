/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.sql.common.setting.Settings;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OpenSearch cluster-settings-wired implementation of Settings.
 * Registers dynamic settings and listens for updates.
 *
 * @opensearch.internal
 */
public class OpenSearchSettings extends Settings {

    /** Calcite engine enabled setting. */
    public static final Setting<Boolean> CALCITE_ENGINE_ENABLED_SETTING = Setting.boolSetting(
        Key.CALCITE_ENGINE_ENABLED.getKeyValue(),
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Calcite pushdown enabled setting. */
    public static final Setting<Boolean> CALCITE_PUSHDOWN_ENABLED_SETTING = Setting.boolSetting(
        Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(),
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Calcite pushdown rowcount estimation factor. */
    public static final Setting<Double> CALCITE_PUSHDOWN_ROWCOUNT_FACTOR_SETTING = Setting.doubleSetting(
        Key.CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR.getKeyValue(),
        1.0,
        0.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Query size limit. */
    public static final Setting<Integer> QUERY_SIZE_LIMIT_SETTING = Setting.intSetting(
        Key.QUERY_SIZE_LIMIT.getKeyValue(),
        200,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Query bucket size. */
    public static final Setting<Integer> QUERY_BUCKET_SIZE_SETTING = Setting.intSetting(
        Key.QUERY_BUCKET_SIZE.getKeyValue(),
        1000,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** SQL enabled. */
    public static final Setting<Boolean> SQL_ENABLED_SETTING = Setting.boolSetting(
        Key.SQL_ENABLED.getKeyValue(),
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** PPL enabled. */
    public static final Setting<Boolean> PPL_ENABLED_SETTING = Setting.boolSetting(
        Key.PPL_ENABLED.getKeyValue(),
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** SQL cursor keep alive. */
    public static final Setting<TimeValue> SQL_CURSOR_KEEP_ALIVE_SETTING = Setting.positiveTimeSetting(
        Key.SQL_CURSOR_KEEP_ALIVE.getKeyValue(),
        TimeValue.timeValueMinutes(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Calcite fallback allowed. */
    public static final Setting<Boolean> CALCITE_FALLBACK_ALLOWED_SETTING = Setting.boolSetting(
        Key.CALCITE_FALLBACK_ALLOWED.getKeyValue(),
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Field type tolerance. */
    public static final Setting<Boolean> FIELD_TYPE_TOLERANCE_SETTING = Setting.boolSetting(
        Key.FIELD_TYPE_TOLERANCE.getKeyValue(),
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final Map<Key, Setting<?>> KEY_SETTING_MAP;

    static {
        Map<Key, Setting<?>> m = new HashMap<>();
        m.put(Key.CALCITE_ENGINE_ENABLED, CALCITE_ENGINE_ENABLED_SETTING);
        m.put(Key.CALCITE_PUSHDOWN_ENABLED, CALCITE_PUSHDOWN_ENABLED_SETTING);
        m.put(Key.CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR, CALCITE_PUSHDOWN_ROWCOUNT_FACTOR_SETTING);
        m.put(Key.QUERY_SIZE_LIMIT, QUERY_SIZE_LIMIT_SETTING);
        m.put(Key.QUERY_BUCKET_SIZE, QUERY_BUCKET_SIZE_SETTING);
        m.put(Key.SQL_ENABLED, SQL_ENABLED_SETTING);
        m.put(Key.PPL_ENABLED, PPL_ENABLED_SETTING);
        m.put(Key.SQL_CURSOR_KEEP_ALIVE, SQL_CURSOR_KEEP_ALIVE_SETTING);
        m.put(Key.CALCITE_FALLBACK_ALLOWED, CALCITE_FALLBACK_ALLOWED_SETTING);
        m.put(Key.FIELD_TYPE_TOLERANCE, FIELD_TYPE_TOLERANCE_SETTING);
        KEY_SETTING_MAP = Collections.unmodifiableMap(m);
    }

    private final Map<Key, Object> latestSettings = new ConcurrentHashMap<>();

    /**
     * Creates OpenSearchSettings with defaults and registers cluster settings listeners.
     *
     * @param clusterSettings the cluster settings to register with
     */
    public OpenSearchSettings(ClusterSettings clusterSettings) {
        // Initialize defaults
        latestSettings.put(Key.CALCITE_ENGINE_ENABLED, true);
        latestSettings.put(Key.CALCITE_PUSHDOWN_ENABLED, true);
        latestSettings.put(Key.CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR, 1.0);
        latestSettings.put(Key.QUERY_SIZE_LIMIT, 200);
        latestSettings.put(Key.QUERY_BUCKET_SIZE, 1000);
        latestSettings.put(Key.SEARCH_MAX_BUCKETS, 65535);
        latestSettings.put(Key.QUERY_MEMORY_LIMIT, new ByteSizeValue((long) (Runtime.getRuntime().maxMemory() * 0.85)));
        latestSettings.put(Key.FIELD_TYPE_TOLERANCE, false);
        latestSettings.put(Key.SQL_ENABLED, true);
        latestSettings.put(Key.PPL_ENABLED, true);
        latestSettings.put(Key.SQL_CURSOR_KEEP_ALIVE, TimeValue.timeValueMinutes(1));
        latestSettings.put(Key.CALCITE_FALLBACK_ALLOWED, false);

        // Register dynamic update listeners
        clusterSettings.addSettingsUpdateConsumer(CALCITE_ENGINE_ENABLED_SETTING, v -> latestSettings.put(Key.CALCITE_ENGINE_ENABLED, v));
        clusterSettings.addSettingsUpdateConsumer(
            CALCITE_PUSHDOWN_ENABLED_SETTING,
            v -> latestSettings.put(Key.CALCITE_PUSHDOWN_ENABLED, v)
        );
        clusterSettings.addSettingsUpdateConsumer(
            CALCITE_PUSHDOWN_ROWCOUNT_FACTOR_SETTING,
            v -> latestSettings.put(Key.CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR, v)
        );
        clusterSettings.addSettingsUpdateConsumer(QUERY_SIZE_LIMIT_SETTING, v -> latestSettings.put(Key.QUERY_SIZE_LIMIT, v));
        clusterSettings.addSettingsUpdateConsumer(QUERY_BUCKET_SIZE_SETTING, v -> latestSettings.put(Key.QUERY_BUCKET_SIZE, v));
        clusterSettings.addSettingsUpdateConsumer(SQL_ENABLED_SETTING, v -> latestSettings.put(Key.SQL_ENABLED, v));
        clusterSettings.addSettingsUpdateConsumer(PPL_ENABLED_SETTING, v -> latestSettings.put(Key.PPL_ENABLED, v));
        clusterSettings.addSettingsUpdateConsumer(SQL_CURSOR_KEEP_ALIVE_SETTING, v -> latestSettings.put(Key.SQL_CURSOR_KEEP_ALIVE, v));
        clusterSettings.addSettingsUpdateConsumer(
            CALCITE_FALLBACK_ALLOWED_SETTING,
            v -> latestSettings.put(Key.CALCITE_FALLBACK_ALLOWED, v)
        );
        clusterSettings.addSettingsUpdateConsumer(FIELD_TYPE_TOLERANCE_SETTING, v -> latestSettings.put(Key.FIELD_TYPE_TOLERANCE, v));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getSettingValue(Key key) {
        return (T) latestSettings.get(key);
    }

    @Override
    public List<?> getSettings() {
        return Collections.emptyList();
    }

    /**
     * Returns all registered Setting objects for plugin registration.
     *
     * @return list of settings
     */
    public static List<Setting<?>> pluginSettings() {
        return List.of(
            CALCITE_ENGINE_ENABLED_SETTING,
            CALCITE_PUSHDOWN_ENABLED_SETTING,
            CALCITE_PUSHDOWN_ROWCOUNT_FACTOR_SETTING,
            QUERY_SIZE_LIMIT_SETTING,
            QUERY_BUCKET_SIZE_SETTING,
            SQL_ENABLED_SETTING,
            PPL_ENABLED_SETTING,
            SQL_CURSOR_KEEP_ALIVE_SETTING,
            CALCITE_FALLBACK_ALLOWED_SETTING,
            FIELD_TYPE_TOLERANCE_SETTING
        );
    }
}
