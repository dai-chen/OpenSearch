/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.storage.serde;

import org.opensearch.querylanguages.opensearch.storage.script.CompoundedScriptEngine.ScriptEngineType;

/**
 * Serialization wrapper for script language type and encoded script.
 *
 * @opensearch.internal
 */
public class SerializationWrapper {

    /** Key for language type in JSON. */
    public static final String LANG_TYPE = "langType";
    /** Key for script in JSON. */
    public static final String SCRIPT = "script";

    /**
     * Wrap script with language type as JSON string.
     *
     * @param langType script language type
     * @param script original script
     * @return JSON string
     */
    public static String wrapWithLangType(ScriptEngineType langType, String script) {
        return String.format(java.util.Locale.ROOT, "{\"%s\":\"%s\",\"%s\":\"%s\"}", LANG_TYPE, langType, SCRIPT, script);
    }

    private SerializationWrapper() {}
}
