/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.storage.script;

/**
 * Stub for CompoundedScriptEngine — only the ScriptEngineType enum and lang name constant
 * are needed by the aggregation DSL builders.
 *
 * @opensearch.internal
 */
public class CompoundedScriptEngine {

    /** The language name for compounded scripts. */
    public static final String COMPOUNDED_LANG_NAME = "opensearch_compounded_script";

    /** Script engine type enum. */
    public enum ScriptEngineType {
        /** V1 legacy engine. */
        V1("v1"),
        /** V2 Calcite engine. */
        V2("v2");

        private final String type;

        ScriptEngineType(String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return type;
        }
    }

    private CompoundedScriptEngine() {}
}
