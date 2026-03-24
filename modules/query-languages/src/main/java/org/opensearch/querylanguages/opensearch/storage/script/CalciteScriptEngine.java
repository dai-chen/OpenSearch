/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.storage.script;

import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptEngine;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Stub script engine for Calcite-based query execution.
 * Script-based filter/sort/aggregation expressions are not yet supported in the PoC.
 *
 * @opensearch.internal
 */
public class CalciteScriptEngine implements ScriptEngine {

    /** The language name for calcite scripts. */
    public static final String CALCITE_LANG_NAME = "opensearch_calcite_script";

    @Override
    public String getType() {
        return CALCITE_LANG_NAME;
    }

    @Override
    public <FactoryType> FactoryType compile(String name, String code, ScriptContext<FactoryType> context, Map<String, String> params) {
        throw new UnsupportedOperationException("Calcite script engine not yet supported in core PoC");
    }

    @Override
    public Set<ScriptContext<?>> getSupportedContexts() {
        return Collections.emptySet();
    }
}
