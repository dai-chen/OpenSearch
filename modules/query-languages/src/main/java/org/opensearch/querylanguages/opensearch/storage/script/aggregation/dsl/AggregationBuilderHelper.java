/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.storage.script.aggregation.dsl;

import org.opensearch.querylanguages.opensearch.data.type.OpenSearchTextType;
import org.opensearch.querylanguages.opensearch.storage.script.CompoundedScriptEngine.ScriptEngineType;
import org.opensearch.querylanguages.opensearch.storage.serde.ExpressionSerializer;
import org.opensearch.querylanguages.opensearch.storage.serde.SerializationWrapper;
import org.opensearch.script.Script;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.ReferenceExpression;

import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.opensearch.querylanguages.opensearch.storage.script.CompoundedScriptEngine.COMPOUNDED_LANG_NAME;
import static org.opensearch.script.Script.DEFAULT_SCRIPT_TYPE;

/** Abstract Aggregation Builder. */
public class AggregationBuilderHelper {

    private final ExpressionSerializer serializer;

    /** Constructor. */
    public AggregationBuilderHelper(ExpressionSerializer serializer) {
        this.serializer = serializer;
    }

    /**
     * Build AggregationBuilder from Expression.
     *
     * @param expression Expression
     * @return AggregationBuilder
     */
    public <T> T build(Expression expression, Function<String, T> fieldBuilder, Function<Script, T> scriptBuilder) {
        if (expression instanceof ReferenceExpression) {
            String fieldName = ((ReferenceExpression) expression).getAttr();
            return fieldBuilder.apply(OpenSearchTextType.convertTextToKeyword(fieldName, expression.type()));
        } else if (expression instanceof FunctionExpression || expression instanceof LiteralExpression) {
            return scriptBuilder.apply(
                new Script(
                    DEFAULT_SCRIPT_TYPE,
                    COMPOUNDED_LANG_NAME,
                    SerializationWrapper.wrapWithLangType(ScriptEngineType.V2, serializer.serialize(expression)),
                    emptyMap()
                )
            );
        } else {
            throw new IllegalStateException(
                String.format(java.util.Locale.ROOT, "metric aggregation doesn't support " + "expression %s", expression)
            );
        }
    }
}
