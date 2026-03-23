/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.request;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.querylanguages.opensearch.request.PredicateAnalyzer.QueryExpression;
import org.opensearch.querylanguages.opensearch.response.agg.FilterParser;
import org.opensearch.querylanguages.opensearch.response.agg.MetricParser;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;

/** Analyzer for converting aggregate filter conditions into OpenSearch filter aggregations. */
public class AggregateFilterAnalyzer {

    /** Helper containing row type, field types, and cluster context for analysis. */
    private final AggregateAnalyzer.AggregateBuilderHelper helper;

    /** Project containing filter expressions referenced by aggregate calls. */
    private final Project project;

    /** Constructor. */
    public AggregateFilterAnalyzer(AggregateAnalyzer.AggregateBuilderHelper helper, Project project) {
        this.helper = helper;
        this.project = project;
    }

    /**
     * Analyzes and applies filter to aggregation if the AggregateCall has a filter condition.
     *
     * @param aggResult the base aggregation and parser to potentially wrap with filter
     * @param aggCall the aggregate call which may contain filter information
     * @param aggFieldName name for the filtered aggregation
     * @return wrapped aggregation with filter if present, otherwise the original result
     * @throws PredicateAnalyzer.ExpressionNotAnalyzableException if filter condition cannot be
     *     analyzed
     */
    public Pair<AggregationBuilder, MetricParser> analyze(
        Pair<AggregationBuilder, MetricParser> aggResult,
        AggregateCall aggCall,
        String aggFieldName
    ) throws PredicateAnalyzer.ExpressionNotAnalyzableException {
        if (project == null || !aggCall.hasFilter()) {
            return aggResult;
        }

        QueryExpression queryExpression = analyzeAggregateFilter(aggCall);
        return Pair.of(
            buildFilterAggregation(aggResult.getLeft(), aggFieldName, queryExpression),
            buildFilterParser(aggResult.getRight(), aggFieldName)
        );
    }

    private QueryExpression analyzeAggregateFilter(AggregateCall aggCall) throws PredicateAnalyzer.ExpressionNotAnalyzableException {
        RexNode filterCondition = project.getProjects().get(aggCall.filterArg);
        return PredicateAnalyzer.analyzeExpression(
            filterCondition,
            helper.rowType.getFieldNames(),
            helper.fieldTypes,
            helper.rowType,
            helper.cluster
        );
    }

    private AggregationBuilder buildFilterAggregation(AggregationBuilder aggBuilder, String aggFieldName, QueryExpression queryExpression) {
        return AggregationBuilders.filter(aggFieldName, queryExpression.builder()).subAggregation(aggBuilder);
    }

    private MetricParser buildFilterParser(MetricParser aggParser, String aggFieldName) {
        return new FilterParser(aggParser, aggFieldName);
    }
}
