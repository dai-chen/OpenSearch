/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Vectorized aggregator that calculates average in vector-wise way by Arrow.
 */
public class VectorizedAvgAggregator extends NumericMetricsAggregator.SingleValue {

    private final ValuesSource.Numeric valuesSource;
    private final Map<Long, Summary> summaries = new HashMap<>();
    private final DocValueFormat format;

    protected VectorizedAvgAggregator(String name, ValuesSourceConfig valuesSourceConfig,
                                      SearchContext context, Aggregator parent,
                                      Map<String, Object> metadata) throws IOException {
        super(name, context, parent, metadata);

        this.valuesSource = (ValuesSource.Numeric) valuesSourceConfig.getValuesSource();
        this.format = valuesSourceConfig.format();
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return null;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        SortedNumericDoubleValues values = valuesSource.doubleValues(ctx); // change to double later to be fair
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                Summary summary = VectorizedAvgAggregator.this.summaries.get(bucket);
                if (summary == null) {
                    summary = new ArrowVectorSummary();
                    summaries.put(bucket, summary);
                }

                if (values.advanceExact(doc)) {
                    for (int i = 0; i < values.docValueCount(); i++) {
                        summary.add(values.nextValue());
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrd) throws IOException {
        if (valuesSource == null || !summaries.containsKey(owningBucketOrd)) {
            return buildEmptyAggregation();
        }
        Summary summary = summaries.get(owningBucketOrd);
        return new InternalAvg(name, summary.sum(), summary.count(), format, metadata());
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (valuesSource == null || !summaries.containsKey(owningBucketOrd)) {
            return Double.NaN;
        }
        Summary summary = summaries.get(owningBucketOrd);
        return summary.sum() / summary.count();
    }

    @Override
    protected void doClose() {
        summaries.forEach((k, v) -> {
            try {
                v.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private interface Summary extends AutoCloseable {
        int count();
        double sum();
        void add(double num);
    }

    private static class ArrowVectorSummary implements Summary {
        // Reuse 1 allocator
        private static RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        private static Projector projector;

        private static Field fieldA;

        private static Field fieldB;

        static {
            ArrowType.FloatingPoint doubleType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            fieldA = Field.notNullable("a", doubleType);
            fieldB = Field.notNullable("b", doubleType);
            TreeNode add = TreeBuilder.makeFunction("add",
                Arrays.asList(TreeBuilder.makeField(fieldA), TreeBuilder.makeField(fieldB)),
                doubleType);

            // Compile project expression a = a + b
            try {
                projector = Projector.make(new Schema(Arrays.asList(fieldA, fieldB)),
                    List.of(TreeBuilder.makeExpression(add, fieldA)));
            } catch (GandivaException e) {
                throw new RuntimeException(e);
            }
        }

        private static final int LANE_SIZE = 2;
        private final Float8Vector vectorA = new Float8Vector(fieldA, rootAllocator);
        private final Float8Vector vectorB = new Float8Vector(fieldB, rootAllocator);
        private int count;
        private int cur;

        ArrowVectorSummary() {
            vectorA.allocateNew(LANE_SIZE);
            vectorB.allocateNew(LANE_SIZE);

            for (int i = 0; i < LANE_SIZE; i++) {
                vectorA.set(i, 0);
            }
            vectorA.setValueCount(LANE_SIZE);
        }

        @Override
        public int count() {
            return count + cur;
        }

        @Override
        public double sum() {
            accumulate();

            int sum = 0;
            for (int i = 0; i < vectorA.getValueCount(); i++) {
                sum += vectorA.get(i);
            }
            return sum;
        }

        @Override
        public void add(double num) {
            vectorB.set(cur++, num);

            if (cur == LANE_SIZE) {
                accumulate();
            }
        }

        private void accumulate() {
            if (cur == 0) {
                return;
            }

            for (int i = cur; i < LANE_SIZE; i++) { // use mask?
                vectorB.set(i, 0);
            }
            vectorB.setValueCount(LANE_SIZE);

            VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(Arrays.asList(fieldA, fieldB), Arrays.asList(vectorA, vectorB));
            VectorUnloader unloader = new VectorUnloader(vectorSchemaRoot);
            ArrowRecordBatch recordBatch = unloader.getRecordBatch();
            try {
                projector.evaluate(recordBatch, List.of(vectorA));
            } catch (GandivaException e) {
                throw new RuntimeException(e);
            }

            count += cur;
            cur = 0;
            vectorB.allocateNew(LANE_SIZE); // reset() not work
        }

        @Override
        public void close() throws Exception {
            vectorB.close();
            vectorA.close();
            projector.close();
            // rootAllocator.close(); // how to release buffer ledger???
        }
    }


    /* JDK Vector API implementation (requires JDK 17 as source/target compatibility which breaks OpenSearch build)
    private static class JdkVectorSummary {
        private final VectorSpecies<Double> species = DoubleVector.SPECIES_MAX;
        private double[] array = new double[species.length()];
        private DoubleVector sum;
        private int count;
        private int cur;

        Summary() {
        }

        void add(double num) {
            array[cur++] = num;
            if (cur == array.length) {
                doSum();
            }
        }

        int count() {
            return count + cur;
        }

        double sum() {
            doSum();
            return sum.reduceLanes(VectorOperators.ADD);
        }

        private void doSum() {
            DoubleVector vector = DoubleVector.fromArray(species, array, 0);
            if (sum == null) {
                sum = vector;
            } else {
                sum = sum.add(vector);
            }

            count += cur;
            array = new double[species.length()];
        }
    }
    */
}
