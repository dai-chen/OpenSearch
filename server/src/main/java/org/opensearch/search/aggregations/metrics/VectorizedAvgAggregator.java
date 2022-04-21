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
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.ObjectArray;
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
import java.util.List;
import java.util.Map;

/**
 * Vectorized aggregator that calculates average in vector-wise way by Arrow.
 */
public class VectorizedAvgAggregator extends NumericMetricsAggregator.SingleValue {

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat format;
    private ObjectArray<Summary> summaries;

    protected VectorizedAvgAggregator(String name, ValuesSourceConfig valuesSourceConfig,
                                      SearchContext context, Aggregator parent,
                                      Map<String, Object> metadata) throws IOException {
        super(name, context, parent, metadata);

        this.valuesSource = (ValuesSource.Numeric) valuesSourceConfig.getValuesSource();
        this.format = valuesSourceConfig.format();
        this.summaries = context.bigArrays().newObjectArray(0);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalAvg(name, 0.0, 0L, format, metadata());
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        SortedNumericDoubleValues values = valuesSource.doubleValues(ctx); // change to double later to be fair
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (summaries.size() <= bucket) {
                    summaries = context.bigArrays().grow(summaries, bucket + 1);
                    summaries.set(bucket, new ArrowVectorSummary());
                }

                if (values.advanceExact(doc)) {
                    for (int i = 0; i < values.docValueCount(); i++) {
                        summaries.get(bucket).add(values.nextValue());
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrd) throws IOException {
        if (valuesSource == null || summaries.size() <= owningBucketOrd) {
            return buildEmptyAggregation();
        }
        Summary summary = summaries.get(owningBucketOrd);
        return new InternalAvg(name, summary.sum(), summary.count(), format, metadata());
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (valuesSource == null || summaries.size() <= owningBucketOrd) {
            return Double.NaN;
        }
        Summary summary = summaries.get(owningBucketOrd);
        return summary.sum() / summary.count();
    }

    @Override
    protected void doClose() {
        Releasables.close(summaries); // close each
    }

    public interface Summary extends AutoCloseable {
        int count();
        double sum();
        void add(double num);
    }

    public static class ArrowVectorSummary implements Summary {
        // Reuse 1 allocator
        private static RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        private static Projector projector;

        private static Field fieldSum;
        private static Field fieldNext;

        static {
            ArrowType.FloatingPoint doubleType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            fieldSum = Field.notNullable("sum", doubleType);
            fieldNext = Field.notNullable("next", doubleType);
            TreeNode add = TreeBuilder.makeFunction("add",
                Arrays.asList(TreeBuilder.makeField(fieldSum), TreeBuilder.makeField(fieldNext)),
                doubleType);

            // Compile project expression sum = sum + next
            try {
                projector = Projector.make(new Schema(Arrays.asList(fieldSum, fieldNext)),
                    List.of(TreeBuilder.makeExpression(add, fieldSum)));
            } catch (GandivaException e) {
                throw new RuntimeException(e);
            }
        }

        private static final int LANE_SIZE = 16;
        private final Float8Vector vectorSum = new Float8Vector(fieldSum, rootAllocator);
        private final Float8Vector vectorNext = new Float8Vector(fieldNext, rootAllocator);
        private int count;
        private int cur;

        public ArrowVectorSummary() {
            vectorSum.allocateNew(LANE_SIZE);
            vectorNext.allocateNew(LANE_SIZE);

            for (int i = 0; i < LANE_SIZE; i++) {
                vectorSum.set(i, 0);
            }
            vectorSum.setValueCount(LANE_SIZE);
        }

        @Override
        public int count() {
            return count + cur;
        }

        @Override
        public double sum() {
            accumulateNextToSum();

            int sum = 0;
            for (int i = 0; i < vectorSum.getValueCount(); i++) {
                sum += vectorSum.get(i);
            }
            return sum;
        }

        @Override
        public void add(double num) {
            vectorNext.set(cur++, num);

            if (cur == LANE_SIZE) {
                accumulateNextToSum();
            }
        }

        private void accumulateNextToSum() {
            if (cur == 0) {
                return;
            }

            for (int i = cur; i < LANE_SIZE; i++) { // use mask?
                vectorNext.set(i, 0);
            }
            vectorNext.setValueCount(LANE_SIZE);

            VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(Arrays.asList(fieldSum, fieldNext), Arrays.asList(vectorSum, vectorNext));
            VectorUnloader unloader = new VectorUnloader(vectorSchemaRoot);
            ArrowRecordBatch recordBatch = unloader.getRecordBatch();
            try {
                projector.evaluate(recordBatch, List.of(vectorSum));
            } catch (GandivaException e) {
                throw new RuntimeException(e);
            }

            count += cur;
            cur = 0;
            vectorNext.allocateNew(LANE_SIZE); // reset() not work
        }

        @Override
        public void close() throws Exception {
            vectorNext.close();
            vectorSum.close();
            // projector.close();
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
