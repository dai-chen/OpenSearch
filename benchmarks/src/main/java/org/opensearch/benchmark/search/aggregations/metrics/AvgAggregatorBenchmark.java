/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.search.aggregations.metrics;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.opensearch.search.aggregations.metrics.VectorizedAvgAggregator;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compare: DoubleVector, IntVector, Off-heap memory ...
 */
@Warmup(iterations = 1)
@Measurement(iterations = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class AvgAggregatorBenchmark {

    @Param(value = { "16"/*, "128", "1024", "8192", "65536", "524288"*/ })
    private int SIZE;

    private int[] A;

    private final VectorizedAvgAggregator.Summary summary = new VectorizedAvgAggregator.ArrowVectorSummary();

    @Setup
    public void init() {
        A = new int[SIZE];
        Random random = new Random();
        for (int i = 0; i < SIZE; i++) {
            A[i] = random.nextInt(100);
        }
    }

    @Benchmark
    public double testArrowSum() {
        for (int num : A) {
            summary.add(num);
        }
        return summary.sum();
    }

}
