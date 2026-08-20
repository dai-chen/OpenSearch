/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatNames;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.exec.IndexReaderProvider;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Turns the value an {@link IndexReaderProvider.Reader} publishes for a Lucene data format into a
 * {@link LuceneReader}. One adapter is registered per format id, and the id alone selects it:
 *
 * <ul>
 *   <li>{@link DataFormatNames#LUCENE} — Lucene as an inverted index. {@code DataFormatAwareEngine}
 *       populates the map from {@link LuceneReaderManager}, so the value already <em>is</em> a
 *       {@link LuceneReader} carrying the {@code writer_generation → segment name} map the parquet
 *       peer needs.</li>
 *   <li>{@link DataFormatNames#LUCENE_DOC_VALUES} — Lucene as a source of row values.
 *       {@code EngineBackedIndexer.EngineBackedReader} has no per-format reader manager and publishes
 *       the bare Lucene {@link DirectoryReader}. There are no writer generations to map (nothing else
 *       shares the shard), so the wrapper gets an empty map.</li>
 * </ul>
 *
 * <p>Because the two ids never appear on the same shard, resolving also tells the caller <em>which</em>
 * format answered — see {@link Resolved#hasDocValues()}. That keeps execution using the same format
 * vocabulary the planner used, instead of re-deriving the index's shape from the reader.
 *
 * <p>Doc-values wrappers are memoised per underlying reader rather than rebuilt per call, because
 * {@link LuceneReader} owns the single {@link org.apache.lucene.search.IndexSearcher} every consumer
 * on the shard must share — building a second searcher over the same reader trips Lucene's
 * "top-reader used to create Weight is not the same as the current reader's top-reader" assertion
 * (see {@link LuceneReader#searcher}, and the self-union note in
 * {@link LuceneAnalyticsBackendPlugin#getFilterDelegationHandle}). The memo is an instance field of
 * this adapter — not a JVM-wide static — keyed on the reader's own cache key and evicted by its close
 * listener, so entries live exactly as long as the reader does. The {@link DataFormatNames#LUCENE}
 * path needs no memo: {@link LuceneReaderManager} already hands out one shared instance per snapshot.
 *
 * @opensearch.internal
 */
final class LuceneReaderAdapter {

    /** How a given format's published value becomes a {@link LuceneReader}. */
    @FunctionalInterface
    private interface ValueAdapter {
        LuceneReader adapt(Object rawValue);
    }

    /**
     * A resolved shard reader plus the format id it was published under.
     *
     * @param reader     the shard's {@link LuceneReader}
     * @param formatName the {@link DataFormat#name()} that answered
     */
    record Resolved(LuceneReader reader, String formatName) {

        /**
         * True when the resolving format carries real Lucene doc values (and indexed numerics) — i.e.
         * {@link DataFormatNames#LUCENE_DOC_VALUES}. False for {@link DataFormatNames#LUCENE}, which on
         * a composite index is a postings-only secondary segment.
         */
        boolean hasDocValues() {
            return DataFormatNames.LUCENE_DOC_VALUES.equals(formatName);
        }
    }

    /** Format id → value adapter, in probe order. */
    private final Map<DataFormat, ValueAdapter> adaptersByFormat;

    /** Memoised {@link DataFormatNames#LUCENE_DOC_VALUES} wrappers; see the class javadoc. */
    private final Map<IndexReader.CacheKey, LuceneReader> docValuesReaders = new ConcurrentHashMap<>();

    LuceneReaderAdapter() {
        Map<DataFormat, ValueAdapter> adapters = new LinkedHashMap<>();
        adapters.put(formatKey(DataFormatNames.LUCENE), LuceneReader.class::cast);
        adapters.put(formatKey(DataFormatNames.LUCENE_DOC_VALUES), raw -> sharedDocValuesReader((DirectoryReader) raw));
        this.adaptersByFormat = Map.copyOf(adapters);
    }

    /**
     * Resolves the shard's {@link LuceneReader}, or {@code null} when the reader publishes nothing for
     * any Lucene format.
     *
     * @throws IllegalStateException if a format's value is not the type that format publishes
     */
    Resolved resolve(IndexReaderProvider.Reader reader) {
        for (Map.Entry<DataFormat, ValueAdapter> entry : adaptersByFormat.entrySet()) {
            String formatName = entry.getKey().name();
            Object raw = reader.reader(entry.getKey());
            if (raw == null) {
                continue;
            }
            try {
                return new Resolved(entry.getValue().adapt(raw), formatName);
            } catch (ClassCastException e) {
                throw new IllegalStateException(
                    "Reader for format [" + formatName + "] is " + raw.getClass().getName() + ", which that format does not publish",
                    e
                );
            }
        }
        return null;
    }

    private LuceneReader sharedDocValuesReader(DirectoryReader directoryReader) {
        IndexReader.CacheHelper cacheHelper = directoryReader.getReaderCacheHelper();
        if (cacheHelper == null) {
            // No stable identity to memoise against (e.g. a filtered reader) — the caller gets a
            // private wrapper. Safe: a per-call searcher is only a problem when two consumers on
            // the same reader must share Weights, which requires a shared cache key to begin with.
            return new LuceneReader(directoryReader, Map.of());
        }
        IndexReader.CacheKey key = cacheHelper.getKey();
        LuceneReader existing = docValuesReaders.get(key);
        if (existing != null) {
            return existing;
        }
        LuceneReader created = new LuceneReader(directoryReader, Map.of());
        LuceneReader raced = docValuesReaders.putIfAbsent(key, created);
        if (raced != null) {
            return raced;
        }
        cacheHelper.addClosedListener(docValuesReaders::remove);
        return created;
    }

    /**
     * A name-only {@link DataFormat} lookup key. Sound because {@link DataFormat#equals} is final and
     * compares {@link DataFormat#name()} alone, so this is interchangeable with whatever descriptor
     * published the reader — no compile-time dependency on the publisher.
     */
    private static DataFormat formatKey(String name) {
        return new DataFormat() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public long priority() {
                return 0L;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
                return Set.of();
            }
        };
    }
}
