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
import org.opensearch.index.engine.exec.IndexReaderProvider;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves the shard's {@link LuceneReader} out of an {@link IndexReaderProvider.Reader}, covering
 * both shapes the framework hands us:
 *
 * <ul>
 *   <li><b>Composite engine</b> — {@code DataFormatAwareEngine} populates the format→reader map from
 *       {@link LuceneReaderManager}, so the value already <em>is</em> a {@link LuceneReader} carrying
 *       the {@code writer_generation → segment name} map the parquet peer needs.</li>
 *   <li><b>Plain (non-composite) shard</b> — {@code EngineBackedIndexer.EngineBackedReader} has no
 *       per-format reader manager and hands out the bare Lucene {@link DirectoryReader}. There are no
 *       writer generations to map (nothing else shares the shard), so the wrapper gets an empty map.</li>
 * </ul>
 *
 * <p>Wrappers are cached per underlying reader rather than rebuilt per call, because
 * {@link LuceneReader} owns the single {@link org.apache.lucene.search.IndexSearcher} every consumer
 * on the shard must share — building a second searcher over the same reader trips Lucene's
 * "top-reader used to create Weight is not the same as the current reader's top-reader" assertion
 * (see {@link LuceneReader#searcher}). The cache is keyed on the reader's own cache key and evicted
 * by its close listener, so entries live exactly as long as the reader does.
 *
 * @opensearch.internal
 */
final class LuceneReaderResolver {

    private static final Map<IndexReader.CacheKey, LuceneReader> PLAIN_READERS = new ConcurrentHashMap<>();

    private LuceneReaderResolver() {}

    /**
     * Returns the {@link LuceneReader} for {@code format}, or {@code null} when the reader carries
     * nothing for that format.
     *
     * @throws IllegalStateException if the format resolves to something that is neither a
     *                               {@link LuceneReader} nor a {@link DirectoryReader}
     */
    static LuceneReader resolve(IndexReaderProvider.Reader reader, DataFormat format) {
        Object raw = reader.reader(format);
        if (raw == null) {
            return null;
        }
        if (raw instanceof LuceneReader luceneReader) {
            return luceneReader;
        }
        if (raw instanceof DirectoryReader directoryReader) {
            return wrapPlain(directoryReader);
        }
        throw new IllegalStateException(
            "Reader for format [" + format.name() + "] is " + raw.getClass().getName() + ", expected LuceneReader or DirectoryReader"
        );
    }

    /** True when this reader came from a plain shard (no composite peer formats). */
    static boolean isPlainShardReader(IndexReaderProvider.Reader reader, DataFormat format) {
        return reader.reader(format) instanceof DirectoryReader;
    }

    private static LuceneReader wrapPlain(DirectoryReader directoryReader) {
        IndexReader.CacheHelper cacheHelper = directoryReader.getReaderCacheHelper();
        if (cacheHelper == null) {
            // No stable identity to cache against (e.g. a filtered reader) — the caller gets a
            // private wrapper. Safe: a per-call searcher is only a problem when two consumers on
            // the same reader must share Weights, which requires a shared cache key to begin with.
            return new LuceneReader(directoryReader, Map.of());
        }
        IndexReader.CacheKey key = cacheHelper.getKey();
        LuceneReader existing = PLAIN_READERS.get(key);
        if (existing != null) {
            return existing;
        }
        LuceneReader created = new LuceneReader(directoryReader, Map.of());
        LuceneReader raced = PLAIN_READERS.putIfAbsent(key, created);
        if (raced != null) {
            return raced;
        }
        cacheHelper.addClosedListener(PLAIN_READERS::remove);
        return created;
    }
}
