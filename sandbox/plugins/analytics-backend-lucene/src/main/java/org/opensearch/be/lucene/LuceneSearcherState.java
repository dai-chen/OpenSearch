/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.opensearch.analytics.spi.BackendExecutionContext;

import java.util.List;
import java.util.Objects;

/**
 * Lucene-side {@link BackendExecutionContext}. Built by {@link LuceneScanInstructionHandler}
 * from the wire bytes {@link LuceneFragmentConvertor} produced (filter {@code QueryBuilder} +
 * aggregate-call column names) and consumed by {@link LuceneSearchExecEngine}.
 *
 * <p>Mirrors the role {@code DataFusionSessionState} plays for the DataFusion backend —
 * a small immutable state record threaded from instruction handler to search engine.
 *
 * <p>Holds no native resources; {@link #close()} is a no-op. The {@link IndexSearcher}'s
 * underlying reader is owned by the caller-acquired {@code ReaderContext}, which closes it
 * after the engine stream drains.
 *
 * @opensearch.internal
 */
final class LuceneSearcherState implements BackendExecutionContext {

    private final IndexSearcher searcher;
    /** Never {@code null}; {@code MatchAllDocsQuery} when the fragment had no filter. */
    private final Query filterQuery;
    /**
     * For {@link LuceneFragmentKind#COUNT}, aggregate-call output names — one Int64 column per name.
     * For {@link LuceneFragmentKind#VALUE_SCAN}, the mapped field names to read doc values for, in
     * output row order.
     */
    private final List<String> outputColumnNames;
    /** What the data node must produce for this fragment. */
    private final LuceneFragmentKind kind;

    LuceneSearcherState(IndexSearcher searcher, Query filterQuery, List<String> outputColumnNames, LuceneFragmentKind kind) {
        this.searcher = Objects.requireNonNull(searcher, "searcher");
        // Never null — see field javadoc. Caller must substitute MatchAllDocsQuery when the
        // fragment has no filter so the search engine doesn't have to branch.
        this.filterQuery = Objects.requireNonNull(filterQuery, "filterQuery (use MatchAllDocsQuery for no-filter fragments)");
        this.outputColumnNames = List.copyOf(Objects.requireNonNull(outputColumnNames, "outputColumnNames"));
        this.kind = Objects.requireNonNull(kind, "kind");
    }

    IndexSearcher searcher() {
        return searcher;
    }

    Query filterQuery() {
        return filterQuery;
    }

    List<String> outputColumnNames() {
        return outputColumnNames;
    }

    LuceneFragmentKind kind() {
        return kind;
    }
}
