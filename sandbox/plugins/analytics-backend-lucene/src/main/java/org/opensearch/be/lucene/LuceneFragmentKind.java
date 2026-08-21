/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

/**
 * What a Lucene-driver fragment asks the data node to produce. Written as the first field of the
 * wire payload {@link LuceneFragmentConvertor#convertFragment} emits and read back by
 * {@link LuceneScanInstructionHandler}; both ends live in this module, so the encoding is a plain
 * ordinal rather than a self-describing format.
 *
 * @opensearch.internal
 */
enum LuceneFragmentKind {

    /**
     * Metadata-only count. The payload's column names are aggregate-call names and every one of them
     * receives the same {@code IndexSearcher.count} value. This is the original (and only) shape the
     * Lucene driver supported.
     */
    COUNT,

    /**
     * Row values read out of Lucene doc values. The payload's column names are the fragment's output
     * columns, which for the supported shapes (scan, passthrough project, filter) are mapped field
     * names the data node resolves against its {@code MapperService}.
     */
    VALUE_SCAN;

    static LuceneFragmentKind fromOrdinal(int ordinal) {
        LuceneFragmentKind[] values = values();
        if (ordinal < 0 || ordinal >= values.length) {
            throw new IllegalStateException("Unknown Lucene fragment kind ordinal [" + ordinal + "]");
        }
        return values[ordinal];
    }
}
