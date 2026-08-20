/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Well-known {@link DataFormat#name()} values shared between the server and the analytics
 * back-end plugins.
 *
 * <p>{@link DataFormat} equality is defined solely on the name (see {@link DataFormat#equals}),
 * so a format name is the identity under which a reader is published and looked up. These
 * constants live here — rather than in a plugin or in the analytics framework — because the
 * server publishes readers under them and cannot depend on either: {@code
 * sandbox/libs/analytics-framework/build.gradle} declares {@code compileOnly project(':server')},
 * so the dependency runs plugin → server and never the other way.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class DataFormatNames {

    /**
     * Lucene as an <em>inverted index</em>. On a composite index with a non-Lucene primary this is
     * the secondary segment: postings only, no doc values and no indexed numerics.
     */
    public static final String LUCENE = "lucene";

    /**
     * Lucene as a source of <em>row values</em> — the shard's own doc values, on an index that never
     * opted into a pluggable data format. Deliberately distinct from {@link #LUCENE}: it is the id a
     * back-end registers value-producing scan and numeric-filter capabilities against, and the id a
     * plain shard publishes its reader under, so neither resolves for a composite secondary.
     */
    public static final String LUCENE_DOC_VALUES = "lucene_doc_values";

    private DataFormatNames() {}
}
