/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Phase-1 evidence for the analytics-engine plain-index scan: an ordinary (non-composite) shard
 * must be able to hand out an {@link IndexReaderProvider.Reader} through
 * {@link IndexShard#getReaderProvider()}. Before {@link EngineBackedIndexer#acquireReader()} was
 * implemented this threw {@code UnsupportedOperationException}, which blocked
 * {@code AnalyticsSearchService.startFragment} on every plain shard.
 */
public class EngineBackedIndexerReaderTests extends IndexShardTestCase {

    /**
     * {@link DataFormat} is equal by name, so this stands in for the descriptor a value-producing
     * back-end looks the reader up with. A plain shard publishes under
     * {@link org.opensearch.index.engine.dataformat.DataFormatNames#LUCENE_DOC_VALUES}, not
     * {@code lucene}: the distinct id is what lets a consumer tell real doc values from a composite
     * index's postings-only Lucene secondary by format id alone.
     */
    private static final DataFormat LUCENE_DOC_VALUES = new DataFormat() {
        @Override
        public String name() {
            return "lucene_doc_values";
        }

        @Override
        public long priority() {
            return 50L;
        }

        @Override
        public Set<FieldTypeCapabilities> supportedFields() {
            return Set.of();
        }
    };

    /** The inverted-index id. A plain shard must NOT answer under this one — see the field above. */
    private static final DataFormat LUCENE = new DataFormat() {
        @Override
        public String name() {
            return "lucene";
        }

        @Override
        public long priority() {
            return 50L;
        }

        @Override
        public Set<FieldTypeCapabilities> supportedFields() {
            return Set.of();
        }
    };

    public void testPlainShardHandsOutDirectoryReaderWithAllDocs() throws Exception {
        IndexShard shard = newStartedShard(true);
        try {
            indexDoc(shard, "_doc", "1", "{\"name\":\"alpha\",\"id\":1}");
            indexDoc(shard, "_doc", "2", "{\"name\":\"beta\",\"id\":2}");
            indexDoc(shard, "_doc", "3", "{\"name\":\"gamma\",\"id\":3}");
            shard.refresh("test");

            IndexReaderProvider readerProvider = shard.getReaderProvider();
            assertNotNull("plain shard must expose a ReaderProvider", readerProvider);
            assertThat(readerProvider, instanceOf(EngineBackedIndexer.class));

            try (GatedCloseable<IndexReaderProvider.Reader> gated = readerProvider.acquireReader()) {
                IndexReaderProvider.Reader reader = gated.get();
                assertNotNull("acquireReader must not return a null reader", reader);

                // The whole point of Phase 1: a value-producing backend can reach real Lucene data.
                Object raw = reader.reader(LUCENE_DOC_VALUES);
                assertThat("lucene_doc_values format must resolve to a DirectoryReader", raw, instanceOf(DirectoryReader.class));
                DirectoryReader directoryReader = (DirectoryReader) raw;
                assertEquals("all three indexed docs must be visible", 3, directoryReader.numDocs());

                // Typed accessor agrees with the untyped one.
                assertSame(raw, reader.getReader(LUCENE_DOC_VALUES, DirectoryReader.class));

                // The two Lucene ids are distinct: a plain shard answers only for doc values, so a
                // consumer can pick its adapter by format id without inspecting the value's type.
                assertNull("a plain shard must not answer under the inverted-index id", reader.reader(LUCENE));

                // A real CatalogSnapshot comes for free via Engine#acquireSnapshot (rung A1) —
                // no need to synthesize one over Lucene segments (rung A2).
                CatalogSnapshot snapshot = reader.catalogSnapshot();
                assertNotNull("plain shard must expose a CatalogSnapshot", snapshot);

                // Unknown formats resolve to nothing rather than blowing up.
                assertNull(reader.reader(new UnknownFormat()));
                assertNull(reader.getReader(new UnknownFormat(), DirectoryReader.class));
            }
        } finally {
            closeShards(shard);
        }
    }

    /** Acquiring twice must yield independent, separately-closeable readers. */
    public void testAcquireReaderIsRepeatable() throws Exception {
        IndexShard shard = newStartedShard(true);
        try {
            indexDoc(shard, "_doc", "1", "{\"name\":\"alpha\"}");
            shard.refresh("test");

            IndexReaderProvider readerProvider = shard.getReaderProvider();
            try (
                GatedCloseable<IndexReaderProvider.Reader> first = readerProvider.acquireReader();
                GatedCloseable<IndexReaderProvider.Reader> second = readerProvider.acquireReader()
            ) {
                assertNotSame(first.get(), second.get());
                assertEquals(1, ((DirectoryReader) first.get().reader(LUCENE_DOC_VALUES)).numDocs());
                assertEquals(1, ((DirectoryReader) second.get().reader(LUCENE_DOC_VALUES)).numDocs());
            }

            // Still usable after both were released — the shard's searcher refcount is balanced.
            try (GatedCloseable<IndexReaderProvider.Reader> third = readerProvider.acquireReader()) {
                assertEquals(1, ((DirectoryReader) third.get().reader(LUCENE_DOC_VALUES)).numDocs());
            }
        } finally {
            closeShards(shard);
        }
    }

    private static final class UnknownFormat extends DataFormat {
        @Override
        public String name() {
            return "not-a-real-format";
        }

        @Override
        public long priority() {
            return 1L;
        }

        @Override
        public Set<FieldTypeCapabilities> supportedFields() {
            return Set.of();
        }
    }
}
