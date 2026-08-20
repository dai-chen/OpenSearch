/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;

/**
 * Reads one column's values out of Lucene doc values and appends them to an Arrow vector.
 *
 * <p>Scope is deliberately the two types the plain-index scan PoC covers — {@code keyword} and
 * {@code long} — mirroring {@code LuceneAnalyticsBackendPlugin}'s {@code DOC_VALUE_TYPES}. Anything
 * else is rejected at planning time by {@code LuceneFragmentConvertor.isValueScanFastPath}, so
 * reaching {@link #forField} with an unsupported type is a planner bug and fails loudly.
 *
 * <p>Multi-valued fields take the first ordinal / first value: doc values are sorted, so this is
 * deterministic, but it is a simplification — the analytics row model has no array column type for
 * these formats yet.
 *
 * @opensearch.internal
 */
abstract class LuceneDocValuesReader {

    private final String fieldName;

    private LuceneDocValuesReader(String fieldName) {
        this.fieldName = fieldName;
    }

    String fieldName() {
        return fieldName;
    }

    /** The Arrow field this reader writes into. Always nullable — a doc may have no value. */
    abstract Field arrowField();

    /** Binds this reader to {@code leaf}; returns a per-leaf cursor. */
    abstract LeafCursor open(LeafReader leaf) throws IOException;

    /** Per-leaf value cursor. {@link #append} must be called with ascending {@code docId}s. */
    interface LeafCursor {
        /** Appends {@code docId}'s value (or null when absent) at {@code rowIndex} of {@code vector}. */
        void append(int docId, int rowIndex, FieldVector vector) throws IOException;
    }

    /** Whether a planner-side field type is readable by this class. Keep in sync with {@link #forField}. */
    static boolean isSupported(org.opensearch.analytics.spi.FieldType fieldType) {
        return fieldType == org.opensearch.analytics.spi.FieldType.KEYWORD || fieldType == org.opensearch.analytics.spi.FieldType.LONG;
    }

    /**
     * Builds the reader for {@code columnName}, resolving its mapping through {@code mapperService}.
     *
     * @throws IllegalStateException when the column is not a mapped field, or its type is outside the
     *                               supported set
     */
    static LuceneDocValuesReader forField(String columnName, MapperService mapperService) {
        if (mapperService == null) {
            throw new IllegalStateException("Lucene value scan requires a MapperService to resolve column [" + columnName + "]");
        }
        MappedFieldType fieldType = mapperService.fieldType(columnName);
        if (fieldType == null) {
            throw new IllegalStateException(
                "Lucene value scan cannot resolve column [" + columnName + "] to a mapped field on this shard"
            );
        }
        if (fieldType.hasDocValues() == false) {
            throw new IllegalStateException("Lucene value scan requires doc values on column [" + columnName + "]");
        }
        String mappingType = fieldType.typeName();
        return switch (mappingType) {
            case "keyword" -> new KeywordReader(columnName);
            case "long" -> new LongReader(columnName);
            default -> throw new IllegalStateException(
                "Lucene value scan does not support column ["
                    + columnName
                    + "] of type ["
                    + mappingType
                    + "]; supported types are keyword and long"
            );
        };
    }

    // ── keyword ──────────────────────────────────────────────────────────────

    private static final class KeywordReader extends LuceneDocValuesReader {

        KeywordReader(String fieldName) {
            super(fieldName);
        }

        @Override
        Field arrowField() {
            return new Field(fieldName(), new FieldType(true, ArrowType.Utf8.INSTANCE, null), null);
        }

        @Override
        LeafCursor open(LeafReader leaf) throws IOException {
            // KeywordFieldMapper writes SortedSetDocValuesField; DocValues.getSortedSet transparently
            // adapts a single-valued SORTED field and yields an empty instance when the segment has
            // no values for the field at all.
            SortedSetDocValues values = DocValues.getSortedSet(leaf, fieldName());
            return (docId, rowIndex, vector) -> {
                VarCharVector out = (VarCharVector) vector;
                if (values.advanceExact(docId) == false) {
                    out.setNull(rowIndex);
                    return;
                }
                long ord = values.nextOrd();
                BytesRef term = values.lookupOrd(ord);
                out.setSafe(rowIndex, term.bytes, term.offset, term.length);
            };
        }
    }

    // ── long ─────────────────────────────────────────────────────────────────

    private static final class LongReader extends LuceneDocValuesReader {

        LongReader(String fieldName) {
            super(fieldName);
        }

        @Override
        Field arrowField() {
            return new Field(fieldName(), new FieldType(true, new ArrowType.Int(64, true), null), null);
        }

        @Override
        LeafCursor open(LeafReader leaf) throws IOException {
            // NumberFieldMapper writes SortedNumericDocValuesField even for single-valued longs.
            // Prefer the sorted-numeric view and fall back to the plain numeric one so a segment
            // written by a different producer still reads.
            SortedNumericDocValues sortedNumeric = leaf.getSortedNumericDocValues(fieldName());
            if (sortedNumeric != null) {
                return (docId, rowIndex, vector) -> {
                    BigIntVector out = (BigIntVector) vector;
                    if (sortedNumeric.advanceExact(docId) == false) {
                        out.setNull(rowIndex);
                        return;
                    }
                    out.setSafe(rowIndex, sortedNumeric.nextValue());
                };
            }
            NumericDocValues numeric = DocValues.getNumeric(leaf, fieldName());
            return (docId, rowIndex, vector) -> {
                BigIntVector out = (BigIntVector) vector;
                if (numeric.advanceExact(docId) == false) {
                    out.setNull(rowIndex);
                    return;
                }
                out.setSafe(rowIndex, numeric.longValue());
            };
        }
    }

    /** Unused today; kept so a future SORTED-only keyword producer has an obvious hook. */
    @SuppressWarnings("unused")
    private static BytesRef firstTerm(SortedDocValues values, int docId) throws IOException {
        return values.advanceExact(docId) ? values.lookupOrd(values.ordValue()) : null;
    }
}
