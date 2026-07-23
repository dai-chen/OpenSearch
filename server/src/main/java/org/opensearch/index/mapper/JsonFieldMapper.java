/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * PoC "json" field type — Design C (ClickHouse JSON / Druid style).
 *
 * <p>A single mapping/cluster-state entry (like flat_object) accepts an arbitrary JSON object, but
 * unlike flat_object each leaf path is written into its OWN dedicated Lucene column
 * ({@code body.status}, {@code body.method}, ...) rather than a shared path=value blob. This makes
 * {@code stats count() by body.status} a NATIVE keyword terms aggregation on a dedicated column,
 * with no cluster-state subfield entries (subfield types are synthesized on demand via
 * {@link #keyedFieldType(String)}).
 *
 * <p>Type inference: for a leaf declared in {@code typed_paths} (e.g. {@code {"duration_ms":"long"}})
 * the value is written as numeric points + sorted-numeric doc values, enabling numeric range and
 * {@code avg()}. All other leaves are keyword.
 *
 * <p>{@code max_dynamic_paths}: caps the number of distinct dedicated columns materialized per
 * document (bounds per-segment Lucene FieldInfos; overflow leaves are dropped in this PoC).
 *
 * @opensearch.internal
 */
public final class JsonFieldMapper extends DynamicKeyFieldMapper {

    public static final String CONTENT_TYPE = "json";
    static final String DOT = ".";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();
        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }
    }

    private final Map<String, String> typedPaths;
    private final int maxDynamicPaths;

    /**
     * Builder for the json field mapper (non-parameterized style, like flat_object).
     */
    public static class Builder extends FieldMapper.Builder<Builder> {
        private Map<String, String> typedPaths = Collections.emptyMap();
        private int maxDynamicPaths = Integer.MAX_VALUE;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        public Builder typedPaths(Map<String, String> typedPaths) {
            this.typedPaths = typedPaths;
            return this;
        }

        public Builder maxDynamicPaths(int maxDynamicPaths) {
            this.maxDynamicPaths = maxDynamicPaths;
            return this;
        }

        @Override
        public JsonFieldMapper build(BuilderContext context) {
            JsonFieldType fieldType = new JsonFieldType(buildFullName(context));
            return new JsonFieldMapper(name, Defaults.FIELD_TYPE, fieldType, typedPaths, maxDynamicPaths);
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    /**
     * TypeParser that reads the {@code typed_paths} and {@code max_dynamic_paths} parameters.
     */
    public static class TypeParser implements Mapper.TypeParser {
        private final BiFunction<String, ParserContext, Builder> builderFunction;

        public TypeParser(BiFunction<String, ParserContext, Builder> builderFunction) {
            this.builderFunction = builderFunction;
        }

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {
            Builder builder = builderFunction.apply(name, parserContext);
            Object typed = node.remove("typed_paths");
            if (typed instanceof Map) {
                Map<String, String> typedPaths = new HashMap<>();
                for (Map.Entry<?, ?> e : ((Map<?, ?>) typed).entrySet()) {
                    typedPaths.put(e.getKey().toString(), e.getValue().toString());
                }
                builder.typedPaths(typedPaths);
            }
            Object cap = node.remove("max_dynamic_paths");
            if (cap != null) {
                builder.maxDynamicPaths(Integer.parseInt(cap.toString()));
            }
            return builder;
        }
    }

    /**
     * The root json field type. The root is not directly queryable/aggregatable; queries target
     * subfields (e.g. {@code body.status}) which resolve through {@link #keyedFieldType(String)}.
     */
    public static final class JsonFieldType extends StringFieldType {

        public JsonFieldType(String name) {
            super(
                name,
                true,
                false,
                false,
                new TextSearchInfo(Defaults.FIELD_TYPE, null, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER),
                Collections.emptyMap()
            );
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isAggregatable() {
            return false;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new IllegalArgumentException(
                "[json] root field [" + name() + "] cannot be queried directly; query a subfield such as [" + name() + ".<path>]"
            );
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new FieldExistsQuery(name());
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            return new SourceValueFetcher(name(), context, null) {
                @Override
                protected Object parseSourceValue(Object value) {
                    return value;
                }
            };
        }
    }

    JsonFieldMapper(
        String simpleName,
        FieldType fieldType,
        JsonFieldType mappedFieldType,
        Map<String, String> typedPaths,
        int maxDynamicPaths
    ) {
        super(simpleName, fieldType, mappedFieldType, CopyTo.empty());
        this.typedPaths = typedPaths;
        this.maxDynamicPaths = maxDynamicPaths;
    }

    @Override
    public MappedFieldType keyedFieldType(String key) {
        String type = typedPaths.get(key);
        String fullName = name() + DOT + key;
        if ("long".equals(type) || "integer".equals(type)) {
            return new NumberFieldMapper.NumberFieldType(fullName, NumberFieldMapper.NumberType.LONG);
        }
        if ("double".equals(type) || "float".equals(type)) {
            return new NumberFieldMapper.NumberFieldType(fullName, NumberFieldMapper.NumberType.DOUBLE);
        }
        return new KeywordFieldType(fullName, true, true, Collections.emptyMap());
    }

    @Override
    protected JsonFieldMapper clone() {
        return (JsonFieldMapper) super.clone();
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        // PoC: no mergeable options.
    }

    @Override
    public JsonFieldType fieldType() {
        return (JsonFieldType) super.fieldType();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("type", contentType());
        if (typedPaths.isEmpty() == false) {
            builder.field("typed_paths", typedPaths);
        }
        if (maxDynamicPaths != Integer.MAX_VALUE) {
            builder.field("max_dynamic_paths", maxDynamicPaths);
        }
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        XContentParser parser = context.parser();
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return;
        }
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "[" + name() + "] expects a JSON object value");
        }
        Deque<String> path = new LinkedList<>(Collections.singleton(fieldType().name()));
        HashSet<String> writtenPaths = new HashSet<>();
        parser.nextToken();
        while (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            parseToken(parser, context, path, writtenPaths);
        }
    }

    private void parseToken(XContentParser parser, ParseContext context, Deque<String> path, HashSet<String> writtenPaths)
        throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.FIELD_NAME) {
            path.addLast(parser.currentName());
            parser.nextToken();
            parseToken(parser, context, path, writtenPaths);
            path.removeLast();
        } else if (token == XContentParser.Token.START_ARRAY) {
            parser.nextToken();
            while (parser.currentToken() != XContentParser.Token.END_ARRAY) {
                parseToken(parser, context, path, writtenPaths);
            }
            parser.nextToken();
        } else if (token == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
            while (parser.currentToken() != XContentParser.Token.END_OBJECT) {
                parseToken(parser, context, path, writtenPaths);
            }
            parser.nextToken();
        } else {
            String value = parser.textOrNull();
            if (value != null) {
                String leafPath = Strings.collectionToDelimitedString(path, DOT);
                writeLeaf(context, leafPath, value, writtenPaths);
            }
            parser.nextToken();
        }
    }

    private void writeLeaf(ParseContext context, String leafPath, String value, HashSet<String> writtenPaths) {
        // max_dynamic_paths cap: bound the number of distinct dedicated columns materialized per document.
        if (writtenPaths.contains(leafPath) == false && writtenPaths.size() >= maxDynamicPaths) {
            return;
        }
        writtenPaths.add(leafPath);

        // subKey = path under the root, e.g. "duration_ms" or "a.b"
        String subKey = leafPath.substring(name().length() + 1);
        String type = typedPaths.get(subKey);

        if (("long".equals(type) || "integer".equals(type))) {
            try {
                long v = Long.parseLong(value.trim());
                context.doc().add(new LongPoint(leafPath, v));
                context.doc().add(new SortedNumericDocValuesField(leafPath, v));
                return;
            } catch (NumberFormatException e) {
                // fall through to keyword
            }
        } else if ("double".equals(type) || "float".equals(type)) {
            try {
                double d = Double.parseDouble(value.trim());
                context.doc().add(new DoublePoint(leafPath, d));
                context.doc().add(new SortedNumericDocValuesField(leafPath, NumericUtils.doubleToSortableLong(d)));
                return;
            } catch (NumberFormatException e) {
                // fall through to keyword
            }
        }

        // default: dedicated keyword column (postings for filter + doc values for aggregation)
        context.doc().add(new Field(leafPath, new BytesRef(value), Defaults.FIELD_TYPE));
        context.doc().add(new SortedSetDocValuesField(leafPath, new BytesRef(value)));
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
