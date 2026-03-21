/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Response for SQL/PPL query execution containing column metadata and data rows.
 *
 * @opensearch.internal
 */
public class QueryLanguageResponse extends ActionResponse implements ToXContentObject {

    private final List<String> columns;
    private final List<List<Object>> dataRows;

    /**
     * Creates a new response.
     *
     * @param columns the column names
     * @param dataRows the data rows
     */
    public QueryLanguageResponse(List<String> columns, List<List<Object>> dataRows) {
        this.columns = columns;
        this.dataRows = dataRows;
    }

    /**
     * Creates a new response from a stream.
     *
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public QueryLanguageResponse(StreamInput in) throws IOException {
        super(in);
        this.columns = in.readStringList();
        int rowCount = in.readVInt();
        this.dataRows = new java.util.ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            this.dataRows.add(in.readList(StreamInput::readGenericValue));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(columns);
        out.writeVInt(dataRows.size());
        for (List<Object> row : dataRows) {
            out.writeCollection(row, StreamOutput::writeGenericValue);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("schema");
        for (String col : columns) {
            builder.startObject().field("name", col).endObject();
        }
        builder.endArray();
        builder.startArray("datarows");
        for (List<Object> row : dataRows) {
            builder.startArray();
            for (Object val : row) {
                builder.value(val);
            }
            builder.endArray();
        }
        builder.endArray();
        builder.field("total", dataRows.size());
        builder.field("size", dataRows.size());
        builder.endObject();
        return builder;
    }
}
