/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

/**
 * The type of a binary value. See <a
 * href="https://opensearch.org/docs/latest/opensearch/supported-field-types/binary/">doc</a>
 */
public class OpenSearchBinaryType extends OpenSearchDataType {

    private static final OpenSearchBinaryType instance = new OpenSearchBinaryType();

    private OpenSearchBinaryType() {
        super(MappingType.Binary);
        exprCoreType = UNKNOWN;
    }

    public static OpenSearchBinaryType of() {
        return OpenSearchBinaryType.instance;
    }

    @Override
    protected OpenSearchDataType cloneEmpty() {
        return instance;
    }
}
