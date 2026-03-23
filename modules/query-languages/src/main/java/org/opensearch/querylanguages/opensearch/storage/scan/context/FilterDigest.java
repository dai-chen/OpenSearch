/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.storage.scan.context;

import org.apache.calcite.rex.RexNode;

public class FilterDigest {
    private final int scriptCount;
    private final RexNode condition;

    public FilterDigest(int scriptCount, RexNode condition) {
        this.scriptCount = scriptCount;
        this.condition = condition;
    }

    public int scriptCount() {
        return scriptCount;
    }

    public RexNode condition() {
        return condition;
    }

    @Override
    public String toString() {
        return condition.toString();
    }
}
