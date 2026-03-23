/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.storage.scan.context;

import org.apache.calcite.rel.RelFieldCollation;

import java.util.List;

import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;

public class RareTopDigest {
    private final String target;
    private final List<String> byList;
    private final Integer number;
    private final RelFieldCollation.Direction direction;

    public RareTopDigest(String target, List<String> byList, Integer number, RelFieldCollation.Direction direction) {
        this.target = target;
        this.byList = byList;
        this.number = number;
        this.direction = direction;
    }

    public String target() {
        return target;
    }

    public List<String> byList() {
        return byList;
    }

    public Integer number() {
        return number;
    }

    public RelFieldCollation.Direction direction() {
        return direction;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(direction == ASCENDING ? "rare" : "top");
        builder.append(" ");
        builder.append(number);
        builder.append(" ");
        builder.append(target);
        if (!byList.isEmpty()) {
            builder.append(" by ");
            builder.append(String.join(",", byList));
        }
        return builder.toString();
    }
}
