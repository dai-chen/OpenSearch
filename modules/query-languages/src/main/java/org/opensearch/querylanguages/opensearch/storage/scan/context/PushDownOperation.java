/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.storage.scan.context;

/**
 * Represents a push down operation that can be applied to an OpenSearchRequestBuilder.
 */
public class PushDownOperation {
    private final PushDownType type;
    private final Object digest;
    private final AbstractAction<?> action;

    /**
     * Creates a push down operation.
     * @param type the push down type
     * @param digest the digest
     * @param action the action
     */
    public PushDownOperation(PushDownType type, Object digest, AbstractAction<?> action) {
        this.type = type;
        this.digest = digest;
        this.action = action;
    }

    /** Returns the push down type. */
    public PushDownType type() {
        return type;
    }

    public Object digest() {
        return digest;
    }

    public AbstractAction<?> action() {
        return action;
    }

    @Override
    public String toString() {
        return type + "->" + digest;
    }
}
