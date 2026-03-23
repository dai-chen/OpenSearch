/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.monitor;

/** Get memory usage from runtime, which is used in v2. */
public class RuntimeMemoryUsage implements MemoryUsage {
    private RuntimeMemoryUsage() {}

    private static class Holder {
        static final MemoryUsage INSTANCE = new RuntimeMemoryUsage();
    }

    /** Returns the singleton instance. */
    public static MemoryUsage getInstance() {
        return Holder.INSTANCE;
    }

    @Override
    public long usage() {
        final long freeMemory = Runtime.getRuntime().freeMemory();
        final long totalMemory = Runtime.getRuntime().totalMemory();
        return totalMemory - freeMemory;
    }

    @Override
    public void setUsage(long usage) {
        throw new UnsupportedOperationException("Cannot set usage in RuntimeMemoryUsage");
    }
}
