/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.monitor;

/**
 * GC-aware memory usage monitor.
 *
 * <p>TODO: Phase 3 — re-implement GC notification listener without com.sun.management
 * for more accurate post-GC memory tracking. For now, delegates to RuntimeMemoryUsage.
 */
public class GCedMemoryUsage implements MemoryUsage {

    private GCedMemoryUsage() {}

    private static class Holder {
        static final MemoryUsage INSTANCE = RuntimeMemoryUsage.getInstance();
    }

    /** Returns the singleton instance. */
    public static MemoryUsage getInstance() {
        return Holder.INSTANCE;
    }

    @Override
    public long usage() {
        return Holder.INSTANCE.usage();
    }

    @Override
    public void setUsage(long value) {
        Holder.INSTANCE.setUsage(value);
    }
}
