/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.monitor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Randomness;
import org.opensearch.sql.common.setting.Settings;

/** OpenSearch Memory Monitor. */
public class OpenSearchMemoryHealthy {
    private static final Logger log = LogManager.getLogger(OpenSearchMemoryHealthy.class);
    private final RandomFail randomFail;
    private final MemoryUsage memoryUsage;

    /**
     * Creates a new monitor with the given settings.
     * @param settings the settings
     */
    public OpenSearchMemoryHealthy(Settings settings) {
        randomFail = new RandomFail();
        memoryUsage = buildMemoryUsage(settings);
    }

    /**
     * Creates a new monitor for testing.
     * @param randomFail the random fail strategy
     * @param memoryUsage the memory usage provider
     */
    public OpenSearchMemoryHealthy(RandomFail randomFail, MemoryUsage memoryUsage) {
        this.randomFail = randomFail;
        this.memoryUsage = memoryUsage;
    }

    private MemoryUsage buildMemoryUsage(Settings settings) {
        try {
            return isCalciteEnabled(settings) ? GCedMemoryUsage.getInstance() : RuntimeMemoryUsage.getInstance();
        } catch (Throwable e) {
            return RuntimeMemoryUsage.getInstance();
        }
    }

    private boolean isCalciteEnabled(Settings settings) {
        if (settings != null) {
            return settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED);
        } else {
            return false;
        }
    }

    /** Is Memory Healthy. Calculate based on the current heap memory usage.
    * @param limitBytes the memory limit in bytes
    * @return true if healthy
    */
    public boolean isMemoryHealthy(long limitBytes) {
        final long memoryUsage = this.memoryUsage.usage();
        log.debug("Memory usage:{}, limit:{}", memoryUsage, limitBytes);
        if (memoryUsage < limitBytes) {
            return true;
        } else {
            log.warn("Memory usage:{} exceed limit:{}", memoryUsage, limitBytes);
            if (randomFail.shouldFail()) {
                log.warn("Fast failing the current request");
                throw new MemoryUsageExceedFastFailureException();
            } else {
                throw new MemoryUsageExceedException();
            }
        }
    }

    static class RandomFail {
        public boolean shouldFail() {
            return Randomness.get().nextBoolean();
        }
    }

    /** Fast failure when memory exceeds limit. */
    public static class MemoryUsageExceedFastFailureException extends MemoryUsageException {
        /** Creates instance. */
        public MemoryUsageExceedFastFailureException() {}
    }

    /** Exception when memory exceeds limit. */
    public static class MemoryUsageExceedException extends MemoryUsageException {
        /** Creates instance. */
        public MemoryUsageExceedException() {}
    }

    /** Base memory usage exception. */
    public static class MemoryUsageException extends RuntimeException {
        /** Creates instance. */
        public MemoryUsageException() {}
    }
}
