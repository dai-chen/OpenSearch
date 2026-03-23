/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.monitor;

import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.monitor.ResourceMonitor;

/**
 * {@link ResourceMonitor} implementation on OpenSearch. When the heap memory usage exceeds certain
 * threshold, the monitor is not healthy. Todo, add metrics.
 */
public class OpenSearchResourceMonitor extends ResourceMonitor {
  private final Settings settings;
  private final OpenSearchMemoryHealthy memoryMonitor;

  /** Constructor of OpenSearchResourceMonitor. */
  public OpenSearchResourceMonitor(Settings settings, OpenSearchMemoryHealthy memoryMonitor) {
    this.settings = settings;
    this.memoryMonitor = memoryMonitor;
  }

  /**
   * Is Healthy.
   *
   * @return true if healthy, otherwise return false.
   */
  @Override
  public boolean isHealthy() {
    try {
      ByteSizeValue limit = settings.getSettingValue(Settings.Key.QUERY_MEMORY_LIMIT);
      if (limit == null) {
        // undefined, be always healthy, this is useful in Calcite standalone ITs
        // since AlwaysHealthyMonitor is not work within Calcite tests.
        return true;
      }
      return memoryMonitor.isMemoryHealthy(limit.getBytes());
    } catch (Exception e) {
      return false;
    }
  }
}
