/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.monitor;

/** Memory usage interface. It is used to get the memory usage of the VM. */
public interface MemoryUsage {
  long usage();

  void setUsage(long usage);
}
