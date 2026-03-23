/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.storage.scan.context;


public class LimitDigest {
  private final int limit;
  private final int offset;

  public LimitDigest(int limit, int offset) {
    this.limit = limit;
    this.offset = offset;
  }

  public int limit() {
    return limit;
  }

  public int offset() {
    return offset;
  }

  @Override
  public String toString() {
    return offset == 0 ? String.valueOf(limit) : "[" + limit + " from " + offset + "]";
  }
}
