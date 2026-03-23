/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.data.utils;

import org.locationtech.jts.geom.GeometryFactory;

public interface GeometryUtils {
  GeometryFactory defaultFactory = new GeometryFactory();
}
