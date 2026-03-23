/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;


/**
 * The type of a geo_point value. See <a
 * href="https://opensearch.org/docs/latest/opensearch/supported-field-types/geo-point/">doc</a>
 */
public class OpenSearchGeoPointType extends OpenSearchDataType {

  private static final OpenSearchGeoPointType instance = new OpenSearchGeoPointType();

  private OpenSearchGeoPointType() {
    super(MappingType.GeoPoint);
    exprCoreType = UNKNOWN;
  }

  public static OpenSearchGeoPointType of() {
    return OpenSearchGeoPointType.instance;
  }

  @Override
  protected OpenSearchDataType cloneEmpty() {
    return instance;
  }
}
