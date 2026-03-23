/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.data.value;

import java.util.Objects;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;
import org.opensearch.sql.data.model.AbstractExprValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.querylanguages.opensearch.data.type.OpenSearchGeoPointType;
import org.opensearch.querylanguages.opensearch.data.utils.GeometryUtils;

/**
 * OpenSearch GeoPointValue. Todo, add this to avoid the unknown value type exception, the
 * implementation will be changed.
 */
public class OpenSearchExprGeoPointValue extends AbstractExprValue {

  private final GeoPoint geoPoint;

  public OpenSearchExprGeoPointValue(Double lat, Double lon) {
    this.geoPoint = new GeoPoint(lat, lon);
  }

  @Override
  public Object value() {
    return geoPoint;
  }

  @Override
  public Point valueForCalcite() {
    // Usually put longitude on x, latitude on y for a Geometry point.
    return GeometryUtils.defaultFactory.createPoint(
        new Coordinate(this.geoPoint.lon, this.geoPoint.lat));
  }

  @Override
  public ExprType type() {
    return OpenSearchGeoPointType.of();
  }

  @Override
  public int compare(ExprValue other) {
    return geoPoint
        .toString()
        .compareTo((((OpenSearchExprGeoPointValue) other).geoPoint).toString());
  }

  @Override
  public boolean equal(ExprValue other) {
    return geoPoint.equals(((OpenSearchExprGeoPointValue) other).geoPoint);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(geoPoint);
  }

  public static class GeoPoint {

    public final Double lat;

    public final Double lon;

    public GeoPoint(Double lat, Double lon) {
      this.lat = lat;
      this.lon = lon;
    }

    @Override
    public String toString() {
      return lat + "," + lon;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      GeoPoint geoPoint = (GeoPoint) o;
      return Objects.equals(lat, geoPoint.lat) && Objects.equals(lon, geoPoint.lon);
    }

    @Override
    public int hashCode() {
      return Objects.hash(lat, lon);
    }
  }
}
