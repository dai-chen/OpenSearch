/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querylanguages.opensearch.mapping;

import java.util.Map;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.querylanguages.opensearch.data.type.OpenSearchDataType;

/**
 * OpenSearch index mapping. Because there is no specific behavior for different field types, string
 * is used to represent field types.
 */
public class IndexMapping {

  /** Field mappings from field name to field type in OpenSearch date type system. */
  private final Map<String, OpenSearchDataType> fieldMappings;

  /**
   * Maps each column in the index definition to an OpenSearchSQL datatype.
   *
   * @param metaData The metadata retrieved from the index mapping defined by the user.
   */
  @SuppressWarnings("unchecked")
  public IndexMapping(MappingMetadata metaData) {
    this.fieldMappings =
        OpenSearchDataType.parseMapping(
            (Map<String, Object>) metaData.getSourceAsMap().getOrDefault("properties", null));
  }

  /**
   * How many fields in the index (after flatten).
   *
   * @return field size
   */
  public int size() {
    return fieldMappings.size();
  }

  public Map<String, OpenSearchDataType> getFieldMappings() {
    return fieldMappings;
  }
}
