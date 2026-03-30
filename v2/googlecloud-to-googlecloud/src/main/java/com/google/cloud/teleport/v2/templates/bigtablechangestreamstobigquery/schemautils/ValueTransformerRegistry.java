/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Registry that maps column family:column qualifier pairs to {@link ValueTransformer} instances.
 *
 * <p>Parses a comma-separated configuration string with format: {@code
 * column_family:column:TRANSFORM_TYPE}
 *
 * <p>Supported transform types:
 *
 * <ul>
 *   <li>{@code BIG_ENDIAN_UINT64_TIMESTAMP_MS} - 8-byte big-endian uint64 Unix milliseconds
 * </ul>
 */
public class ValueTransformerRegistry implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Map<String, Map<String, ValueTransformer>> transformersByFamily;

  private ValueTransformerRegistry(
      Map<String, Map<String, ValueTransformer>> transformersByFamily) {
    this.transformersByFamily = transformersByFamily;
  }

  /**
   * Parses a comma-separated transform configuration string.
   *
   * @param config format: "family:column:TYPE,family2:column2:TYPE2"
   * @return a new registry, or null if config is null or empty
   * @throws IllegalArgumentException if the config format is invalid or a transform type is unknown
   */
  public static ValueTransformerRegistry parse(String config) {
    if (StringUtils.isBlank(config)) {
      return null;
    }

    Map<String, Map<String, ValueTransformer>> transformersByFamily = new HashMap<>();
    for (String entry : config.split(",")) {
      String trimmed = entry.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      String[] parts = trimmed.split(":");
      if (parts.length != 3) {
        throw new IllegalArgumentException(
            "Invalid columnTransforms entry '"
                + trimmed
                + "'. Expected format: column_family:column:TRANSFORM_TYPE");
      }
      String family = parts[0];
      String column = parts[1];
      String type = parts[2];

      ValueTransformer transformer = createTransformer(type);
      transformersByFamily.computeIfAbsent(family, k -> new HashMap<>()).put(column, transformer);
    }
    return new ValueTransformerRegistry(transformersByFamily);
  }

  /**
   * Transforms a cell value if a transformer is registered for the given column.
   *
   * @return the transformed string, or null if no transformer matches or transformation fails
   */
  public String transform(String family, String column, byte[] bytes) {
    Map<String, ValueTransformer> familyMap = transformersByFamily.get(family);
    if (familyMap == null) {
      return null;
    }
    ValueTransformer transformer = familyMap.get(column);
    if (transformer == null) {
      return null;
    }
    return transformer.transform(bytes);
  }

  /** Returns true if a transformer is registered for the given column. */
  public boolean hasTransformer(String family, String column) {
    Map<String, ValueTransformer> familyMap = transformersByFamily.get(family);
    return familyMap != null && familyMap.containsKey(column);
  }

  private static ValueTransformer createTransformer(String type) {
    switch (type) {
      case "BIG_ENDIAN_UINT64_TIMESTAMP_MS":
        return new BigEndianTimestampTransformer();
      default:
        throw new IllegalArgumentException(
            "Unknown transform type '"
                + type
                + "'. Supported types: BIG_ENDIAN_UINT64_TIMESTAMP_MS");
    }
  }
}
