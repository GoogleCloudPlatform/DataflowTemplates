/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.source.dynamodb;

import com.google.cloud.teleport.v2.reader.io.schema.typemapping.UnifiedTypeMapping;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.provider.unified.UnifiedMappingProvider;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 * Provides a set of predefined mappings from Amazon DynamoDB data types to {@link
 * UnifiedMappingProvider.Type} based on DynamoDB's public documentation.
 *
 * <p>See:
 * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
 *
 * <ul>
 *   <li>Scalar Types: {@code S} (String), {@code N} (Number), {@code B} (Binary), {@code BOOL}
 *       (Boolean), {@code NULL} (Null)
 *   <li>Document Types: {@code M} (Map), {@code L} (List)
 *   <li>Set Types: {@code SS} (String Set), {@code NS} (Number Set), {@code BS} (Binary Set)
 * </ul>
 */
public final class DynamoDbMappingsProvider {

  private static final ImmutableMap<String, UnifiedMappingProvider.Type>
      DYNAMODB_TO_UNIFIED_SCALAR_TYPE =
          ImmutableMap.<String, UnifiedMappingProvider.Type>builder()
              // Scalar types (short descriptors & full names)
              .put("S", UnifiedMappingProvider.Type.STRING)
              .put("STRING", UnifiedMappingProvider.Type.STRING)
              .put("N", UnifiedMappingProvider.Type.NUMBER)
              .put("NUMBER", UnifiedMappingProvider.Type.NUMBER)
              .put("B", UnifiedMappingProvider.Type.BYTES)
              .put("BINARY", UnifiedMappingProvider.Type.BYTES)
              .put("BOOL", UnifiedMappingProvider.Type.BOOLEAN)
              .put("BOOLEAN", UnifiedMappingProvider.Type.BOOLEAN)
              .put("NULL", UnifiedMappingProvider.Type.STRING)
              // Document types (serialized to JSON)
              .put("M", UnifiedMappingProvider.Type.JSON)
              .put("MAP", UnifiedMappingProvider.Type.JSON)
              .put("L", UnifiedMappingProvider.Type.JSON)
              .put("LIST", UnifiedMappingProvider.Type.JSON)
              // Fallback
              .put("UNSUPPORTED", UnifiedMappingProvider.Type.UNSUPPORTED)
              .build();

  private static final ImmutableMap<String, UnifiedMappingProvider.Type>
      DYNAMODB_SET_TO_UNIFIED_ELEMENT_TYPE =
          ImmutableMap.<String, UnifiedMappingProvider.Type>builder()
              .put("SS", UnifiedMappingProvider.Type.STRING)
              .put("STRING_SET", UnifiedMappingProvider.Type.STRING)
              .put("NS", UnifiedMappingProvider.Type.NUMBER)
              .put("NUMBER_SET", UnifiedMappingProvider.Type.NUMBER)
              .put("BS", UnifiedMappingProvider.Type.BYTES)
              .put("BINARY_SET", UnifiedMappingProvider.Type.BYTES)
              .build();

  private static final ImmutableMap<String, UnifiedTypeMapping> TYPE_MAPPING = buildTypeMapping();

  private static ImmutableMap<String, UnifiedTypeMapping> buildTypeMapping() {
    ImmutableMap.Builder<String, UnifiedTypeMapping> builder = ImmutableMap.builder();
    for (Map.Entry<String, UnifiedMappingProvider.Type> entry :
        DYNAMODB_TO_UNIFIED_SCALAR_TYPE.entrySet()) {
      builder.put(entry.getKey(), UnifiedMappingProvider.getMapping(entry.getValue()));
    }
    for (Map.Entry<String, UnifiedMappingProvider.Type> entry :
        DYNAMODB_SET_TO_UNIFIED_ELEMENT_TYPE.entrySet()) {
      builder.put(entry.getKey(), UnifiedMappingProvider.getArrayMapping(entry.getValue()));
    }
    return builder.build();
  }

  private DynamoDbMappingsProvider() {}

  /** Returns the map of DynamoDB source types to {@link UnifiedTypeMapping}. */
  public static ImmutableMap<String, UnifiedTypeMapping> getMapping() {
    return TYPE_MAPPING;
  }
}
