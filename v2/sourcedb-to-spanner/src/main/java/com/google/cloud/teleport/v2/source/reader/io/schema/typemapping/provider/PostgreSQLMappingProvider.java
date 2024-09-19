/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider;

import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapping;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.UnifiedMappingProvider;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Provides a set of {@link org.apache.avro.Schema Avro Schemas} that each of the PostgreSQL
 * database's type must map into.
 *
 * @see <a href="https://cloud.google.com/datastream/docs/unified-types#map-psql">Mappings of
 *     unified types to source and destination data types</a>
 */
public final class PostgreSQLMappingProvider {
  // TODO(thiagotnunes): Add missing type mappings
  private static final ImmutableMap<String, UnifiedTypeMapping> MAPPING =
      ImmutableMap.<String, UnifiedMappingProvider.Type>builder()
          .put("BIGINT", UnifiedMappingProvider.Type.LONG)
          // TODO(thiagotnunes): Refine mapping type according to
          // https://cloud.google.com/datastream/docs/unified-types#map-psql (if there is a limit
          // for length we should use varchar instead)
          .put("CHARACTER VARYING", UnifiedMappingProvider.Type.STRING)
          .put("DATE", UnifiedMappingProvider.Type.DATE)
          .put("INT8", UnifiedMappingProvider.Type.LONG)
          // TODO(thiagotnunes): Refine mapping type according to
          // https://cloud.google.com/datastream/docs/unified-types#map-psql (if there is a limit
          // for length we should use varchar instead)
          .put("TEXT", UnifiedMappingProvider.Type.STRING)
          // TODO(thiagotnunes): Refine mapping type according to
          // https://cloud.google.com/datastream/docs/unified-types#map-psql (if there is a limit
          // for length we should use varchar instead)
          .put("VARCHAR", UnifiedMappingProvider.Type.STRING)
          .build()
          .entrySet()
          .stream()
          .map(e -> Map.entry(e.getKey(), UnifiedMappingProvider.getMapping(e.getValue())))
          .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

  public static ImmutableMap<String, UnifiedTypeMapping> getMapping() {
    return MAPPING;
  }

  private PostgreSQLMappingProvider() {}
}
