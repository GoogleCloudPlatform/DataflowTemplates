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
          .put("BIGSERIAL", UnifiedMappingProvider.Type.LONG)
          .put("BIT", UnifiedMappingProvider.Type.BYTES)
          .put("BIT VARYING", UnifiedMappingProvider.Type.BYTES)
          .put("BOOL", UnifiedMappingProvider.Type.BOOLEAN)
          .put("BOOLEAN", UnifiedMappingProvider.Type.BOOLEAN)
          .put("BOX", UnifiedMappingProvider.Type.UNSUPPORTED)
          .put("BYTEA", UnifiedMappingProvider.Type.BYTES)
          // TODO(thiagotnunes): Refine mapping type according to
          // https://cloud.google.com/datastream/docs/unified-types#map-psql (if there is a limit
          // for length we should use varchar instead)
          .put("CHAR", UnifiedMappingProvider.Type.STRING)
          // TODO(thiagotnunes): Refine mapping type according to
          // https://cloud.google.com/datastream/docs/unified-types#map-psql (if there is a limit
          // for length we should use varchar instead)
          .put("CHARACTER", UnifiedMappingProvider.Type.STRING)
          // TODO(thiagotnunes): Refine mapping type according to
          // https://cloud.google.com/datastream/docs/unified-types#map-psql (if there is a limit
          // for length we should use varchar instead)
          .put("CHARACTER VARYING", UnifiedMappingProvider.Type.STRING)
          .put("CIDR", UnifiedMappingProvider.Type.STRING)
          .put("CIRCLE", UnifiedMappingProvider.Type.UNSUPPORTED)
          .put("CITEXT", UnifiedMappingProvider.Type.STRING)
          .put("DATE", UnifiedMappingProvider.Type.DATE)
          .put("DECIMAL", UnifiedMappingProvider.Type.NUMBER)
          .put("DOUBLE PRECISION", UnifiedMappingProvider.Type.DOUBLE)
          .put("ENUM", UnifiedMappingProvider.Type.STRING)
          .put("FLOAT4", UnifiedMappingProvider.Type.FLOAT)
          .put("FLOAT8", UnifiedMappingProvider.Type.DOUBLE)
          .put("INET", UnifiedMappingProvider.Type.STRING)
          .put("INT", UnifiedMappingProvider.Type.INTEGER)
          .put("INTEGER", UnifiedMappingProvider.Type.INTEGER)
          // TODO(thiagotnunes): INTERVAL
          .put("INT2", UnifiedMappingProvider.Type.INTEGER)
          .put("INT4", UnifiedMappingProvider.Type.INTEGER)
          .put("INT8", UnifiedMappingProvider.Type.LONG)
          .put("JSON", UnifiedMappingProvider.Type.JSON)
          .put("JSONB", UnifiedMappingProvider.Type.JSON)
          .put("LINE", UnifiedMappingProvider.Type.UNSUPPORTED)
          .put("LSEG", UnifiedMappingProvider.Type.UNSUPPORTED)
          .put("MACADDR", UnifiedMappingProvider.Type.STRING)
          .put("MACADDR8", UnifiedMappingProvider.Type.STRING)
          .put("MONEY", UnifiedMappingProvider.Type.DOUBLE)
          .put("NUMERIC", UnifiedMappingProvider.Type.NUMBER)
          .put("OID", UnifiedMappingProvider.Type.LONG)
          .put("PATH", UnifiedMappingProvider.Type.UNSUPPORTED)
          .put("PG_LSN", UnifiedMappingProvider.Type.UNSUPPORTED)
          .put("PG_SNAPSHOT", UnifiedMappingProvider.Type.UNSUPPORTED)
          .put("POINT", UnifiedMappingProvider.Type.UNSUPPORTED)
          .put("POLYGON", UnifiedMappingProvider.Type.UNSUPPORTED)
          .put("REAL", UnifiedMappingProvider.Type.FLOAT)
          .put("SERIAL", UnifiedMappingProvider.Type.INTEGER)
          .put("SERIAL2", UnifiedMappingProvider.Type.INTEGER)
          .put("SERIAL4", UnifiedMappingProvider.Type.INTEGER)
          .put("SERIAL8", UnifiedMappingProvider.Type.LONG)
          .put("SMALLINT", UnifiedMappingProvider.Type.INTEGER)
          .put("SMALLSERIAL", UnifiedMappingProvider.Type.INTEGER)
          // TODO(thiagotnunes): Refine mapping type according to
          // https://cloud.google.com/datastream/docs/unified-types#map-psql (if there is a limit
          // for length we should use varchar instead)
          .put("TEXT", UnifiedMappingProvider.Type.STRING)
          .put("TIME", UnifiedMappingProvider.Type.TIME)
          .put("TIMETZ", UnifiedMappingProvider.Type.TIME_WITH_TIME_ZONE)
          .put("TIMESTAMP", UnifiedMappingProvider.Type.TIMESTAMP)
          .put("TIMESTAMPTZ", UnifiedMappingProvider.Type.TIMESTAMP_WITH_TIME_ZONE)
          .put("TIME WITH TIME ZONE", UnifiedMappingProvider.Type.TIME_WITH_TIME_ZONE)
          .put("TIME WITHOUT TIME ZONE", UnifiedMappingProvider.Type.TIME)
          .put("TIMESTAMP WITH TIME ZONE", UnifiedMappingProvider.Type.TIMESTAMP_WITH_TIME_ZONE)
          .put("TIMESTAMP WITHOUT TIME ZONE", UnifiedMappingProvider.Type.TIMESTAMP)
          .put("TSQUERY", UnifiedMappingProvider.Type.STRING)
          .put("TSVECTOR", UnifiedMappingProvider.Type.STRING)
          .put("TXID_SNAPSHOT", UnifiedMappingProvider.Type.STRING)
          .put("UUID", UnifiedMappingProvider.Type.STRING)
          .put("VARBIT", UnifiedMappingProvider.Type.BYTES)
          // TODO(thiagotnunes): Refine mapping type according to
          // https://cloud.google.com/datastream/docs/unified-types#map-psql (if there is a limit
          // for length we should use varchar instead)
          .put("VARCHAR", UnifiedMappingProvider.Type.STRING)
          .put("XML", UnifiedMappingProvider.Type.STRING)
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
