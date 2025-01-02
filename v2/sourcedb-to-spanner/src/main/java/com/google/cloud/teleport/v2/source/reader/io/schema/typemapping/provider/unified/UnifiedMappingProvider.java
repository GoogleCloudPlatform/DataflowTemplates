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
package com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified;

import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapping;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomLogical.Json;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomLogical.Number;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomLogical.TimeIntervalMicros;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.DateTime;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.Interval;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.IntervalNano;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/**
 * Provides a set of {@link Schema Avro Schemas} that each of the source database's type must map
 * into.
 *
 * @see <a href = https://cloud.google.com/datastream/docs/unified-types> Mappings of unified types
 *     to source and destination data types</a>
 *     <p><b>Note:</b>
 *     <p>Most of the type mappings are simple, as in they do not depend on additional parameters
 *     like length, precision, scale etc. For such mappings, we just wrap the Schema in the {@link
 *     SimpleUnifiedTypeMapping} imlementation. Types that need to consider length, precision etc
 *     like {@link Varchar} or {@link Decimal} implement the {@link UnifiedTypeMapping} interface
 *     separately.
 */
public final class UnifiedMappingProvider {
  public enum Type {
    BOOLEAN,
    BYTES,
    DOUBLE,
    DATE,
    DATETIME,
    DECIMAL,
    FLOAT,
    INTEGER,
    INTERVAL,
    JSON,
    LONG,
    NUMBER,
    STRING,
    TIME,
    TIME_INTERVAL,
    TIMESTAMP,
    TIMESTAMP_WITH_TIME_ZONE,
    TIME_WITH_TIME_ZONE,
    VARCHAR,
    UNSUPPORTED,
    INTERVAL_NANO,
  }

  // Implementation Detail, ImmutableMap.of(...) supports only upto 10 arguments.
  private static final ImmutableMap<Type, UnifiedTypeMapping> MAPPING =
      ImmutableMap.<Type, UnifiedTypeMapping>builder()
          .putAll(
              ImmutableMap.<Type, Schema>builder()
                  .put(Type.BOOLEAN, SchemaBuilder.builder().booleanType())
                  .put(Type.BYTES, SchemaBuilder.builder().bytesType())
                  .put(
                      Type.DATE, LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType()))
                  .put(Type.DATETIME, DateTime.SCHEMA)
                  // Decimal is queued below.
                  .put(Type.DOUBLE, SchemaBuilder.builder().doubleType())
                  .put(Type.FLOAT, SchemaBuilder.builder().floatType())
                  .put(Type.INTEGER, SchemaBuilder.builder().intType())
                  .put(Type.INTERVAL, Interval.SCHEMA)
                  .put(Type.JSON, Json.SCHEMA)
                  .put(Type.LONG, SchemaBuilder.builder().longType())
                  .put(Type.NUMBER, Number.SCHEMA)
                  .put(Type.STRING, SchemaBuilder.builder().stringType())
                  .put(
                      Type.TIME,
                      LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder().longType()))
                  .put(Type.TIME_INTERVAL, TimeIntervalMicros.SCHEMA)
                  .put(
                      Type.TIMESTAMP,
                      LogicalTypes.timestampMicros()
                          .addToSchema(SchemaBuilder.builder().longType()))
                  .put(Type.TIMESTAMP_WITH_TIME_ZONE, CustomSchema.TimeStampTz.SCHEMA)
                  .put(Type.TIME_WITH_TIME_ZONE, CustomSchema.TimeTz.SCHEMA)
                  .put(Type.INTERVAL_NANO, IntervalNano.SCHEMA)
                  .build()
                  .entrySet()
                  .stream()
                  .map(e -> Map.entry(e.getKey(), SimpleUnifiedTypeMapping.create(e.getValue())))
                  .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)))
          .put(Type.DECIMAL, new Decimal())
          .put(Type.VARCHAR, new Varchar())
          .put(Type.UNSUPPORTED, new Unsupported())
          .build();

  /**
   * Returns the {@link UnifiedTypeMapping} for a unified type mapping.
   *
   * @param type reference to the unified type to which an avro schema mapping is requested.
   * @return mapping implementation. Default is {@link Unsupported} for unrecognized type.
   */
  public static UnifiedTypeMapping getMapping(Type type) {
    return MAPPING.getOrDefault(type, new Unsupported());
  }

  /** Static final class. * */
  private UnifiedMappingProvider() {}
}
