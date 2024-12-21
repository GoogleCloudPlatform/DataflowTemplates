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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper;

import com.datastax.driver.core.Row;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.mappings.CassandraMappingsProvider;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.commons.collections4.Transformer;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@AutoValue
abstract class CassandraRowMapper implements Transformer<Row, SourceRow>, Serializable {
  public static final ImmutableMap<String, CassandraFieldMapper<?>> MAPPINGS =
      CassandraMappingsProvider.getFieldMapping();

  public static CassandraRowMapper create(
      SourceSchemaReference sourceSchemaReference, SourceTableSchema sourceTableSchema) {
    return new AutoValue_CassandraRowMapper(sourceSchemaReference, sourceTableSchema);
  }

  abstract SourceSchemaReference sourceSchemaReference();

  abstract SourceTableSchema sourceTableSchema();

  long getCurrentTimeMicros() {
    Instant now = Instant.now();
    long nanos = TimeUnit.SECONDS.toNanos(now.getEpochSecond()) + now.getNano();
    return TimeUnit.NANOSECONDS.toMicros(nanos);
  }

  public @UnknownKeyFor @NonNull @Initialized SourceRow map(
      @UnknownKeyFor @NonNull @Initialized Row row) {
    /* Todo Decide if any of the element time like max time or min time is needed here. */
    long time = getCurrentTimeMicros();

    SourceRow.Builder sourceRowBuilder =
        SourceRow.builder(sourceSchemaReference(), sourceTableSchema(), "", time);

    sourceTableSchema()
        .sourceColumnNameToSourceColumnType()
        .forEach(
            (key, value) -> {
              Schema schema = sourceTableSchema().getAvroPayload().getField(key).schema();
              // The Unified avro mapping produces a union of the mapped type with null type
              // except for "Unsupported" case.
              if (schema.isUnion()) {
                schema = schema.getTypes().get(1);
              }
              sourceRowBuilder.setField(
                  key,
                  MAPPINGS
                      .getOrDefault(value.getName().toUpperCase(), MAPPINGS.get("UNSUPPORTED"))
                      .mapValue(row, key, schema));
            });
    return sourceRowBuilder.build();
  }

  @Override
  public SourceRow transform(Row row) {
    return map(row);
  }
}
