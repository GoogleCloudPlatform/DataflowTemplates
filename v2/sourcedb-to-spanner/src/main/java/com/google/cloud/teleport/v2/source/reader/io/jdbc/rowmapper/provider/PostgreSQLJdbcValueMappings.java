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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.provider;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.ResultSetValueExtractor;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.rowmapper.ResultSetValueMapper;
import com.google.common.collect.ImmutableMap;
import java.sql.ResultSet;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import org.apache.commons.lang3.tuple.Pair;

/** PostgreSQL data type mapping to AVRO types. */
public class PostgreSQLJdbcValueMappings implements JdbcValueMappingsProvider {
  private static final ResultSetValueMapper<?> valuePassThrough = (value, schema) -> value;

  private static final Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  private static final ResultSetValueExtractor<java.sql.Date> utcDateExtractor =
      (rs, fieldName) -> rs.getDate(fieldName, utcCalendar);

  private static final ResultSetValueMapper<java.sql.Date> sqlDateToAvro =
      (value, schema) -> (int) value.toLocalDate().toEpochDay();

  // TODO(thiagotnunes): Add missing type mappings
  private static final ImmutableMap<String, JdbcValueMapper<?>> SCHEMA_MAPPINGS =
      ImmutableMap.<String, Pair<ResultSetValueExtractor<?>, ResultSetValueMapper<?>>>builder()
          .put("BIGINT", Pair.of(ResultSet::getLong, valuePassThrough))
          .put("CHARACTER VARYING", Pair.of(ResultSet::getString, valuePassThrough))
          .put("DATE", Pair.of(utcDateExtractor, sqlDateToAvro))
          .put("INT8", Pair.of(ResultSet::getLong, valuePassThrough))
          .put("TEXT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("VARCHAR", Pair.of(ResultSet::getString, valuePassThrough))
          .build()
          .entrySet()
          .stream()
          .map(
              entry ->
                  Map.entry(
                      entry.getKey(),
                      new JdbcValueMapper<>(
                          entry.getValue().getLeft(), entry.getValue().getRight())))
          .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

  /** Get static mapping of SourceColumnType to {@link JdbcValueMapper}. */
  @Override
  public ImmutableMap<String, JdbcValueMapper<?>> getMappings() {
    return SCHEMA_MAPPINGS;
  }
}
