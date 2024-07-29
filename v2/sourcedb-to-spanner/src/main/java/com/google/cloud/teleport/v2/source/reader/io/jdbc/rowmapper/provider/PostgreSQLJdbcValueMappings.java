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
import java.time.Instant;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;

public class PostgreSQLJdbcValueMappings implements JdbcValueMappingsProvider {
  private static final ResultSetValueMapper<?> valuePassThrough = (value, schema) -> value;

  private static final Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  private static long instantToMicro(Instant instant) {
    return TimeUnit.SECONDS.toMicros(instant.getEpochSecond())
        + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
  }

  private static final ResultSetValueExtractor<java.sql.Timestamp> utcTimeStampExtractor =
      (rs, fieldName) -> rs.getTimestamp(fieldName, utcCalendar);

  private static final ResultSetValueExtractor<java.sql.Date> utcDateExtractor =
      (rs, fieldName) -> rs.getDate(fieldName, utcCalendar);

  private static final ResultSetValueMapper<java.sql.Date> sqlDateToAvroTimestampMicros =
      (value, schema) -> TimeUnit.DAYS.toMicros(value.toLocalDate().toEpochDay());

  private static final ResultSetValueMapper<java.sql.Timestamp> sqlTimestampToAvroTimestampMicros =
      (value, schema) -> instantToMicro(value.toInstant());

  private static final ImmutableMap<String, JdbcValueMapper<?>> SCHEMA_MAPPINGS =
      ImmutableMap.<String, Pair<ResultSetValueExtractor<?>, ResultSetValueMapper<?>>>builder()
          .put("BIGINT", Pair.of(ResultSet::getLong, valuePassThrough))
          .put("BIGSERIAL", Pair.of(ResultSet::getLong, valuePassThrough))
          .put("BIT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("BIT VARYING", Pair.of(ResultSet::getString, valuePassThrough))
          .put("BOOL", Pair.of(ResultSet::getBoolean, valuePassThrough))
          .put("BOOLEAN", Pair.of(ResultSet::getBoolean, valuePassThrough))
          .put("BOX", Pair.of(ResultSet::getString, valuePassThrough))
          .put("BYTEA", Pair.of(ResultSet::getString, valuePassThrough))
          .put("CHAR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("CHARACTER", Pair.of(ResultSet::getString, valuePassThrough))
          .put("CHARACTER VARYING", Pair.of(ResultSet::getString, valuePassThrough))
          .put("CIDR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("CIRCLE", Pair.of(ResultSet::getString, valuePassThrough))
          .put("DATE", Pair.of(utcDateExtractor, sqlDateToAvroTimestampMicros))
          .put("DECIMAL", Pair.of(ResultSet::getString, valuePassThrough))
          .put("DOUBLE PRECISION", Pair.of(ResultSet::getDouble, valuePassThrough))
          .put("FLOAT4", Pair.of(ResultSet::getFloat, valuePassThrough))
          .put("FLOAT8", Pair.of(ResultSet::getDouble, valuePassThrough))
          .put("INET", Pair.of(ResultSet::getString, valuePassThrough))
          .put("INTEGER", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("INT2", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("INT4", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("INT8", Pair.of(ResultSet::getLong, valuePassThrough))
          .put("INTERVAL", Pair.of(ResultSet::getString, valuePassThrough))
          .put("JSON", Pair.of(ResultSet::getString, valuePassThrough))
          .put("JSONB", Pair.of(ResultSet::getString, valuePassThrough))
          .put("LINE", Pair.of(ResultSet::getString, valuePassThrough))
          .put("LSEG", Pair.of(ResultSet::getString, valuePassThrough))
          .put("MACADDR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("MACADDR8", Pair.of(ResultSet::getString, valuePassThrough))
          .put("MONEY", Pair.of(ResultSet::getString, valuePassThrough))
          .put("NUMERIC", Pair.of(ResultSet::getString, valuePassThrough))
          .put("PATH", Pair.of(ResultSet::getString, valuePassThrough))
          .put("PG_LSN", Pair.of(ResultSet::getString, valuePassThrough))
          .put("PG_SNAPSHOT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("POINT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("POLYGON", Pair.of(ResultSet::getString, valuePassThrough))
          .put("REAL", Pair.of(ResultSet::getFloat, valuePassThrough))
          .put("SERIAL2", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("SERIAL4", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("SERIAL8", Pair.of(ResultSet::getLong, valuePassThrough))
          .put("SMALLINT", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("SMALLSERIAL", Pair.of(ResultSet::getInt, valuePassThrough))
          .put("TEXT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("TIME", Pair.of(ResultSet::getTime, valuePassThrough))
          .put("TIME WITHOUT TIME ZONE", Pair.of(ResultSet::getTime, valuePassThrough))
          .put("TIMESTAMP", Pair.of(utcTimeStampExtractor, sqlTimestampToAvroTimestampMicros))
          .put(
              "TIMESTAMP WITHOUT TIME ZONE",
              Pair.of(utcTimeStampExtractor, sqlTimestampToAvroTimestampMicros))
          .put("TIMESTAMPTZ", Pair.of(utcTimeStampExtractor, sqlTimestampToAvroTimestampMicros))
          .put(
              "TIMESTAMP WITH TIME ZONE",
              Pair.of(utcTimeStampExtractor, sqlTimestampToAvroTimestampMicros))
          .put("TIMETZ", Pair.of(ResultSet::getTime, valuePassThrough))
          .put("TIME WITH TIME ZONE", Pair.of(ResultSet::getTime, valuePassThrough))
          .put("TSQUERY", Pair.of(ResultSet::getString, valuePassThrough))
          .put("TSVECTOR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("TXID_SNAPSHOT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("UUID", Pair.of(ResultSet::getString, valuePassThrough))
          .put("VARBIT", Pair.of(ResultSet::getString, valuePassThrough))
          .put("VARCHAR", Pair.of(ResultSet::getString, valuePassThrough))
          .put("XML", Pair.of(ResultSet::getString, valuePassThrough))
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
