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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.mappings;

import com.datastax.driver.core.Duration;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper.CassandraFieldMapper;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper.CassandraRowValueExtractor;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper.CassandraRowValueMapper;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapping;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.IntervalNano;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.UnifiedMappingProvider;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.codec.binary.Hex;

public class CassandraMappingsProvider {
  /** Pass the value as is to avro. */
  private static final CassandraRowValueMapper valuePassThrough = (value, schema) -> value;

  /** Pass the value as a string to avro. */
  private static final CassandraRowValueMapper toString = (value, schema) -> value.toString();

  /** Pass the value as an integer to avro. */
  private static final CassandraRowValueMapper<Byte> byteToInt =
      (value, schema) -> value.intValue();

  private static final CassandraRowValueMapper<Short> shortToInt =
      (value, schema) -> value.intValue();

  /** Map {@link ByteBuffer} to a Hex encoded String. */
  private static final CassandraRowValueMapper<ByteBuffer> ByteBufferToHexString =
      (value, schema) -> new String(Hex.encodeHex(value.array()));

  /**
   * Map {@link LocalDate} to {@link LogicalTypes.Date}. Cassandra Date type encodes number of days
   * since epoch, without any time or time zone component.
   *
   * <p>See: <a href="https://cassandra.apache.org/doc/stable/cassandra/cql/types.html">types</a>
   * for additional information on date type.
   */
  private static final CassandraRowValueMapper<LocalDate> localDateToAvroLogicalDate =
      (value, schema) -> value.getDaysSinceEpoch();

  private static final CassandraRowValueExtractor<Duration> getDuration =
      (row, name) -> row.get(name, TypeCodec.duration());

  private static final CassandraRowValueMapper<Duration> durationToAvro =
      (value, schema) ->
          new GenericRecordBuilder(IntervalNano.SCHEMA)
              .set(IntervalNano.MONTHS_FIELD_NAME, value.getMonths())
              .set(IntervalNano.DAYS_FIELD_NAME, value.getDays())
              .set(IntervalNano.NANOS_FIELD_NAME, value.getNanoseconds())
              .build();

  /**
   * Cassandra represents `Time` field as 64 bit singed integer representing number of nanoseconds
   * since midnight. See <a
   * href=https://cassandra.apache.org/doc/stable/cassandra/cql/types.html>types documentation</a>
   * for further details.
   */
  private static final CassandraRowValueMapper<Long> cassandraTimeToIntervalNano =
      (value, schema) ->
          new GenericRecordBuilder(IntervalNano.SCHEMA)
              .set(IntervalNano.NANOS_FIELD_NAME, value)
              .build();

  private static final CassandraRowValueMapper<Date> dateToAvro =
      (value, schema) -> value.getTime() * 1000L;

  private static final CassandraMappings CASSANDRA_MAPPINGS =
      CassandraMappings.builder()
          .put(
              "ASCII",
              UnifiedMappingProvider.Type.STRING,
              Row::getString,
              valuePassThrough,
              String.class)
          .put(
              "BIGINT",
              UnifiedMappingProvider.Type.LONG,
              Row::getLong,
              valuePassThrough,
              Long.class)
          .put(
              "BLOB",
              UnifiedMappingProvider.Type.STRING,
              Row::getBytes,
              ByteBufferToHexString,
              ByteBuffer.class)
          .put(
              "BOOLEAN",
              UnifiedMappingProvider.Type.BOOLEAN,
              Row::getBool,
              valuePassThrough,
              Boolean.class)
          .put(
              "COUNTER",
              UnifiedMappingProvider.Type.LONG,
              Row::getLong,
              valuePassThrough,
              Long.class)
          .put(
              "DATE",
              UnifiedMappingProvider.Type.DATE,
              Row::getDate,
              localDateToAvroLogicalDate,
              LocalDate.class)
          // The Cassandra decimal does not have precision and scale fixed in the
          // schema which would be needed if we want to map it to Avro Decimal.
          .put(
              "DECIMAL",
              UnifiedMappingProvider.Type.STRING,
              Row::getDecimal,
              toString,
              BigDecimal.class)
          .put(
              "DOUBLE",
              UnifiedMappingProvider.Type.DOUBLE,
              Row::getDouble,
              valuePassThrough,
              Double.class)
          .put(
              "DURATION",
              UnifiedMappingProvider.Type.INTERVAL_NANO,
              getDuration,
              durationToAvro,
              Duration.class)
          .put(
              "FLOAT",
              UnifiedMappingProvider.Type.FLOAT,
              Row::getFloat,
              valuePassThrough,
              Float.class)
          .put(
              "INET", UnifiedMappingProvider.Type.STRING, Row::getInet, toString, InetAddress.class)
          .put(
              "INT",
              UnifiedMappingProvider.Type.INTEGER,
              Row::getInt,
              valuePassThrough,
              Integer.class)
          .put(
              "SMALLINT",
              UnifiedMappingProvider.Type.INTEGER,
              Row::getShort,
              shortToInt,
              Short.class)
          .put(
              "TEXT",
              UnifiedMappingProvider.Type.STRING,
              Row::getString,
              valuePassThrough,
              String.class)
          .put(
              "TIME",
              UnifiedMappingProvider.Type.INTERVAL_NANO,
              Row::getTime,
              cassandraTimeToIntervalNano,
              Long.class)
          .put(
              "TIMESTAMP",
              UnifiedMappingProvider.Type.TIMESTAMP,
              Row::getTimestamp,
              dateToAvro,
              Date.class)
          .put("TIMEUUID", UnifiedMappingProvider.Type.STRING, Row::getUUID, toString, UUID.class)
          .put("TINYINT", UnifiedMappingProvider.Type.INTEGER, Row::getByte, byteToInt, Byte.class)
          .put("UUID", UnifiedMappingProvider.Type.STRING, Row::getUUID, toString, UUID.class)
          .put(
              "VARCHAR",
              UnifiedMappingProvider.Type.STRING,
              Row::getString,
              valuePassThrough,
              String.class)
          .put(
              "VARINT",
              UnifiedMappingProvider.Type.NUMBER,
              Row::getVarint,
              toString,
              BigInteger.class)
          .put(
              "UNSUPPORTED",
              UnifiedMappingProvider.Type.UNSUPPORTED,
              (row, name) -> null,
              (value, schema) -> null,
              null)
          .build();

  private CassandraMappingsProvider() {}

  /** Mappings for unified type interface. */
  public static ImmutableMap<String, UnifiedTypeMapping> getMapping() {
    return CASSANDRA_MAPPINGS.typeMapping();
  }

  /**
   * Field Mappers for {@link
   * com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper.CassandraSourceRowMapper}.
   */
  public static ImmutableMap<String, CassandraFieldMapper<?>> getFieldMapping() {
    return CASSANDRA_MAPPINGS.fieldMapping();
  }
}
