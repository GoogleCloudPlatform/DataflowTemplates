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
package com.google.cloud.teleport.v2.source.oracle.reader.io.jdbc.rowmapper.provider;

import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.JdbcMappings;
import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.JdbcValueMapper;
import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.ResultSetValueExtractor;
import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.ResultSetValueMapper;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.provider.unified.CustomSchema.TimeStampTz;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecordBuilder;

public class OracleJdbcValueMappings implements JdbcValueMappingsProvider {

  private static final ResultSetValueMapper<?> valuePassThrough = (value, schema) -> value;

  private static final ResultSetValueExtractor<java.sql.Timestamp> timestampExtractor =
      (rs, fieldName) -> rs.getTimestamp(fieldName);

  private static final ResultSetValueMapper<java.sql.Timestamp> timestampToAvroMicros =
      (value, schema) -> {
        return TimeUnit.SECONDS.toMicros(value.getTime() / 1000)
            + TimeUnit.NANOSECONDS.toMicros(value.getNanos());
      };

  private static final ResultSetValueMapper<java.sql.Timestamp> timestampTzToAvroRecord =
      (value, schema) -> {
        long micros =
            TimeUnit.SECONDS.toMicros(value.getTime() / 1000)
                + TimeUnit.NANOSECONDS.toMicros(value.getNanos());
        return new GenericRecordBuilder(TimeStampTz.SCHEMA)
            .set(TimeStampTz.TIMESTAMP_FIELD_NAME, micros)
            .set(TimeStampTz.OFFSET_FIELD_NAME, 0)
            .build();
      };

  private static final ResultSetValueMapper<java.math.BigDecimal> bigDecimalToByteArray =
      (value, schema) -> {
        if (value == null) {
          return null;
        }
        return ByteBuffer.wrap(value.unscaledValue().toByteArray());
      };

  private static final ResultSetValueMapper<byte[]> byteArrayToByteBuffer =
      (value, schema) -> {
        if (value == null) {
          return null;
        }
        return ByteBuffer.wrap(value);
      };

  private static final JdbcMappings JDBC_MAPPINGS =
      JdbcMappings.builder()
          .put("VARCHAR", ResultSet::getString, valuePassThrough, 1024)
          .put("VARCHAR2", ResultSet::getString, valuePassThrough, 1024)
          .put("CHAR", ResultSet::getString, valuePassThrough, 255)
          .put("NVARCHAR2", ResultSet::getString, valuePassThrough, 1024)
          .put("NCHAR", ResultSet::getString, valuePassThrough, 255)
          .put("NUMBER", ResultSet::getString, valuePassThrough, 32)
          .put("DECIMAL", ResultSet::getBigDecimal, bigDecimalToByteArray, 32)
          .put("FLOAT", ResultSet::getFloat, valuePassThrough, 4)
          .put("DOUBLE PRECISION", ResultSet::getDouble, valuePassThrough, 8)
          .put("BINARY_FLOAT", ResultSet::getFloat, valuePassThrough, 4)
          .put("BINARY_DOUBLE", ResultSet::getDouble, valuePassThrough, 8)
          .put(
              "INTEGER",
              (rs, colName) -> {
                BigDecimal val = rs.getBigDecimal(colName);
                return rs.wasNull() ? null : val.longValue();
              },
              valuePassThrough,
              8)
          .put(
              "INT",
              (rs, colName) -> {
                BigDecimal val = rs.getBigDecimal(colName);
                return rs.wasNull() ? null : val.longValue();
              },
              valuePassThrough,
              8)
          .put(
              "SMALLINT",
              (rs, colName) -> {
                BigDecimal val = rs.getBigDecimal(colName);
                return rs.wasNull() ? null : val.longValue();
              },
              valuePassThrough,
              4)
          .put("DATE", timestampExtractor, timestampToAvroMicros, 8)
          .put("TIMESTAMP", timestampExtractor, timestampToAvroMicros, 8)
          .put("TIMESTAMP WITH TIME ZONE", timestampExtractor, timestampTzToAvroRecord, 8)
          .put("TIMESTAMP WITH LOCAL TIME ZONE", timestampExtractor, timestampTzToAvroRecord, 8)
          .put("RAW", ResultSet::getBytes, byteArrayToByteBuffer, 2000)
          .put("BOOLEAN", ResultSet::getBoolean, valuePassThrough, 1)
          .put("CLOB", ResultSet::getString, valuePassThrough, 4000)
          .put("NCLOB", ResultSet::getString, valuePassThrough, 4000)
          .put("BLOB", ResultSet::getBytes, byteArrayToByteBuffer, 4000)
          .build();

  @Override
  public ImmutableMap<String, JdbcValueMapper<?>> getMappings() {
    return JDBC_MAPPINGS.mappings();
  }

  @Override
  public int estimateColumnSize(SourceColumnType sourceColumnType) {
    String typeName = sourceColumnType.getName().toUpperCase();
    if (JDBC_MAPPINGS.sizeEstimators().containsKey(typeName)) {
      return JDBC_MAPPINGS.sizeEstimators().get(typeName).apply(sourceColumnType);
    }
    return 100;
  }
}
