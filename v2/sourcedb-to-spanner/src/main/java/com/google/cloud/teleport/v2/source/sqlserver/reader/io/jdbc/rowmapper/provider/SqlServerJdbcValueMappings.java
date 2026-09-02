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
package com.google.cloud.teleport.v2.source.sqlserver.reader.io.jdbc.rowmapper.provider;

import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.JdbcMappings;
import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.JdbcValueMapper;
import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.JdbcValueMappingsProvider;
import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.ResultSetValueExtractor;
import com.google.cloud.teleport.v2.reader.io.jdbc.rowmapper.ResultSetValueMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerJdbcValueMappings implements JdbcValueMappingsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(SqlServerJdbcValueMappings.class);

  private static final ResultSetValueMapper<Object> valuePassThrough = (value, schema) -> value;

  private static final ResultSetValueMapper<byte[]> bytesToByteBuffer =
      (value, schema) -> ByteBuffer.wrap(value);

  private static final ResultSetValueMapper<Timestamp> sqlTimestampToAvroTimestampMicros =
      (value, schema) -> value.getTime() * 1000 + (value.getNanos() / 1000) % 1000;

  /* Extract UTC Values for date and time related types */
  private static final Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
  private static final ResultSetValueExtractor<Timestamp> utcTimeStampExtractor =
      (rs, index) -> rs.getTimestamp(index, utcCalendar);

  private static final ResultSetValueExtractor<Date> utcDateExtractor =
      (rs, index) -> rs.getDate(index, utcCalendar);

  private static final ResultSetValueMapper<Date> sqlDateToAvroDate =
      (value, schema) -> (int) value.toLocalDate().toEpochDay();

  private static final ResultSetValueMapper<BigDecimal> bigDecimalToByteArray =
      (value, schema) -> ByteBuffer.wrap(value.unscaledValue().toByteArray());

  private static final ResultSetValueMapper<Object> vectorToAvroArray =
      (value, schema) -> {
        if (value == null) {
          return null;
        }
        // Depending on the JDBC version and connection properties, value will either be raw byte[]
        // containing
        // SQL Server's internal binary vector representation (the default behavior in the JDBC
        // driver when reading
        //  native VECTOR columns) or a String / CharSequence in JSON array format ("[1.5, 2.5,
        // 3.5]").
        if (value instanceof byte[]) {
          byte[] bytes = (byte[]) value;
          if (bytes.length < 8) {
            return Collections.emptyList();
          }
          ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
          buf.position(8);
          int numDimensions = (bytes.length - 8) / 4;
          List<Double> list = new ArrayList<>(numDimensions);
          for (int i = 0; i < numDimensions; i++) {
            list.add(Double.valueOf(Float.toString(buf.getFloat())));
          }
          return list;
        }
        String trimmed = value.toString().trim();
        if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
          trimmed = trimmed.substring(1, trimmed.length() - 1).trim();
        }
        if (trimmed.isEmpty()) {
          return Collections.emptyList();
        }
        String[] parts = trimmed.split(",");
        List<Double> list = new ArrayList<>(parts.length);
        for (String part : parts) {
          list.add(Double.valueOf(part.trim()));
        }
        return list;
      };

  private static int getLengthOrPrecision(SourceColumnType sourceColumnType) {
    Long[] mods = sourceColumnType.getMods();
    return (mods != null && mods.length > 0 && mods[0] != null) ? mods[0].intValue() : 0;
  }

  private static int getLengthOrPrecision(SourceColumnType sourceColumnType, int defaultValue) {
    int n = getLengthOrPrecision(sourceColumnType);
    if (n > 0) {
      return n;
    }
    LOG.warn(
        "Column {} has no length/precision (n={}). Using default: {}",
        sourceColumnType,
        n,
        defaultValue);
    return defaultValue;
  }

  private static int estimateDecimalSize(SourceColumnType sourceColumnType) {
    int p = getLengthOrPrecision(sourceColumnType, 38);
    if (p <= 9) {
      return 5;
    } else if (p <= 19) {
      return 9;
    } else if (p <= 28) {
      return 13;
    }
    return 17;
  }

  private static final JdbcMappings JDBC_MAPPINGS =
      JdbcMappings.builder()
          .put("TINYINT", ResultSet::getLong, valuePassThrough, 1)
          .put("SMALLINT", ResultSet::getLong, valuePassThrough, 2)
          .put("INT", ResultSet::getLong, valuePassThrough, 4)
          .put("BIGINT", ResultSet::getLong, valuePassThrough, 8)
          .put("BIT", ResultSet::getBoolean, valuePassThrough, 1)
          .put(
              "DECIMAL",
              ResultSet::getBigDecimal,
              bigDecimalToByteArray,
              SqlServerJdbcValueMappings::estimateDecimalSize)
          .put(
              "NUMERIC",
              ResultSet::getBigDecimal,
              bigDecimalToByteArray,
              SqlServerJdbcValueMappings::estimateDecimalSize)
          .put("MONEY", ResultSet::getBigDecimal, bigDecimalToByteArray, 8)
          .put("SMALLMONEY", ResultSet::getBigDecimal, bigDecimalToByteArray, 4)
          .put("FLOAT", ResultSet::getDouble, valuePassThrough, 8)
          .put("REAL", ResultSet::getFloat, valuePassThrough, 4)
          .put("DATE", utcDateExtractor, sqlDateToAvroDate, 4)
          .put("TIME", ResultSet::getString, valuePassThrough, 12)
          .put("DATETIME2", utcTimeStampExtractor, sqlTimestampToAvroTimestampMicros, 11)
          .put("DATETIMEOFFSET", utcTimeStampExtractor, sqlTimestampToAvroTimestampMicros, 11)
          .put("DATETIME", utcTimeStampExtractor, sqlTimestampToAvroTimestampMicros, 11)
          .put("SMALLDATETIME", utcTimeStampExtractor, sqlTimestampToAvroTimestampMicros, 11)
          .put(
              "CHAR",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> getLengthOrPrecision(sourceColumnType, 255))
          .put(
              "VARCHAR",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> getLengthOrPrecision(sourceColumnType, 65535))
          .put("TEXT", ResultSet::getString, valuePassThrough, 65535)
          .put(
              "NCHAR",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> getLengthOrPrecision(sourceColumnType, 255) * 2)
          .put(
              "NVARCHAR",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> {
                int n = getLengthOrPrecision(sourceColumnType, 32767);
                return Math.min(n * 2, 65535);
              })
          .put("NTEXT", ResultSet::getString, valuePassThrough, 65535)
          .put(
              "BINARY",
              ResultSet::getBytes,
              bytesToByteBuffer,
              sourceColumnType -> getLengthOrPrecision(sourceColumnType, 255))
          .put(
              "VARBINARY",
              ResultSet::getBytes,
              bytesToByteBuffer,
              sourceColumnType -> getLengthOrPrecision(sourceColumnType, 65535))
          .put("IMAGE", ResultSet::getBytes, bytesToByteBuffer, 65535)
          .put("ROWVERSION", ResultSet::getBytes, bytesToByteBuffer, 8)
          .put("TIMESTAMP", ResultSet::getBytes, bytesToByteBuffer, 8)
          .put("UNIQUEIDENTIFIER", ResultSet::getString, valuePassThrough, 36)
          .put("XML", ResultSet::getString, valuePassThrough, 65535)
          .put("JSON", ResultSet::getString, valuePassThrough, 65535)
          .put(
              "VECTOR",
              ResultSet::getObject,
              vectorToAvroArray,
              sourceColumnType -> {
                int n = getLengthOrPrecision(sourceColumnType, 0);
                return n > 0 ? 8 + n * 4 : 65535;
              })
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
    LOG.warn("Unknown column type: {}. Defaulting to size: 65,535.", sourceColumnType);
    return 65_535;
  }
}
