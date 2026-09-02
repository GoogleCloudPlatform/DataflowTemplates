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
import java.sql.ResultSet;
import java.util.Calendar;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerJdbcValueMappings implements JdbcValueMappingsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(SqlServerJdbcValueMappings.class);

  private static final ResultSetValueMapper<Object> valuePassThrough = (value, schema) -> value;

  private static final ResultSetValueMapper<byte[]> bytesToByteBuffer =
      (value, schema) -> ByteBuffer.wrap(value);

  private static final ResultSetValueMapper<java.sql.Timestamp> sqlTimestampToAvroTimestampMicros =
      (value, schema) -> value.getTime() * 1000 + (value.getNanos() / 1000) % 1000;

  private static final ResultSetValueExtractor<java.sql.Timestamp> utcTimeStampExtractor =
      (rs, index) -> rs.getTimestamp(index, Calendar.getInstance(TimeZone.getTimeZone("UTC")));

  private static final ResultSetValueExtractor<java.sql.Date> utcDateExtractor =
      (rs, index) -> rs.getDate(index, Calendar.getInstance(TimeZone.getTimeZone("UTC")));

  private static final ResultSetValueMapper<java.sql.Date> sqlDateToAvroDate =
      (value, schema) -> (int) value.toLocalDate().toEpochDay();

  private static final ResultSetValueMapper<BigDecimal> bigDecimalToByteArray =
      (value, schema) -> ByteBuffer.wrap(value.unscaledValue().toByteArray());

  private static final ResultSetValueMapper<Object> vectorToAvroArray =
      (value, schema) -> {
        if (value == null) {
          return null;
        }
        if (value instanceof byte[]) {
          byte[] bytes = (byte[]) value;
          if (bytes.length < 8) {
            return java.util.Collections.emptyList();
          }
          java.nio.ByteBuffer buf =
              java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN);
          buf.position(8);
          int numDimensions = (bytes.length - 8) / 4;
          java.util.List<Double> list = new java.util.ArrayList<>(numDimensions);
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
          return java.util.Collections.emptyList();
        }
        String[] parts = trimmed.split(",");
        java.util.List<Double> list = new java.util.ArrayList<>(parts.length);
        for (String part : parts) {
          list.add(Double.valueOf(part.trim()));
        }
        return list;
      };

  private static final JdbcMappings JDBC_MAPPINGS =
      JdbcMappings.builder()
          .put("TINYINT", ResultSet::getLong, valuePassThrough, 1)
          .put("SMALLINT", ResultSet::getLong, valuePassThrough, 2)
          .put("INT", ResultSet::getLong, valuePassThrough, 4)
          .put("BIGINT", ResultSet::getLong, valuePassThrough, 8)
          .put("BIT", ResultSet::getBoolean, valuePassThrough, 1)
          .put("DECIMAL", ResultSet::getBigDecimal, bigDecimalToByteArray, 16)
          .put("NUMERIC", ResultSet::getBigDecimal, bigDecimalToByteArray, 16)
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
          .put("CHAR", ResultSet::getString, valuePassThrough, 255)
          .put("VARCHAR", ResultSet::getString, valuePassThrough, 65535)
          .put("TEXT", ResultSet::getString, valuePassThrough, 65535)
          .put("NCHAR", ResultSet::getString, valuePassThrough, 255)
          .put("NVARCHAR", ResultSet::getString, valuePassThrough, 65535)
          .put("NTEXT", ResultSet::getString, valuePassThrough, 65535)
          .put("BINARY", ResultSet::getBytes, bytesToByteBuffer, 65535)
          .put("VARBINARY", ResultSet::getBytes, bytesToByteBuffer, 65535)
          .put("IMAGE", ResultSet::getBytes, bytesToByteBuffer, 65535)
          .put("ROWVERSION", ResultSet::getBytes, bytesToByteBuffer, 8)
          .put("TIMESTAMP", ResultSet::getBytes, bytesToByteBuffer, 8)
          .put("UNIQUEIDENTIFIER", ResultSet::getString, valuePassThrough, 36)
          .put("XML", ResultSet::getString, valuePassThrough, 65535)
          .put("JSON", ResultSet::getString, valuePassThrough, 65535)
          .put("VECTOR", ResultSet::getObject, vectorToAvroArray, 65535)
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
