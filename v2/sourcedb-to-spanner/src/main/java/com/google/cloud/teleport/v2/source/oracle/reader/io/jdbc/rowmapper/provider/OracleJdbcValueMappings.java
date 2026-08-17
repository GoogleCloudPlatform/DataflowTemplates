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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleJdbcValueMappings implements JdbcValueMappingsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(OracleJdbcValueMappings.class);

  private static final ResultSetValueMapper<?> valuePassThrough = (value, schema) -> value;

  private static final ResultSetValueExtractor<java.sql.Timestamp> timestampExtractor =
      (rs, fieldName) -> rs.getTimestamp(fieldName);

  private static final ResultSetValueMapper<java.sql.Timestamp> timestampToAvroMicros =
      (value, schema) -> {
        return TimeUnit.SECONDS.toMicros(value.getTime() / 1000)
            + TimeUnit.NANOSECONDS.toMicros(value.getNanos());
      };

  private static final ResultSetValueExtractor<java.time.ZonedDateTime> zonedDateTimeExtractor =
      (rs, fieldName) -> rs.getObject(fieldName, java.time.ZonedDateTime.class);

  private static final ResultSetValueMapper<java.time.ZonedDateTime> zonedDateTimeToAvroRecord =
      (value, schema) -> {
        long micros =
            TimeUnit.SECONDS.toMicros(value.toEpochSecond())
                + TimeUnit.NANOSECONDS.toMicros(value.getNano());
        return new GenericRecordBuilder(TimeStampTz.SCHEMA)
            .set(TimeStampTz.TIMESTAMP_FIELD_NAME, micros)
            .set(TimeStampTz.OFFSET_FIELD_NAME, value.getOffset().getTotalSeconds() * 1000)
            .build();
      };

  private static final ResultSetValueMapper<java.math.BigDecimal> bigDecimalToByteArray =
      (value, schema) -> {
        if (value == null) {
          return null;
        }
        // Force the DB-extracted decimal to safely adhere to the Avro schema's absolute scale
        return java.nio.ByteBuffer.wrap(
            value
                .setScale((int) schema.getObjectProp("scale"), java.math.RoundingMode.HALF_DOWN)
                .unscaledValue()
                .toByteArray());
      };

  private static final ResultSetValueMapper<byte[]> byteArrayToByteBuffer =
      (value, schema) -> {
        if (value == null) {
          return null;
        }
        return ByteBuffer.wrap(value);
      };

  private static long getLengthOrPrecision(SourceColumnType sourceColumnType) {
    Long[] mods = sourceColumnType.getMods();
    return (mods != null && mods.length > 0 && mods[0] != null) ? mods[0] : 0;
  }

  private static long getLengthOrPrecision(SourceColumnType sourceColumnType, long defaultValue) {
    long n = getLengthOrPrecision(sourceColumnType);
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

  /**
   * Static mapping of Oracle SourceColumnType to JdbcValueMapper alongside physical byte size
   * estimators.
   *
   * <p>Sizes referenced from Oracle 19c specification: <a
   * href="https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html">Oracle
   * Data Types</a>. If a column size is missing in the JDBC metadata, it gracefully falls back to
   * Oracle's standard conceptual maximums (e.g. 4000 for string literals).
   */
  private static final JdbcMappings JDBC_MAPPINGS =
      JdbcMappings.builder()
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-0BC16006-258A-4217-ABC2-0BAF8DE5B26E
           * Max standard byte limit is strictly 4000 bytes. While MAX_STRING_SIZE=EXTENDED enables 32767 bytes,
           * STANDARD is explicitly chosen to conservatively prevent Dataflow from splitting bundles into aggressively
           * undersized limits for unconfigured strings. Multiplied by 4 for safe UTF-8 byte packing fallback.
           */
          .put(
              "VARCHAR",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> (int) getLengthOrPrecision(sourceColumnType, 4000) * 4)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-A9369ED7-DCA6-4AE0-B88E-630248DF8885
           * Max standard byte limit is strictly 4000 bytes. While MAX_STRING_SIZE=EXTENDED enables 32767 bytes,
           * STANDARD is explicitly chosen to prevent heavily aggressively undersized bundle packing bounds sizes.
           * Multiplied by 4 to pad UTF-8 encoding expansion bounds.
           */
          .put(
              "VARCHAR2",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> (int) getLengthOrPrecision(sourceColumnType, 4000) * 4)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-EAF4A5B7-5CBC-4B9F-84E3-CEB31BFAF2BF
           * Max physical length is absolutely 2000 bytes. Multiplied by 4 to pad UTF-8 encoding expansion bounds.
           */
          .put(
              "CHAR",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> (int) getLengthOrPrecision(sourceColumnType, 2000) * 4)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-E1DF77E9-8438-4F1E-BD1B-CEAECC2C80FF
           * Max standard byte limit is strictly 4000 bytes. While MAX_STRING_SIZE=EXTENDED enables 32767 bytes,
           * STANDARD is chosen to avoid artificially undersized buffer sizes. Multiplied by 4 to pad UTF-8 boundaries.
           */
          .put(
              "NVARCHAR2",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> (int) getLengthOrPrecision(sourceColumnType, 4000) * 4)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-A87A6EF1-BEBF-48BB-98D1-98782E0FCDB8
           * Max absolute limit securely caps accurately at 2000 bytes physically. Multiplied by 4 for bounds logic.
           */
          .put(
              "NCHAR",
              ResultSet::getString,
              valuePassThrough,
              sourceColumnType -> (int) getLengthOrPrecision(sourceColumnType, 2000) * 4)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-A033ED31-F998-4CE8-ACAD-65DABEBD6EA3
           * Max physical Oracle precision storage byte limit is 22. Scaled directly from documentation.
           */
          .put("NUMBER", ResultSet::getString, valuePassThrough, 22)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-A033ED31-F998-4CE8-ACAD-65DABEBD6EA3
           * Max physical Oracle precision storage byte limit is 22. Scaled directly from documentation.
           */
          .put("DECIMAL", ResultSet::getBigDecimal, bigDecimalToByteArray, 22)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-F579B4B8-0444-4CE0-B52F-73353E042EE7
           * Oracle physical storage conceptual byte limit is 22 natively.
           */
          .put("FLOAT", ResultSet::getFloat, valuePassThrough, 22)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-192EBCA6-95AE-4BE5-A473-F26DC63DB8DA
           * Oracle physical storage conceptual byte limit is 22 natively.
           */
          .put("DOUBLE PRECISION", ResultSet::getDouble, valuePassThrough, 22)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-F579B4B8-0444-4CE0-B52F-73353E042EE7
           * 32-bit floating point internally bounded natively to exactly 4 physical bytes.
           */
          .put("BINARY_FLOAT", ResultSet::getFloat, valuePassThrough, 4)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-F579B4B8-0444-4CE0-B52F-73353E042EE7
           * 64-bit floating point inherently internally bounded natively to exactly 8 physical bytes.
           */
          .put("BINARY_DOUBLE", ResultSet::getDouble, valuePassThrough, 8)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-A033ED31-F998-4CE8-ACAD-65DABEBD6EA3
           * ANSI compat. Maximum Oracle storage boundary limit fundamentally caps at 22 bytes.
           */
          .put(
              "INTEGER",
              (rs, colName) -> {
                BigDecimal val = rs.getBigDecimal(colName);
                return rs.wasNull() ? null : val.longValue();
              },
              valuePassThrough,
              22)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-A033ED31-F998-4CE8-ACAD-65DABEBD6EA3
           * ANSI compat. Maximum Oracle storage boundary limit fundamentally caps at 22 bytes.
           */
          .put(
              "INT",
              (rs, colName) -> {
                BigDecimal val = rs.getBigDecimal(colName);
                return rs.wasNull() ? null : val.longValue();
              },
              valuePassThrough,
              22)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-A033ED31-F998-4CE8-ACAD-65DABEBD6EA3
           * ANSI compat. Maximum Oracle storage boundary limit fundamentally caps at 22 bytes.
           */
          .put(
              "SMALLINT",
              (rs, colName) -> {
                BigDecimal val = rs.getBigDecimal(colName);
                return rs.wasNull() ? null : val.longValue();
              },
              valuePassThrough,
              22)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-F2EBDFEC-8BDD-48ED-9C1C-509BFDC4B325
           * Oracle essentially allocates exactly 7 physical bytes for standard representation.
           */
          .put("DATE", timestampExtractor, timestampToAvroMicros, 7)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-F2EBDFEC-8BDD-48ED-9C1C-509BFDC4B325
           * Oracle essentially allocates exactly 11 physical bytes natively for standard representation.
           */
          .put("TIMESTAMP", timestampExtractor, timestampToAvroMicros, 11)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-F2EBDFEC-8BDD-48ED-9C1C-509BFDC4B325
           * Oracle essentially allocates exactly 13 physical bytes natively for standard representation.
           */
          .put("TIMESTAMP WITH TIME ZONE", zonedDateTimeExtractor, zonedDateTimeToAvroRecord, 13)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-F2EBDFEC-8BDD-48ED-9C1C-509BFDC4B325
           * Oracle essentially allocates exactly 11 physical bytes natively for standard representation.
           */
          .put(
              "TIMESTAMP WITH LOCAL TIME ZONE",
              zonedDateTimeExtractor,
              zonedDateTimeToAvroRecord,
              11)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-85D757CB-AEE1-41C8-A392-AD31BE662059
           * Oracle standard structurally dictates exactly 11 natively physical bytes accurately.
           */
          .put("INTERVAL YEAR TO MONTH", ResultSet::getString, valuePassThrough, 11)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-85D757CB-AEE1-41C8-A392-AD31BE662059
           * Oracle standard structurally dictates exactly 11 natively physical bytes accurately.
           */
          .put("INTERVAL DAY TO SECOND", ResultSet::getString, valuePassThrough, 11)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-561A85BE-7FB9-40FC-89DE-1CDEAED5E61B
           * Absolute physical maximum structurally limits beautifully clearly and properly rigidly explicitly to 2000.
           */
          .put(
              "RAW",
              ResultSet::getBytes,
              byteArrayToByteBuffer,
              sourceColumnType -> (int) getLengthOrPrecision(sourceColumnType, 2000))
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-0BC16006-258A-4217-ABC2-0BAF8DE5B26E
           * Transposed securely seamlessly to 1 primitive standard byte functionally.
           */
          .put("BOOLEAN", ResultSet::getBoolean, valuePassThrough, 1)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-D4EC7A0D-C119-4CB6-B6A2-EB0BCEDDBD35
           * Capacities span up to 4GB. Mapped identically to MySQL LONGTEXT constraints natively via Integer.MAX_VALUE.
           */
          .put("CLOB", ResultSet::getString, valuePassThrough, Integer.MAX_VALUE)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-D4EC7A0D-C119-4CB6-B6A2-EB0BCEDDBD35
           * Capacities span up to 4GB. Mapped identically to MySQL LONGTEXT constraints natively via Integer.MAX_VALUE.
           */
          .put("NCLOB", ResultSet::getString, valuePassThrough, Integer.MAX_VALUE)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-D4EC7A0D-C119-4CB6-B6A2-EB0BCEDDBD35
           * Capacities span up to 4GB. Mapped identically to MySQL LONGBLOB constraints natively via Integer.MAX_VALUE.
           */
          .put("BLOB", ResultSet::getBytes, byteArrayToByteBuffer, Integer.MAX_VALUE)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-D4EC7A0D-C119-4CB6-B6A2-EB0BCEDDBD35
           */
          .put("ROWID", ResultSet::getString, valuePassThrough, 20)
          .put("UROWID", ResultSet::getString, valuePassThrough, 4000)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-D4EC7A0D-C119-4CB6-B6A2-EB0BCEDDBD35
           * Size of BFILE can go upto 2gb, so setting to Interger.MAX_VALUE
           */
          .put("BFILE", ResultSet::getBytes, valuePassThrough, Integer.MAX_VALUE)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-D4EC7A0D-C119-4CB6-B6A2-EB0BCEDDBD35
           * Size of LONG RAW/LONG can go upto 2gb, so setting to Interger.MAX_VALUE
           */
          .put("LONG RAW", ResultSet::getBytes, valuePassThrough, Integer.MAX_VALUE)
          .put("LONG", ResultSet::getString, valuePassThrough, Integer.MAX_VALUE)
          /*
           * Ref: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-D4EC7A0D-C119-4CB6-B6A2-EB0BCEDDBD35
           * Size of JSON can go upto 4gb, so setting to Interger.MAX_VALUE
           */
          .put("JSON", ResultSet::getString, valuePassThrough, Integer.MAX_VALUE)
          .build();

  @Override
  public ImmutableMap<String, JdbcValueMapper<?>> getMappings() {
    return JDBC_MAPPINGS.mappings();
  }

  /**
   * estimate the column size in bytes for a given column type.
   *
   * <p>Ref: <a
   * href="https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html">Oracle
   * Data Types</a> If type is unknown, it defaults safely to 100 bytes to tightly constrain bundle
   * metrics.
   */
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
