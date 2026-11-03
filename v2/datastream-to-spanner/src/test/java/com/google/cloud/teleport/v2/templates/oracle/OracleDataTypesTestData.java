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
package com.google.cloud.teleport.v2.templates.oracle;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Test data for {@link OracleDatastreamToSpannerDataTypesIT}, derived from the Oracle data type
 * mapping matrix.
 *
 * <p>Each {@link TypeTable} tests one Oracle column type mapping: its rows are inserted in Oracle
 * by {@code oracle-schema.sql} and replicated to a Spanner column of the table's Spanner type,
 * where the expected values are asserted. Value lists are shared between tables through the {@code
 * *_VALUES} constants. Non-key tables use ids {@code 1..n}, in the order of the expected values,
 * and every table has a single data column, named after the table with the {@code _table} suffix
 * replaced by {@code _col} (e.g. {@code char_col} in {@code char_table}). Expected values are the
 * Spanner values as text ({@code null} is SQL NULL): BYTES values are Base64, TIMESTAMP values are
 * RFC 3339. {@link UnsupportedTable}s cover Oracle types Datastream cannot replicate: only the row
 * count is checked.
 *
 * <p>The PostgreSQL-dialect test uses the same Oracle tables, rows and expected values, with the
 * PostgreSQL equivalent of each Spanner type (see {@link #postgresTables()}).
 *
 * <p>Types and values that are not tested:
 *
 * <ul>
 *   <li><b>TIMESTAMP WITH TIME ZONE</b> (scenario C): Oracle ORA-02329: TIMESTAMP WITH TIME ZONE
 *       cannot be a primary key.
 *   <li><b>TIMESTAMP WITH (LOCAL) TIME ZONE: 0001-01-01 / 9999-12-31</b> (scenario A/B/C): Open
 *       template bug: micros-&gt;nanos long overflow outside ~1677-09-21..2262-04-11
 *       (FormatDatastreamRecordToJson). Replaced by 1700-01-01 / 2200-12-31.
 *   <li><b>TIMESTAMP WITH (LOCAL) TIME ZONE: non-UTC offsets / region names</b> (scenario A/B/C):
 *       Open bug: for Oracle, Datastream sends the local wall-clock time plus the offset, and the
 *       template treats the wall clock as the instant (offset ignored). Only UTC values are
 *       validated.
 *   <li><b>JSON</b> (scenario A/B): Native JSON column type requires Oracle 21c (ORA-00902 on
 *       Oracle 19c); removed so the test runs on Oracle 19c and later.
 *   <li><b>BOOLEAN</b> (scenario A/B/C): Native BOOLEAN column type requires Oracle 23ai; test
 *       instance is Oracle XE 21c (ORA-00902).
 *   <li><b>(all)</b> (scenario E): Oracle has no scalar array column types; its collection types
 *       (VARRAY, NESTED TABLE) are unsupported by Datastream and covered in scenario D.
 *   <li><b>SDO_GEOMETRY</b> (scenario D): Oracle Spatial/Locator types are not installed in the
 *       gvenzl/oracle-xe:21-slim image (ORA-00902).
 *   <li><b>SDO_TOPO_GEOMETRY</b> (scenario D): Oracle Spatial/Locator types are not installed in
 *       the gvenzl/oracle-xe:21-slim image (ORA-00902).
 *   <li><b>SDO_GEORASTER</b> (scenario D): Oracle Spatial/Locator types are not installed in the
 *       gvenzl/oracle-xe:21-slim image (ORA-00902).
 *   <li><b>VECTOR</b> (scenario D): VECTOR requires Oracle 23ai (ORA-00902 on 21c).
 *   <li><b>ORDAUDIO</b> (scenario D): Oracle Multimedia (ORDSYS) was removed in Oracle 21c
 *       (ORA-00902).
 *   <li><b>ORDIMAGE</b> (scenario D): Oracle Multimedia (ORDSYS) was removed in Oracle 21c
 *       (ORA-00902).
 *   <li><b>ORDVIDEO</b> (scenario D): Oracle Multimedia (ORDSYS) was removed in Oracle 21c
 *       (ORA-00902).
 *   <li><b>ORDDOC</b> (scenario D): Oracle Multimedia (ORDSYS) was removed in Oracle 21c
 *       (ORA-00902).
 *   <li><b>SI_STILLIMAGE</b> (scenario D): SQL/MM Still Image (part of Oracle Multimedia) was
 *       removed in Oracle 21c (ORA-00902).
 *   <li><b>SI_COLOR</b> (scenario D): SQL/MM Still Image (part of Oracle Multimedia) was removed in
 *       Oracle 21c (ORA-00902).
 *   <li><b>SI_AVERAGECOLOR</b> (scenario D): SQL/MM Still Image (part of Oracle Multimedia) was
 *       removed in Oracle 21c (ORA-00902).
 *   <li><b>SI_COLORHISTOGRAM</b> (scenario D): SQL/MM Still Image (part of Oracle Multimedia) was
 *       removed in Oracle 21c (ORA-00902).
 *   <li><b>SI_POSITIONALCOLOR</b> (scenario D): SQL/MM Still Image (part of Oracle Multimedia) was
 *       removed in Oracle 21c (ORA-00902).
 *   <li><b>SI_TEXTURE</b> (scenario D): SQL/MM Still Image (part of Oracle Multimedia) was removed
 *       in Oracle 21c (ORA-00902).
 *   <li><b>SI_FEATURELIST</b> (scenario D): SQL/MM Still Image (part of Oracle Multimedia) was
 *       removed in Oracle 21c (ORA-00902).
 *   <li><b>Expression</b> (scenario D): Oracle Expression Filter (EXF$) was removed; type not
 *       available (ORA-00902).
 *   <li><b>REF CURSOR</b> (scenario D): PL/SQL-only type; cannot be a table column.
 *   <li><b>ASSOCIATIVE ARRAY</b> (scenario D): PL/SQL-only type; cannot be a table column.
 * </ul>
 */
final class OracleDataTypesTestData {

  private OracleDataTypesTestData() {}

  /** A table testing one Oracle column type mapping, with the expected Spanner values. */
  record TypeTable(
      String table, String sourceType, String spannerType, boolean pk, List<String> values) {

    String column() {
      return table.replaceAll("_table$", "_col");
    }
  }

  /** A table for an Oracle type Datastream cannot replicate; only the row count is checked. */
  record UnsupportedTable(String table, String sourceType, String spannerType, int rowCount) {

    String column() {
      return table.replaceAll("_table$", "_col");
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Value lists
  // ---------------------------------------------------------------------------------------------

  private static final List<String> VARCHAR2_VALUES =
      values(
          null,
          " ",
          "DROP TABLE",
          repeat("", "A", 4000),
          "Hello World",
          "a",
          null,
          "こんにちは世界 😀",
          "O'Brien");

  private static final List<String> VARCHAR2_TO_BYTES_VALUES =
      values(
          null,
          "IA==",
          "RFJPUCBUQUJMRQ==",
          repeatBase64("", "A", 4000),
          "SGVsbG8gV29ybGQ=",
          "YQ==",
          null,
          "44GT44KT44Gr44Gh44Gv5LiW55WMIPCfmIA=",
          "TydCcmllbg==");

  private static final List<String> VARCHAR2_PK_VALUES =
      values(" ", "DROP TABLE", repeat("", "A", 4000), "Hello World", "a", "こんにちは世界 😀", "O'Brien");

  private static final List<String> CHAR_VALUES =
      values(
          null,
          repeat("", " ", 2000),
          repeat("DROP TABLE", " ", 1990),
          repeat("", "A", 2000),
          repeat("Hello World", " ", 1989),
          repeat("a", " ", 1999),
          null,
          repeat("こんにちは世界 😀", " ", 1974),
          repeat("O'Brien", " ", 1993));

  private static final List<String> CHAR_TO_BYTES_VALUES =
      values(
          null,
          repeatBase64("", " ", 2000),
          repeatBase64("DROP TABLE", " ", 1990),
          repeatBase64("", "A", 2000),
          repeatBase64("Hello World", " ", 1989),
          repeatBase64("a", " ", 1999),
          null,
          repeatBase64("こんにちは世界 😀", " ", 1974),
          repeatBase64("O'Brien", " ", 1993));

  private static final List<String> CHAR_PK_VALUES =
      values(
          repeat("", " ", 2000),
          repeat("DROP TABLE", " ", 1990),
          repeat("", "A", 2000),
          repeat("Hello World", " ", 1989),
          repeat("a", " ", 1999),
          repeat("こんにちは世界 😀", " ", 1974),
          repeat("O'Brien", " ", 1993));

  private static final List<String> NVARCHAR2_VALUES =
      values(
          null,
          " ",
          "DROP TABLE",
          repeat("", "A", 2000),
          "Hello World",
          "a",
          null,
          "こんにちは世界 😀",
          "O'Brien");

  private static final List<String> NVARCHAR2_TO_BYTES_VALUES =
      values(
          null,
          "IA==",
          "RFJPUCBUQUJMRQ==",
          repeatBase64("", "A", 2000),
          "SGVsbG8gV29ybGQ=",
          "YQ==",
          null,
          "44GT44KT44Gr44Gh44Gv5LiW55WMIPCfmIA=",
          "TydCcmllbg==");

  private static final List<String> NVARCHAR2_PK_VALUES =
      values(" ", "DROP TABLE", repeat("", "A", 2000), "Hello World", "a", "こんにちは世界 😀", "O'Brien");

  private static final List<String> NCHAR_VALUES =
      values(
          null,
          repeat("", " ", 1000),
          repeat("DROP TABLE", " ", 990),
          repeat("", "A", 1000),
          repeat("Hello World", " ", 989),
          repeat("a", " ", 999),
          null,
          repeat("こんにちは世界 😀", " ", 990),
          repeat("O'Brien", " ", 993));

  private static final List<String> NCHAR_TO_BYTES_VALUES =
      values(
          null,
          repeatBase64("", " ", 1000),
          repeatBase64("DROP TABLE", " ", 990),
          repeatBase64("", "A", 1000),
          repeatBase64("Hello World", " ", 989),
          repeatBase64("a", " ", 999),
          null,
          repeatBase64("こんにちは世界 😀", " ", 990),
          repeatBase64("O'Brien", " ", 993));

  private static final List<String> NCHAR_PK_VALUES =
      values(
          repeat("", " ", 1000),
          repeat("DROP TABLE", " ", 990),
          repeat("", "A", 1000),
          repeat("Hello World", " ", 989),
          repeat("a", " ", 999),
          repeat("こんにちは世界 😀", " ", 990),
          repeat("O'Brien", " ", 993));

  /**
   * CSV values 1e38, -1e38, +/-(38 nines) exceed Spanner NUMERIC (29 integer digits) and are tested
   * via FLOAT64/STRING mappings.
   */
  private static final List<String> NUMBER_VALUES =
      values(
          "0",
          null,
          "123.456",
          "99999999999999999999999999999.999999999",
          "-99999999999999999999999999999.999999999",
          "0.000000001",
          "-0.5");

  private static final List<String> NUMBER_TO_FLOAT64_VALUES =
      values("1.0E38", "-1.0E38", "0.0", "1.0E38", "-1.0E38", null, "123.456", "1.0E-9", "-0.5");

  /** Datastream emits Oracle NUMBER as text; small values use scientific notation (e.g. 1E-9). */
  private static final List<String> NUMBER_TO_STR_VALUES =
      values(
          "100000000000000000000000000000000000000",
          "-100000000000000000000000000000000000000",
          "0",
          "99999999999999999999999999999999999999",
          "-99999999999999999999999999999999999999",
          null,
          "123.456",
          "1E-9",
          "-0.5");

  private static final List<String> NUMBER_TO_INT64_VALUES =
      values("0", null, "12345", "-12345", "9223372036854775807", "-9223372036854775808");

  /**
   * Oracle NUMERIC/DECIMAL/DEC without precision = NUMBER(*,0); 123.456 is rounded to 123 by
   * Oracle. CSV values beyond 29 integer digits exceed Spanner NUMERIC.
   */
  private static final List<String> NUMERIC_VALUES =
      values(
          "0",
          null,
          "12345",
          "-12345",
          "123",
          "99999999999999999999999999999",
          "-99999999999999999999999999999");

  private static final List<String> NUMERIC_TO_FLOAT64_VALUES =
      values("1.0E38", "-1.0E38", "0.0", "1.0E38", "-1.0E38", null, "12345.0", "123.0");

  private static final List<String> NUMERIC_TO_STR_VALUES =
      values(
          "100000000000000000000000000000000000000",
          "-100000000000000000000000000000000000000",
          "0",
          "99999999999999999999999999999999999999",
          "-99999999999999999999999999999999999999",
          null,
          "12345",
          "123");

  private static final List<String> NUMERIC_TO_INT64_VALUES =
      values("0", null, "12345", "-12345", "9223372036854775807", "-9223372036854775808", "123");

  private static final List<String> FLOAT_VALUES =
      values("0", null, "3.14159", "-123.456", "0.1", "12345678.5");

  /**
   * CSV values +/-1.79769313486231E308, NaN and Infinity are not representable in Oracle
   * NUMBER-based FLOAT (ORA-01426), so they are skipped; max/min boundary uses +/-1E125 and 1E-130.
   */
  private static final List<String> FLOAT_TO_FLOAT64_VALUES =
      values("1.0E38", "-1.0E38", "0.0", null, "3.14159", "1.0E125", "-1.0E125", "1.0E-130", "0.1");

  /**
   * CSV values +/-1.79769313486231E308, NaN and Infinity are not representable in Oracle
   * NUMBER-based FLOAT (ORA-01426), so they are skipped; max/min boundary uses +/-1E125 and 1E-130.
   */
  private static final List<String> FLOAT_TO_STR_VALUES =
      values("1E+38", "-1E+38", "0", null, "3.14159", "1E+125", "-1E+125", "1E-130", "0.1");

  private static final List<String> FLOAT_TO_INT64_VALUES = values("0", null, "12345", "-12345");

  private static final List<String> BINARY_FLOAT_VALUES =
      values(
          "1.0E38",
          "-1.0E38",
          "0.0",
          null,
          "3.40282E38",
          "-3.40282E38",
          "NaN",
          "Infinity",
          "3.14",
          "-Infinity",
          "1.17549435E-38");

  /** Datastream emits number text (e.g. 1E+38, 0). */
  private static final List<String> BINARY_FLOAT_TO_STR_VALUES =
      values(
          "1E+38",
          "-1E+38",
          "0",
          null,
          "3.40282E+38",
          "-3.40282E+38",
          "NaN",
          "Infinity",
          "3.14",
          "-Infinity",
          "1.17549435E-38");

  /** NaN/Infinity and values beyond 29 integer digits are not representable in Spanner NUMERIC. */
  private static final List<String> BINARY_FLOAT_TO_NUMERIC_VALUES =
      values("0", null, "3.14", "-12.5", "12345");

  private static final List<String> BINARY_DOUBLE_VALUES =
      values(
          "1.0E38",
          "-1.0E38",
          "0.0",
          null,
          "1.79769313486231E308",
          "-1.79769313486231E308",
          "NaN",
          "Infinity",
          "3.141592653589793",
          "-Infinity",
          "2.2250738585072014E-308");

  /** Datastream emits number text (e.g. 1E+38, 0). */
  private static final List<String> BINARY_DOUBLE_TO_STR_VALUES =
      values(
          "1E+38",
          "-1E+38",
          "0",
          null,
          "1.79769313486231E+308",
          "-1.79769313486231E+308",
          "NaN",
          "Infinity",
          "3.141592653589793",
          "-Infinity",
          "2.2250738585072014E-308");

  /** Spanner NUMERIC scale is 9: 3.141592653589793 is rounded (HALF_UP) to 3.141592654. */
  private static final List<String> BINARY_DOUBLE_TO_NUMERIC_VALUES =
      values("0", null, "3.141592654", "-12.5", "12345");

  private static final List<String> INTEGER_TO_NUMERIC_VALUES =
      values("0", null, "12345", "99999999999999999999999999999", "-99999999999999999999999999999");

  private static final List<String> INTEGER_TO_STR_VALUES =
      values(
          "100000000000000000000000000000000000000",
          "-100000000000000000000000000000000000000",
          "0",
          null,
          "99999999999999999999999999999999999999",
          "12345");

  private static final List<String> INTEGER_TO_FLOAT64_VALUES =
      values("1.0E38", "-1.0E38", "0.0", null, "1.0E38", "12345.0");

  private static final List<String> INTEGER_PK_VALUES =
      values("0", "12345", "-12345", "9223372036854775807", "-9223372036854775808");

  private static final List<String> DATE_VALUES =
      values(
          "0001-01-01T00:00:00Z",
          "9999-12-31T23:59:59Z",
          null,
          "2024-01-15T10:30:45Z",
          "2024-02-29T23:59:59Z",
          "1970-01-01T00:00:00Z");

  private static final List<String> DATE_TO_DATE_VALUES =
      values("0001-01-01", "9999-12-31", null, "2024-01-15", "2024-02-29", "1970-01-01");

  /** Expected value assumes epoch microseconds. */
  private static final List<String> DATE_TO_INT64_VALUES =
      values(
          "-62135596800000000",
          "253402300799000000",
          null,
          "1705314645000000",
          "1709251199000000",
          "0");

  private static final List<String> DATE_PK_VALUES =
      values(
          "0001-01-01T00:00:00Z",
          "9999-12-31T23:59:59Z",
          "2024-01-15T10:30:45Z",
          "2024-02-29T23:59:59Z",
          "1970-01-01T00:00:00Z");

  private static final List<String> TIMESTAMP_VALUES =
      values(
          "0001-01-01T00:00:00Z",
          "9999-12-31T23:59:59Z",
          null,
          "2024-01-15T10:30:45.123456Z",
          "2024-02-29T12:00:00Z",
          "9999-12-31T23:59:59.999999Z",
          "1970-01-01T00:00:00Z");

  /** Expected value assumes epoch microseconds. */
  private static final List<String> TIMESTAMP_TO_INT64_VALUES =
      values(
          "-62135596800000000",
          "253402300799000000",
          null,
          "1705314645123456",
          "1709208000000000",
          "253402300799999999",
          "0");

  private static final List<String> TIMESTAMP_PK_VALUES =
      values(
          "0001-01-01T00:00:00Z",
          "9999-12-31T23:59:59Z",
          "2024-01-15T10:30:45.123456Z",
          "2024-02-29T12:00:00Z",
          "9999-12-31T23:59:59.999999Z",
          "1970-01-01T00:00:00Z");

  private static final List<String> TIMESTAMP_TZ_VALUES =
      values(
          "1700-01-01T00:00:00Z",
          "2200-12-31T23:59:59.999999Z",
          null,
          "2024-01-15T10:30:45.123456Z",
          "2024-02-29T23:00:00Z",
          "2024-07-01T12:00:00Z");

  /** Expected value assumes epoch microseconds (UTC). */
  private static final List<String> TIMESTAMP_TZ_TO_INT64_VALUES =
      values(
          "-8520336000000000",
          "7289654399999999",
          null,
          "1705314645123456",
          "1709247600000000",
          "1719835200000000");

  private static final List<String> TIMESTAMP_LTZ_PK_VALUES =
      values(
          "1700-01-01T00:00:00Z",
          "2200-12-31T23:59:59.999999Z",
          "2024-01-15T10:30:45.123456Z",
          "2024-02-29T23:00:00Z",
          "2024-07-01T12:00:00Z");

  private static final List<String> RAW_VALUES =
      values(
          null,
          "IA==",
          "QQ==",
          "RFJPUCBUQUJMRQ==",
          "3q2+7w==",
          hexBase64("FF", 2000),
          "AA==",
          null);

  /**
   * Matrix lists STRING (Base64); Datastream emits RAW as uppercase hex and the template stores it
   * verbatim (same convention as MySQL binary -> STRING).
   */
  private static final List<String> RAW_TO_STR_VALUES =
      values(
          null, "20", "41", "44524F50205441424C45", "DEADBEEF", repeat("", "F", 4000), "00", null);

  private static final List<String> RAW_PK_VALUES =
      values("IA==", "QQ==", "RFJPUCBUQUJMRQ==", "3q2+7w==", hexBase64("FF", 2000), "AA==");

  private static final List<String> BLOB_VALUES =
      values("", repeatBase64("", "A", 100000), null, "3q2+7w==", "AA==");

  private static final List<String> CLOB_VALUES =
      values("", repeat("", "A", 100000), null, "Hello CLOB", "こんにちは世界 😀", "O'Brien");

  private static final List<String> CLOB_TO_BYTES_VALUES =
      values(
          "",
          repeatBase64("", "A", 100000),
          null,
          "SGVsbG8gQ0xPQg==",
          "44GT44KT44Gr44Gh44Gv5LiW55WMIPCfmIA=",
          "TydCcmllbg==");

  // ---------------------------------------------------------------------------------------------
  // Tables
  // ---------------------------------------------------------------------------------------------

  /** Type mapping tables (scenarios A, B and C). */
  static List<TypeTable> tables() {
    return List.of(
        table("varchar2_table", "VARCHAR2(4000)", "STRING(MAX)", VARCHAR2_VALUES),
        table("varchar2_to_str4000_table", "VARCHAR2(4000)", "STRING(4000)", VARCHAR2_VALUES),
        table("varchar2_to_bytes_table", "VARCHAR2(4000)", "BYTES(MAX)", VARCHAR2_TO_BYTES_VALUES),
        pk("varchar2_pk_table", "VARCHAR2(4000)", "STRING(MAX)", VARCHAR2_PK_VALUES),
        table("varchar_table", "VARCHAR(4000)", "STRING(MAX)", VARCHAR2_VALUES),
        table("varchar_to_str4000_table", "VARCHAR(4000)", "STRING(4000)", VARCHAR2_VALUES),
        table("varchar_to_bytes_table", "VARCHAR(4000)", "BYTES(MAX)", VARCHAR2_TO_BYTES_VALUES),
        pk("varchar_pk_table", "VARCHAR(4000)", "STRING(MAX)", VARCHAR2_PK_VALUES),
        table("char_table", "CHAR(2000)", "STRING(MAX)", CHAR_VALUES),
        table("char_to_str2000_table", "CHAR(2000)", "STRING(2000)", CHAR_VALUES),
        table("char_to_bytes_table", "CHAR(2000)", "BYTES(MAX)", CHAR_TO_BYTES_VALUES),
        pk("char_pk_table", "CHAR(2000)", "STRING(MAX)", CHAR_PK_VALUES),
        table("character_table", "CHARACTER(2000)", "STRING(MAX)", CHAR_VALUES),
        table("character_to_str2000_table", "CHARACTER(2000)", "STRING(2000)", CHAR_VALUES),
        table("character_to_bytes_table", "CHARACTER(2000)", "BYTES(MAX)", CHAR_TO_BYTES_VALUES),
        pk("character_pk_table", "CHARACTER(2000)", "STRING(MAX)", CHAR_PK_VALUES),
        table("nvarchar2_table", "NVARCHAR2(2000)", "STRING(MAX)", NVARCHAR2_VALUES),
        table("nvarchar2_to_str2000_table", "NVARCHAR2(2000)", "STRING(2000)", NVARCHAR2_VALUES),
        table(
            "nvarchar2_to_bytes_table", "NVARCHAR2(2000)", "BYTES(MAX)", NVARCHAR2_TO_BYTES_VALUES),
        pk("nvarchar2_pk_table", "NVARCHAR2(2000)", "STRING(MAX)", NVARCHAR2_PK_VALUES),
        table("nchar_table", "NCHAR(1000)", "STRING(MAX)", NCHAR_VALUES),
        table("nchar_to_str1000_table", "NCHAR(1000)", "STRING(1000)", NCHAR_VALUES),
        table("nchar_to_bytes_table", "NCHAR(1000)", "BYTES(MAX)", NCHAR_TO_BYTES_VALUES),
        pk("nchar_pk_table", "NCHAR(1000)", "STRING(MAX)", NCHAR_PK_VALUES),
        table("nchar_varying_table", "NCHAR VARYING(2000)", "STRING(MAX)", NVARCHAR2_VALUES),
        table(
            "nchar_varying_to_str2000_table",
            "NCHAR VARYING(2000)",
            "STRING(2000)",
            NVARCHAR2_VALUES),
        table(
            "nchar_varying_to_bytes_table",
            "NCHAR VARYING(2000)",
            "BYTES(MAX)",
            NVARCHAR2_TO_BYTES_VALUES),
        pk("nchar_varying_pk_table", "NCHAR VARYING(2000)", "STRING(MAX)", NVARCHAR2_PK_VALUES),
        table("nat_character_table", "NATIONAL CHARACTER(1000)", "STRING(MAX)", NCHAR_VALUES),
        table(
            "nat_character_to_str1000_table",
            "NATIONAL CHARACTER(1000)",
            "STRING(1000)",
            NCHAR_VALUES),
        table(
            "nat_character_to_bytes_table",
            "NATIONAL CHARACTER(1000)",
            "BYTES(MAX)",
            NCHAR_TO_BYTES_VALUES),
        pk("nat_character_pk_table", "NATIONAL CHARACTER(1000)", "STRING(MAX)", NCHAR_PK_VALUES),
        table("nat_char_table", "NATIONAL CHAR(1000)", "STRING(MAX)", NCHAR_VALUES),
        table("nat_char_to_str1000_table", "NATIONAL CHAR(1000)", "STRING(1000)", NCHAR_VALUES),
        table(
            "nat_char_to_bytes_table", "NATIONAL CHAR(1000)", "BYTES(MAX)", NCHAR_TO_BYTES_VALUES),
        pk("nat_char_pk_table", "NATIONAL CHAR(1000)", "STRING(MAX)", NCHAR_PK_VALUES),
        table(
            "nat_characvar_table",
            "NATIONAL CHARACTER VARYING(2000)",
            "STRING(MAX)",
            NVARCHAR2_VALUES),
        table(
            "nat_characvar_to_str2000_table",
            "NATIONAL CHARACTER VARYING(2000)",
            "STRING(2000)",
            NVARCHAR2_VALUES),
        table(
            "nat_characvar_to_bytes_table",
            "NATIONAL CHARACTER VARYING(2000)",
            "BYTES(MAX)",
            NVARCHAR2_TO_BYTES_VALUES),
        pk(
            "nat_characvar_pk_table",
            "NATIONAL CHARACTER VARYING(2000)",
            "STRING(MAX)",
            NVARCHAR2_PK_VALUES),
        table("nat_charvar_table", "NATIONAL CHAR VARYING(2000)", "STRING(MAX)", NVARCHAR2_VALUES),
        table(
            "nat_charvar_to_str2000_table",
            "NATIONAL CHAR VARYING(2000)",
            "STRING(2000)",
            NVARCHAR2_VALUES),
        table(
            "nat_charvar_to_bytes_table",
            "NATIONAL CHAR VARYING(2000)",
            "BYTES(MAX)",
            NVARCHAR2_TO_BYTES_VALUES),
        pk(
            "nat_charvar_pk_table",
            "NATIONAL CHAR VARYING(2000)",
            "STRING(MAX)",
            NVARCHAR2_PK_VALUES),
        table("number_table", "NUMBER", "NUMERIC", NUMBER_VALUES),
        table("number_to_float64_table", "NUMBER", "FLOAT64", NUMBER_TO_FLOAT64_VALUES),
        table("number_to_str_table", "NUMBER", "STRING(MAX)", NUMBER_TO_STR_VALUES),
        // Only integral values within INT64 range are inserted.
        table("number_to_int64_table", "NUMBER", "INT64", NUMBER_TO_INT64_VALUES),
        table("numeric_table", "NUMERIC", "NUMERIC", NUMERIC_VALUES),
        table("numeric_to_float64_table", "NUMERIC", "FLOAT64", NUMERIC_TO_FLOAT64_VALUES),
        table("numeric_to_str_table", "NUMERIC", "STRING(MAX)", NUMERIC_TO_STR_VALUES),
        table("numeric_to_int64_table", "NUMERIC", "INT64", NUMERIC_TO_INT64_VALUES),
        table("decimal_table", "DECIMAL", "NUMERIC", NUMERIC_VALUES),
        table("decimal_to_float64_table", "DECIMAL", "FLOAT64", NUMERIC_TO_FLOAT64_VALUES),
        table("decimal_to_str_table", "DECIMAL", "STRING(MAX)", NUMERIC_TO_STR_VALUES),
        table("decimal_to_int64_table", "DECIMAL", "INT64", NUMERIC_TO_INT64_VALUES),
        table("dec_table", "DEC", "NUMERIC", NUMERIC_VALUES),
        table("dec_to_float64_table", "DEC", "FLOAT64", NUMERIC_TO_FLOAT64_VALUES),
        table("dec_to_str_table", "DEC", "STRING(MAX)", NUMERIC_TO_STR_VALUES),
        table("dec_to_int64_table", "DEC", "INT64", NUMERIC_TO_INT64_VALUES),
        // CSV values +/-1.79769313486231E308, NaN and Infinity are not representable in Oracle
        // NUMBER-based FLOAT (ORA-01426), so they are skipped; max/min boundary uses +/-1E125 and
        // 1E-130. Values exceeding 29 integer digits are excluded for NUMERIC.
        table("float_table", "FLOAT", "NUMERIC", FLOAT_VALUES),
        table("float_to_float64_table", "FLOAT", "FLOAT64", FLOAT_TO_FLOAT64_VALUES),
        table("float_to_str_table", "FLOAT", "STRING(MAX)", FLOAT_TO_STR_VALUES),
        // CSV values +/-1.79769313486231E308, NaN and Infinity are not representable in Oracle
        // NUMBER-based FLOAT (ORA-01426), so they are skipped; max/min boundary uses +/-1E125 and
        // 1E-130.
        table("float_to_int64_table", "FLOAT", "INT64", FLOAT_TO_INT64_VALUES),
        table("double_prec_table", "DOUBLE PRECISION", "FLOAT64", FLOAT_TO_FLOAT64_VALUES),
        // CSV values +/-1.79769313486231E308, NaN and Infinity are not representable in Oracle
        // NUMBER-based FLOAT (ORA-01426), so they are skipped; max/min boundary uses +/-1E125 and
        // 1E-130.
        table("double_prec_to_numeric_table", "DOUBLE PRECISION", "NUMERIC", FLOAT_VALUES),
        table("double_prec_to_str_table", "DOUBLE PRECISION", "STRING(MAX)", FLOAT_TO_STR_VALUES),
        // CSV values +/-1.79769313486231E308, NaN and Infinity are not representable in Oracle
        // NUMBER-based FLOAT (ORA-01426), so they are skipped; max/min boundary uses +/-1E125 and
        // 1E-130.
        table("double_prec_to_int64_table", "DOUBLE PRECISION", "INT64", FLOAT_TO_INT64_VALUES),
        table("real_table", "REAL", "FLOAT64", FLOAT_TO_FLOAT64_VALUES),
        // CSV values +/-1.79769313486231E308, NaN and Infinity are not representable in Oracle
        // NUMBER-based FLOAT (ORA-01426), so they are skipped; max/min boundary uses +/-1E125 and
        // 1E-130.
        table("real_to_numeric_table", "REAL", "NUMERIC", FLOAT_VALUES),
        table("real_to_str_table", "REAL", "STRING(MAX)", FLOAT_TO_STR_VALUES),
        // CSV values +/-1.79769313486231E308, NaN and Infinity are not representable in Oracle
        // NUMBER-based FLOAT (ORA-01426), so they are skipped; max/min boundary uses +/-1E125 and
        // 1E-130.
        table("real_to_int64_table", "REAL", "INT64", FLOAT_TO_INT64_VALUES),
        table("binary_float_table", "BINARY_FLOAT", "FLOAT32", BINARY_FLOAT_VALUES),
        table("binary_float_to_float64_table", "BINARY_FLOAT", "FLOAT64", BINARY_FLOAT_VALUES),
        table(
            "binary_float_to_str_table", "BINARY_FLOAT", "STRING(MAX)", BINARY_FLOAT_TO_STR_VALUES),
        table(
            "binary_float_to_numeric_table",
            "BINARY_FLOAT",
            "NUMERIC",
            BINARY_FLOAT_TO_NUMERIC_VALUES),
        table("binary_float_to_int64_table", "BINARY_FLOAT", "INT64", FLOAT_TO_INT64_VALUES),
        table("binary_double_table", "BINARY_DOUBLE", "FLOAT64", BINARY_DOUBLE_VALUES),
        table(
            "binary_double_to_str_table",
            "BINARY_DOUBLE",
            "STRING(MAX)",
            BINARY_DOUBLE_TO_STR_VALUES),
        table(
            "binary_double_to_numeric_table",
            "BINARY_DOUBLE",
            "NUMERIC",
            BINARY_DOUBLE_TO_NUMERIC_VALUES),
        table("binary_double_to_int64_table", "BINARY_DOUBLE", "INT64", FLOAT_TO_INT64_VALUES),
        // CSV values 1e38, -1e38 and 38 nines exceed INT64 and are tested via STRING/FLOAT64
        // mappings.
        table("integer_table", "INTEGER", "INT64", NUMBER_TO_INT64_VALUES),
        table("integer_to_numeric_table", "INTEGER", "NUMERIC", INTEGER_TO_NUMERIC_VALUES),
        table("integer_to_str_table", "INTEGER", "STRING(MAX)", INTEGER_TO_STR_VALUES),
        table("integer_to_float64_table", "INTEGER", "FLOAT64", INTEGER_TO_FLOAT64_VALUES),
        pk("integer_pk_table", "INTEGER", "INT64", INTEGER_PK_VALUES),
        // CSV values 1e38, -1e38 and 38 nines exceed INT64 and are tested via STRING/FLOAT64
        // mappings.
        table("int_table", "INT", "INT64", NUMBER_TO_INT64_VALUES),
        table("int_to_numeric_table", "INT", "NUMERIC", INTEGER_TO_NUMERIC_VALUES),
        table("int_to_str_table", "INT", "STRING(MAX)", INTEGER_TO_STR_VALUES),
        table("int_to_float64_table", "INT", "FLOAT64", INTEGER_TO_FLOAT64_VALUES),
        pk("int_pk_table", "INT", "INT64", INTEGER_PK_VALUES),
        // CSV values 1e38, -1e38 and 38 nines exceed INT64 and are tested via STRING/FLOAT64
        // mappings.
        table("smallint_table", "SMALLINT", "INT64", NUMBER_TO_INT64_VALUES),
        table("smallint_to_numeric_table", "SMALLINT", "NUMERIC", INTEGER_TO_NUMERIC_VALUES),
        table("smallint_to_str_table", "SMALLINT", "STRING(MAX)", INTEGER_TO_STR_VALUES),
        table("smallint_to_float64_table", "SMALLINT", "FLOAT64", INTEGER_TO_FLOAT64_VALUES),
        pk("smallint_pk_table", "SMALLINT", "INT64", INTEGER_PK_VALUES),
        table("date_table", "DATE", "TIMESTAMP", DATE_VALUES),
        table("date_to_date_table", "DATE", "DATE", DATE_TO_DATE_VALUES),
        table("date_to_str_table", "DATE", "STRING(MAX)", DATE_VALUES),
        table("date_to_int64_table", "DATE", "INT64", DATE_TO_INT64_VALUES),
        pk("date_pk_table", "DATE", "TIMESTAMP", DATE_PK_VALUES),
        table("timestamp_table", "TIMESTAMP", "TIMESTAMP", TIMESTAMP_VALUES),
        table("timestamp_to_str_table", "TIMESTAMP", "STRING(MAX)", TIMESTAMP_VALUES),
        table("timestamp_to_int64_table", "TIMESTAMP", "INT64", TIMESTAMP_TO_INT64_VALUES),
        pk("timestamp_pk_table", "TIMESTAMP", "TIMESTAMP", TIMESTAMP_PK_VALUES),
        table("timestamp_tz_table", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP", TIMESTAMP_TZ_VALUES),
        table(
            "timestamp_tz_to_str_table",
            "TIMESTAMP WITH TIME ZONE",
            "STRING(MAX)",
            TIMESTAMP_TZ_VALUES),
        table(
            "timestamp_tz_to_int64_table",
            "TIMESTAMP WITH TIME ZONE",
            "INT64",
            TIMESTAMP_TZ_TO_INT64_VALUES),
        table(
            "timestamp_ltz_table",
            "TIMESTAMP WITH LOCAL TIME ZONE",
            "TIMESTAMP",
            TIMESTAMP_TZ_VALUES),
        table(
            "timestamp_ltz_to_str_table",
            "TIMESTAMP WITH LOCAL TIME ZONE",
            "STRING(MAX)",
            TIMESTAMP_TZ_VALUES),
        table(
            "timestamp_ltz_to_int64_table",
            "TIMESTAMP WITH LOCAL TIME ZONE",
            "INT64",
            TIMESTAMP_TZ_TO_INT64_VALUES),
        pk(
            "timestamp_ltz_pk_table",
            "TIMESTAMP WITH LOCAL TIME ZONE",
            "TIMESTAMP",
            TIMESTAMP_LTZ_PK_VALUES),
        table("raw_table", "RAW", "BYTES(MAX)", RAW_VALUES),
        table("raw_to_bytes2000_table", "RAW", "BYTES(2000)", RAW_VALUES),
        table("raw_to_str_table", "RAW", "STRING(MAX)", RAW_TO_STR_VALUES),
        pk("raw_pk_table", "RAW", "BYTES(MAX)", RAW_PK_VALUES),
        table("blob_table", "BLOB", "BYTES(MAX)", BLOB_VALUES),
        // Matrix alternative: STRING(MAX) (Base64).
        table("blob_to_str_table", "BLOB", "STRING(MAX)", BLOB_VALUES),
        table("clob_table", "CLOB", "STRING(MAX)", CLOB_VALUES),
        table("clob_to_bytes_table", "CLOB", "BYTES(MAX)", CLOB_TO_BYTES_VALUES),
        table("nclob_table", "NCLOB", "STRING(MAX)", CLOB_VALUES),
        table("nclob_to_bytes_table", "NCLOB", "BYTES(MAX)", CLOB_TO_BYTES_VALUES));
  }

  /** Tables of types Datastream cannot replicate (scenario D): only the row count is checked. */
  static List<UnsupportedTable> unsupportedTables() {
    return List.of(
        // CSV value '+999999999 23:59:59.999999999' is a DAY TO SECOND literal and is tested in
        // interval_day_to_second_table.
        unsupported("interval_year_to_month_table", "INTERVAL YEAR TO MONTH", "STRING(MAX)", 3),
        unsupported("interval_day_to_second_table", "INTERVAL DAY TO SECOND", "STRING(MAX)", 3),
        // CSV value 'A'*100000 cannot be expressed as a SQL literal for LONG RAW; a 1-byte value
        // is used.
        unsupported("long_raw_table", "LONG RAW", "BYTES(MAX)", 3),
        unsupported("bfile_table", "BFILE", "STRING(MAX)", 2),
        // CSV value 'A'*100000 exceeds SQL literal limits; 4000 characters are used.
        unsupported("long_table", "LONG", "STRING(MAX)", 3),
        unsupported("rowid_table", "ROWID", "STRING(MAX)", 2),
        unsupported("urowid_table", "UROWID", "STRING(MAX)", 2),
        // CSV value '<bad_xml' is rejected by Oracle (not well-formed XML) and is skipped.
        unsupported("xmltype_table", "XMLType", "STRING(MAX)", 2),
        unsupported("anydata_table", "ANYDATA", "BYTES(MAX)", 2),
        // CSV value ANYDATA.ConvertNumber(5) is not an ANYTYPE; only NULL is inserted.
        unsupported("anytype_table", "ANYTYPE", "BYTES(MAX)", 1),
        // CSV value ANYDATA.ConvertNumber(5) is not an ANYDATASET; only NULL is inserted.
        unsupported("anydataset_table", "ANYDATASET", "BYTES(MAX)", 1),
        unsupported("uritype_table", "URIType", "STRING(MAX)", 2),
        // URIFactory.getURI('http://test.com') returns an HTTPURITYPE; a DBURITYPE value is used
        // instead.
        unsupported("dburitype_table", "DBUriType", "STRING(MAX)", 2),
        // URIFactory.getURI('http://test.com') returns an HTTPURITYPE; an XDBURITYPE value is used
        // instead.
        unsupported("xdburitype_table", "XDBUriType", "STRING(MAX)", 2),
        unsupported("httpuritype_table", "HttpUriType", "STRING(MAX)", 2),
        // MLSLABEL values require Trusted Oracle labels; only NULL is inserted.
        unsupported("mlslabel_table", "MLSLABEL", "STRING(MAX)", 1),
        unsupported("ref_table", "REF", "STRING(MAX)", 2),
        unsupported("varray_table", "VARRAY", "ARRAY<STRING(MAX)>", 2),
        unsupported("nested_table_table", "NESTED TABLE", "JSON", 2),
        unsupported("object_type_table", "OBJECT TYPE", "JSON", 2));
  }

  /**
   * Type mapping tables of the PostgreSQL-dialect test: the same tables, rows and expected values
   * as {@link #tables()}, with the PostgreSQL equivalent of each Spanner type, as created by {@code
   * oracle-POSTGRESQL-spanner-schema.sql}.
   */
  static List<TypeTable> postgresTables() {
    return tables().stream()
        .map(
            t ->
                new TypeTable(
                    t.table(), t.sourceType(), postgresType(t.spannerType()), t.pk(), t.values()))
        .toList();
  }

  /** Unsupported-type tables of the PostgreSQL-dialect test, see {@link #postgresTables()}. */
  static List<UnsupportedTable> postgresUnsupportedTables() {
    return unsupportedTables().stream()
        .map(
            t ->
                new UnsupportedTable(
                    t.table(), t.sourceType(), postgresType(t.spannerType()), t.rowCount()))
        .toList();
  }

  /**
   * The PostgreSQL-dialect equivalent of a GoogleSQL Spanner column type. PostgreSQL {@code bytea}
   * has no length, so {@code BYTES(n)} maps to {@code bytea}.
   */
  private static String postgresType(String googleSqlType) {
    Matcher string = Pattern.compile("STRING\\((MAX|\\d+)\\)").matcher(googleSqlType);
    if (string.matches()) {
      return string.group(1).equals("MAX") ? "varchar" : "varchar(" + string.group(1) + ")";
    }
    if (googleSqlType.matches("BYTES\\((MAX|\\d+)\\)")) {
      return "bytea";
    }
    switch (googleSqlType) {
      case "INT64":
        return "bigint";
      case "FLOAT64":
        return "float8";
      case "FLOAT32":
        return "float4";
      case "NUMERIC":
        return "numeric";
      case "DATE":
        return "date";
      case "TIMESTAMP":
        return "timestamptz";
      case "JSON":
        return "jsonb";
      case "ARRAY<STRING(MAX)>":
        return "varchar[]";
      default:
        throw new IllegalArgumentException("No PostgreSQL type for " + googleSqlType);
    }
  }

  private static TypeTable table(
      String table, String sourceType, String spannerType, List<String> values) {
    return new TypeTable(table, sourceType, spannerType, false, values);
  }

  private static TypeTable pk(
      String table, String sourceType, String spannerType, List<String> values) {
    return new TypeTable(table, sourceType, spannerType, true, values);
  }

  private static UnsupportedTable unsupported(
      String table, String sourceType, String spannerType, int rowCount) {
    return new UnsupportedTable(table, sourceType, spannerType, rowCount);
  }

  /** The expected values of a table; {@code null} is SQL NULL. */
  private static List<String> values(String... values) {
    return Arrays.asList(values);
  }

  /** {@code prefix} followed by {@code unit} repeated {@code times} times. */
  private static String repeat(String prefix, String unit, int times) {
    return prefix + unit.repeat(times);
  }

  /** Base64 of the UTF-8 bytes of {@link #repeat(String, String, int)}, for BYTES columns. */
  private static String repeatBase64(String prefix, String unit, int times) {
    return base64(repeat(prefix, unit, times).getBytes(StandardCharsets.UTF_8));
  }

  /** Base64 of the bytes given by {@code hex} repeated {@code times} times. */
  private static String hexBase64(String hex, int times) {
    return base64(HexFormat.of().parseHex(hex.repeat(times)));
  }

  private static String base64(byte[] bytes) {
    return Base64.getEncoder().encodeToString(bytes);
  }
}
