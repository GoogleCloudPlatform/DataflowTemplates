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

/**
 * Test data for {@link SpannerToOracleDataTypesIT} (GoogleSQL) and {@link
 * SpannerToOracleDataTypesPGDialectIT} (PostgreSQL dialect), derived from the Oracle data type
 * mapping matrix.
 *
 * <p>Each {@link TypeTable} tests one Oracle column type mapping: its rows are written to Spanner
 * with the table's Spanner type and the replicated Oracle values are asserted. Value lists are
 * shared between tables (and dialects) through the {@code *_VALUES} constants. Non-key tables use
 * ids {@code 1..n} and every table has a single data column, named after the table with the {@code
 * _table} suffix replaced by {@code _col} (e.g. {@code char_col} in {@code char_table}). {@link
 * UnsupportedTable}s cover Oracle types the reverse generator cannot write: only the row count is
 * checked.
 *
 * <p>Types that are not tested:
 *
 * <ul>
 *   <li><b>TIMESTAMP WITH TIME ZONE</b> (scenario C): Oracle ORA-02329: TIMESTAMP WITH TIME ZONE
 *       cannot be a primary key.
 *   <li><b>BOOLEAN</b> (scenario A/B/C): Native BOOLEAN column type requires Oracle 23ai; the test
 *       container is Oracle XE 21c (ORA-00902).
 *   <li><b>(all)</b> (scenario E): Oracle has no scalar array column types; VARRAY/NESTED TABLE are
 *       covered in scenario D.
 *   <li><b>SDO_GEOMETRY / SDO_TOPO_GEOMETRY / SDO_GEORASTER</b> (scenario D): Oracle
 *       Spatial/Locator types are not installed in gvenzl/oracle-xe:21-slim (ORA-00902).
 *   <li><b>VECTOR</b> (scenario D): Requires Oracle 23ai (ORA-00902 on 21c).
 *   <li><b>ORDAUDIO / ORDIMAGE / ORDVIDEO / ORDDOC</b> (scenario D): Oracle Multimedia (ORDSYS) was
 *       removed in Oracle 21c (ORA-00902).
 *   <li><b>SI_STILLIMAGE / SI_COLOR / SI_AVERAGECOLOR / SI_COLORHISTOGRAM / SI_POSITIONALCOLOR /
 *       SI_TEXTURE / SI_FEATURELIST</b> (scenario D): SQL/MM Still Image (Oracle Multimedia) was
 *       removed in Oracle 21c (ORA-00902).
 *   <li><b>Expression</b> (scenario D): Oracle Expression Filter (EXF$) is not available
 *       (ORA-00902).
 *   <li><b>REF CURSOR</b> (scenario D): PL/SQL-only type; cannot be a table column.
 *   <li><b>ASSOCIATIVE ARRAY</b> (scenario D): PL/SQL-only type; cannot be a table column.
 *   <li><b>JSON</b>: Native JSON column type: excluded from these tests for now.
 *   <li><b>CLOB / NCLOB alternatives</b> (scenario B): The matrix lists no Spanner PostgreSQL
 *       alternative for CLOB/NCLOB.
 *   <li><b>RAW -&gt; BYTEA(length)</b> (scenario B): Spanner PostgreSQL has no length-limited
 *       BYTEA; the matrix lists only VARCHAR (Base64).
 * </ul>
 */
final class OracleDataTypesTestData {

  private OracleDataTypesTestData() {}

  /** How a replicated Oracle value is read back and compared with the expected value. */
  enum Compare {
    STRING,
    NUMBER,
    FLOAT,
    DOUBLE,
    BYTES
  }

  /** A value written to Spanner and the value expected in Oracle; {@code null} is SQL NULL. */
  record Row(String spanner, String expected) {}

  /** A table testing one Oracle column type mapping. */
  record TypeTable(
      String table,
      String sourceType,
      String spannerType,
      boolean pk,
      Compare compare,
      String readTemplate,
      List<Row> rows) {

    String column() {
      return table.replaceAll("_table$", "_col");
    }

    /** SQL expression reading the column, e.g. {@code "col"} or {@code TO_CHAR("col", ...)}. */
    String readExpr() {
      return String.format(readTemplate, "\"" + column() + "\"");
    }
  }

  /**
   * A table for an Oracle type the reverse generator cannot write; only the row count is checked.
   */
  record UnsupportedTable(
      String table, String sourceType, String spannerType, List<String> values) {

    String column() {
      return table.replaceAll("_table$", "_col");
    }
  }

  private static final String COLUMN = "%s";
  private static final String DATE_TEXT = "TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS')";
  private static final String TIMESTAMP_TEXT = "TO_CHAR(%s, 'YYYY-MM-DD HH24:MI:SS.FF6')";
  private static final String TIMESTAMP_UTC_TEXT =
      "TO_CHAR(SYS_EXTRACT_UTC(%s), 'YYYY-MM-DD HH24:MI:SS.FF6')";

  private static final Compare STRING = Compare.STRING;
  private static final Compare NUMBER = Compare.NUMBER;
  private static final Compare FLOAT = Compare.FLOAT;
  private static final Compare DOUBLE = Compare.DOUBLE;
  private static final Compare BYTES = Compare.BYTES;

  // ---------------------------------------------------------------------------------------------
  // Value lists
  // ---------------------------------------------------------------------------------------------

  /**
   * Oracle stores the empty string as NULL. The longest value uses the column's maximum length
   * (MAX_STRING_SIZE=STANDARD) instead of 32767 characters.
   */
  private static final List<Row> VARCHAR2_VALUES =
      List.of(
          row("", null),
          same(" "),
          same("DROP TABLE"),
          same(repeat("", "A", 4000)),
          same("Hello World"),
          same("a"),
          same(null),
          same("こんにちは世界 😀"),
          same("O'Brien"));

  /**
   * Spanner BYTES (PostgreSQL: bytea) values hold the UTF-8 bytes of the text; the expected Oracle
   * value is the decoded text.
   */
  private static final List<Row> VARCHAR2_TO_BYTES_VALUES =
      List.of(
          row("", null),
          row("IA==", " "),
          row("RFJPUCBUQUJMRQ==", "DROP TABLE"),
          row(repeatBase64("", "A", 4000), repeat("", "A", 4000)),
          row("SGVsbG8gV29ybGQ=", "Hello World"),
          row("YQ==", "a"),
          same(null),
          row("44GT44KT44Gr44Gh44Gv5LiW55WMIPCfmIA=", "こんにちは世界 😀"),
          row("TydCcmllbg==", "O'Brien"));

  private static final List<Row> VARCHAR2_PK_VALUES =
      List.of(
          same(" "),
          same("DROP TABLE"),
          same(repeat("", "A", 4000)),
          same("Hello World"),
          same("a"),
          same("こんにちは世界 😀"),
          same("O'Brien"));

  /**
   * Oracle stores the empty string as NULL. The longest value uses the column's maximum length
   * (MAX_STRING_SIZE=STANDARD) instead of 32767 characters.
   */
  private static final List<Row> CHAR_VALUES =
      List.of(
          row("", null),
          row(" ", repeat("", " ", 2000)),
          row("DROP TABLE", repeat("DROP TABLE", " ", 1990)),
          same(repeat("", "A", 2000)),
          row("Hello World", repeat("Hello World", " ", 1989)),
          row("a", repeat("a", " ", 1999)),
          same(null),
          row("こんにちは世界 😀", repeat("こんにちは世界 😀", " ", 1974)),
          row("O'Brien", repeat("O'Brien", " ", 1993)));

  /**
   * Spanner BYTES (PostgreSQL: bytea) values hold the UTF-8 bytes of the text; the expected Oracle
   * value is the decoded text.
   */
  private static final List<Row> CHAR_TO_BYTES_VALUES =
      List.of(
          row("", null),
          row("IA==", repeat("", " ", 2000)),
          row("RFJPUCBUQUJMRQ==", repeat("DROP TABLE", " ", 1990)),
          row(repeatBase64("", "A", 2000), repeat("", "A", 2000)),
          row("SGVsbG8gV29ybGQ=", repeat("Hello World", " ", 1989)),
          row("YQ==", repeat("a", " ", 1999)),
          same(null),
          row("44GT44KT44Gr44Gh44Gv5LiW55WMIPCfmIA=", repeat("こんにちは世界 😀", " ", 1974)),
          row("TydCcmllbg==", repeat("O'Brien", " ", 1993)));

  private static final List<Row> CHAR_PK_VALUES =
      List.of(
          row(" ", repeat("", " ", 2000)),
          row("DROP TABLE", repeat("DROP TABLE", " ", 1990)),
          same(repeat("", "A", 2000)),
          row("Hello World", repeat("Hello World", " ", 1989)),
          row("a", repeat("a", " ", 1999)),
          row("こんにちは世界 😀", repeat("こんにちは世界 😀", " ", 1974)),
          row("O'Brien", repeat("O'Brien", " ", 1993)));

  /**
   * Oracle stores the empty string as NULL. The longest value uses the column's maximum length
   * (MAX_STRING_SIZE=STANDARD) instead of 32767 characters.
   */
  private static final List<Row> NVARCHAR2_VALUES =
      List.of(
          row("", null),
          same(" "),
          same("DROP TABLE"),
          same(repeat("", "A", 2000)),
          same("Hello World"),
          same("a"),
          same(null),
          same("こんにちは世界 😀"),
          same("O'Brien"));

  /**
   * Spanner BYTES (PostgreSQL: bytea) values hold the UTF-8 bytes of the text; the expected Oracle
   * value is the decoded text.
   */
  private static final List<Row> NVARCHAR2_TO_BYTES_VALUES =
      List.of(
          row("", null),
          row("IA==", " "),
          row("RFJPUCBUQUJMRQ==", "DROP TABLE"),
          row(repeatBase64("", "A", 2000), repeat("", "A", 2000)),
          row("SGVsbG8gV29ybGQ=", "Hello World"),
          row("YQ==", "a"),
          same(null),
          row("44GT44KT44Gr44Gh44Gv5LiW55WMIPCfmIA=", "こんにちは世界 😀"),
          row("TydCcmllbg==", "O'Brien"));

  private static final List<Row> NVARCHAR2_PK_VALUES =
      List.of(
          same(" "),
          same("DROP TABLE"),
          same(repeat("", "A", 2000)),
          same("Hello World"),
          same("a"),
          same("こんにちは世界 😀"),
          same("O'Brien"));

  /**
   * Oracle stores the empty string as NULL. The longest value uses the column's maximum length
   * (MAX_STRING_SIZE=STANDARD) instead of 32767 characters.
   */
  private static final List<Row> NCHAR_VALUES =
      List.of(
          row("", null),
          row(" ", repeat("", " ", 1000)),
          row("DROP TABLE", repeat("DROP TABLE", " ", 990)),
          same(repeat("", "A", 1000)),
          row("Hello World", repeat("Hello World", " ", 989)),
          row("a", repeat("a", " ", 999)),
          same(null),
          row("こんにちは世界 😀", repeat("こんにちは世界 😀", " ", 990)),
          row("O'Brien", repeat("O'Brien", " ", 993)));

  /**
   * Spanner BYTES (PostgreSQL: bytea) values hold the UTF-8 bytes of the text; the expected Oracle
   * value is the decoded text.
   */
  private static final List<Row> NCHAR_TO_BYTES_VALUES =
      List.of(
          row("", null),
          row("IA==", repeat("", " ", 1000)),
          row("RFJPUCBUQUJMRQ==", repeat("DROP TABLE", " ", 990)),
          row(repeatBase64("", "A", 1000), repeat("", "A", 1000)),
          row("SGVsbG8gV29ybGQ=", repeat("Hello World", " ", 989)),
          row("YQ==", repeat("a", " ", 999)),
          same(null),
          row("44GT44KT44Gr44Gh44Gv5LiW55WMIPCfmIA=", repeat("こんにちは世界 😀", " ", 990)),
          row("TydCcmllbg==", repeat("O'Brien", " ", 993)));

  private static final List<Row> NCHAR_PK_VALUES =
      List.of(
          row(" ", repeat("", " ", 1000)),
          row("DROP TABLE", repeat("DROP TABLE", " ", 990)),
          same(repeat("", "A", 1000)),
          row("Hello World", repeat("Hello World", " ", 989)),
          row("a", repeat("a", " ", 999)),
          row("こんにちは世界 😀", repeat("こんにちは世界 😀", " ", 990)),
          row("O'Brien", repeat("O'Brien", " ", 993)));

  /**
   * 1e38 and +/-(38 nines) exceed Spanner NUMERIC (29 integer digits), so they are tested through
   * the FLOAT64 and STRING alternatives instead.
   */
  private static final List<Row> NUMBER_VALUES =
      List.of(
          same("0"),
          same(null),
          same("123.456"),
          same("99999999999999999999999999999.999999999"),
          same("-99999999999999999999999999999.999999999"),
          same("0.000000001"),
          same("-0.5"));

  private static final List<Row> NUMBER_TO_FLOAT64_VALUES =
      List.of(
          row("1.0E38", "1E38"),
          row("-1.0E38", "-1E38"),
          row("0.0", "0"),
          same(null),
          same("123.456"),
          row("1.0E-9", "0.000000001"),
          same("-0.5"));

  private static final List<Row> NUMBER_TO_STR_VALUES =
      List.of(
          row("1e38", "1E38"),
          row("-1e38", "-1E38"),
          same("0"),
          same("99999999999999999999999999999999999999"),
          same("-99999999999999999999999999999999999999"),
          same(null),
          same("123.456"),
          same("-0.5"));

  /**
   * 1e38 and 38 nines exceed INT64 (PostgreSQL: bigint), so they are tested through the other
   * Spanner types instead.
   */
  private static final List<Row> NUMBER_TO_INT64_VALUES =
      List.of(
          same("0"),
          same(null),
          same("12345"),
          same("-12345"),
          same("9223372036854775807"),
          same("-9223372036854775808"));

  /**
   * Oracle NUMERIC/DECIMAL/DEC without precision = NUMBER(*,0): fractions are rounded by Oracle.
   */
  private static final List<Row> NUMERIC_VALUES =
      List.of(
          same("0"),
          same(null),
          same("12345"),
          same("-12345"),
          row("123.456", "123"),
          same("99999999999999999999999999999"),
          same("-99999999999999999999999999999"));

  /**
   * Oracle NUMERIC/DECIMAL/DEC without precision = NUMBER(*,0): fractions are rounded by Oracle.
   */
  private static final List<Row> NUMERIC_TO_FLOAT64_VALUES =
      List.of(
          row("1.0E38", "1E38"),
          row("-1.0E38", "-1E38"),
          row("0.0", "0"),
          same(null),
          row("12345.0", "12345"),
          row("123.456", "123"));

  /**
   * Oracle NUMERIC/DECIMAL/DEC without precision = NUMBER(*,0): fractions are rounded by Oracle.
   */
  private static final List<Row> NUMERIC_TO_STR_VALUES =
      List.of(
          row("1e38", "1E38"),
          row("-1e38", "-1E38"),
          same("0"),
          same("99999999999999999999999999999999999999"),
          same("-99999999999999999999999999999999999999"),
          same(null),
          same("12345"),
          row("123.456", "123"));

  /**
   * +/-1.79769313486231E308, NaN and Infinity are not representable in Oracle's NUMBER-based FLOAT;
   * the boundary values are +/-1E125 and 1E-130.
   */
  private static final List<Row> FLOAT_VALUES =
      List.of(
          same("0"),
          same(null),
          same("3.14159"),
          same("-123.456"),
          same("0.1"),
          same("12345678.5"));

  /**
   * +/-1.79769313486231E308, NaN and Infinity are not representable in Oracle's NUMBER-based FLOAT;
   * the boundary values are +/-1E125 and 1E-130.
   */
  private static final List<Row> FLOAT_TO_FLOAT64_VALUES =
      List.of(
          row("1.0E38", "1E38"),
          row("-1.0E38", "-1E38"),
          row("0.0", "0"),
          same(null),
          same("3.14159"),
          row("1.0E125", "1E125"),
          row("-1.0E125", "-1E125"),
          row("1.0E-130", "1E-130"),
          same("0.1"));

  /**
   * +/-1.79769313486231E308, NaN and Infinity are not representable in Oracle's NUMBER-based FLOAT;
   * the boundary values are +/-1E125 and 1E-130.
   */
  private static final List<Row> FLOAT_TO_STR_VALUES =
      List.of(
          row("1e38", "1E38"),
          row("-1e38", "-1E38"),
          same("0"),
          same(null),
          same("3.14159"),
          same("1E125"),
          same("-1E125"),
          same("1E-130"),
          same("0.1"));

  /**
   * +/-1.79769313486231E308, NaN and Infinity are not representable in Oracle's NUMBER-based FLOAT;
   * the boundary values are +/-1E125 and 1E-130.
   */
  private static final List<Row> FLOAT_TO_INT64_VALUES =
      List.of(same("0"), same(null), same("12345"), same("-12345"));

  private static final List<Row> BINARY_FLOAT_VALUES =
      List.of(
          same("1.0E38"),
          same("-1.0E38"),
          same("0.0"),
          same(null),
          same("3.40282E38"),
          same("-3.40282E38"),
          same("NaN"),
          same("Infinity"),
          same("3.14"),
          same("-Infinity"),
          same("1.17549435E-38"));

  /** NaN/Infinity and values beyond 29 integer digits are not representable in Spanner NUMERIC. */
  private static final List<Row> BINARY_FLOAT_TO_NUMERIC_VALUES =
      List.of(row("0", "0.0"), same(null), same("3.14"), same("-12.5"), row("12345", "12345.0"));

  private static final List<Row> BINARY_FLOAT_TO_INT64_VALUES =
      List.of(row("0", "0.0"), same(null), row("12345", "12345.0"), row("-12345", "-12345.0"));

  private static final List<Row> BINARY_DOUBLE_VALUES =
      List.of(
          same("1.0E38"),
          same("-1.0E38"),
          same("0.0"),
          same(null),
          same("1.79769313486231E308"),
          same("-1.79769313486231E308"),
          same("NaN"),
          same("Infinity"),
          same("3.141592653589793"),
          same("-Infinity"),
          same("2.2250738585072014E-308"));

  private static final List<Row> BINARY_DOUBLE_TO_NUMERIC_VALUES =
      List.of(
          row("0", "0.0"), same(null), same("3.141592654"), same("-12.5"), row("12345", "12345.0"));

  private static final List<Row> INTEGER_TO_NUMERIC_VALUES =
      List.of(
          same("0"),
          same(null),
          same("12345"),
          same("99999999999999999999999999999"),
          same("-99999999999999999999999999999"));

  private static final List<Row> INTEGER_TO_STR_VALUES =
      List.of(
          row("1e38", "1E38"),
          row("-1e38", "-1E38"),
          same("0"),
          same(null),
          same("99999999999999999999999999999999999999"),
          same("12345"));

  private static final List<Row> INTEGER_TO_FLOAT64_VALUES =
      List.of(
          row("1.0E38", "1E38"),
          row("-1.0E38", "-1E38"),
          row("0.0", "0"),
          same(null),
          row("12345.0", "12345"));

  private static final List<Row> INTEGER_PK_VALUES =
      List.of(
          same("0"),
          same("12345"),
          same("-12345"),
          same("9223372036854775807"),
          same("-9223372036854775808"));

  /** Spanner strings hold RFC 3339 timestamps. */
  private static final List<Row> DATE_VALUES =
      List.of(
          row("0001-01-01T00:00:00Z", "0001-01-01 00:00:00"),
          row("9999-12-31T23:59:59Z", "9999-12-31 23:59:59"),
          same(null),
          row("2024-01-15T10:30:45Z", "2024-01-15 10:30:45"),
          row("2024-02-29T23:59:59Z", "2024-02-29 23:59:59"),
          row("1970-01-01T00:00:00Z", "1970-01-01 00:00:00"));

  private static final List<Row> DATE_TO_DATE_VALUES =
      List.of(
          row("0001-01-01", "0001-01-01 00:00:00"),
          row("9999-12-31", "9999-12-31 00:00:00"),
          same(null),
          row("2024-01-15", "2024-01-15 00:00:00"),
          row("2024-02-29", "2024-02-29 00:00:00"),
          row("1970-01-01", "1970-01-01 00:00:00"));

  /** Spanner INT64 (PostgreSQL: bigint) values hold epoch microseconds. */
  private static final List<Row> DATE_TO_INT64_VALUES =
      List.of(
          row("-62135596800000000", "0001-01-01 00:00:00"),
          row("253402300799000000", "9999-12-31 23:59:59"),
          same(null),
          row("1705314645000000", "2024-01-15 10:30:45"),
          row("1709251199000000", "2024-02-29 23:59:59"),
          row("0", "1970-01-01 00:00:00"));

  private static final List<Row> DATE_PK_VALUES =
      List.of(
          row("0001-01-01T00:00:00Z", "0001-01-01 00:00:00"),
          row("9999-12-31T23:59:59Z", "9999-12-31 23:59:59"),
          row("2024-01-15T10:30:45Z", "2024-01-15 10:30:45"),
          row("2024-02-29T23:59:59Z", "2024-02-29 23:59:59"),
          row("1970-01-01T00:00:00Z", "1970-01-01 00:00:00"));

  /** Spanner strings hold RFC 3339 timestamps. */
  private static final List<Row> TIMESTAMP_VALUES =
      List.of(
          row("0001-01-01T00:00:00Z", "0001-01-01 00:00:00.000000"),
          row("9999-12-31T23:59:59Z", "9999-12-31 23:59:59.000000"),
          same(null),
          row("2024-01-15T10:30:45.123456Z", "2024-01-15 10:30:45.123456"),
          row("2024-02-29T12:00:00Z", "2024-02-29 12:00:00.000000"),
          row("9999-12-31T23:59:59.999999Z", "9999-12-31 23:59:59.999999"),
          row("1970-01-01T00:00:00Z", "1970-01-01 00:00:00.000000"));

  /** Spanner INT64 (PostgreSQL: bigint) values hold epoch microseconds. */
  private static final List<Row> TIMESTAMP_TO_INT64_VALUES =
      List.of(
          row("-62135596800000000", "0001-01-01 00:00:00.000000"),
          row("253402300799000000", "9999-12-31 23:59:59.000000"),
          same(null),
          row("1705314645123456", "2024-01-15 10:30:45.123456"),
          row("1709208000000000", "2024-02-29 12:00:00.000000"),
          row("253402300799999999", "9999-12-31 23:59:59.999999"),
          row("0", "1970-01-01 00:00:00.000000"));

  private static final List<Row> TIMESTAMP_PK_VALUES =
      List.of(
          row("0001-01-01T00:00:00Z", "0001-01-01 00:00:00.000000"),
          row("9999-12-31T23:59:59Z", "9999-12-31 23:59:59.000000"),
          row("2024-01-15T10:30:45.123456Z", "2024-01-15 10:30:45.123456"),
          row("2024-02-29T12:00:00Z", "2024-02-29 12:00:00.000000"),
          row("9999-12-31T23:59:59.999999Z", "9999-12-31 23:59:59.999999"),
          row("1970-01-01T00:00:00Z", "1970-01-01 00:00:00.000000"));

  /**
   * Oracle RAW stores empty bytes as NULL. The matrix has no Spanner PostgreSQL default for RAW, so
   * bytea is used.
   */
  private static final List<Row> RAW_VALUES =
      List.of(
          row("", null),
          same("IA=="),
          same("QQ=="),
          same("RFJPUCBUQUJMRQ=="),
          same("3q2+7w=="),
          same(hexBase64("FF", 2000)),
          same("AA=="),
          same(null));

  /**
   * Matrix alternative STRING (PostgreSQL: varchar) holding Base64: the Spanner string is the
   * Base64 text of the bytes.
   */
  private static final List<Row> RAW_TO_STR_VALUES =
      List.of(
          row("", null),
          same("IA=="),
          same("QQ=="),
          same("RFJPUCBUQUJMRQ=="),
          same("3q2+7w=="),
          row(repeat("", "/", 2666, "8="), hexBase64("FF", 2000)),
          same("AA=="),
          same(null));

  /** The matrix has no Spanner PostgreSQL primary key type for RAW, so bytea is used. */
  private static final List<Row> RAW_PK_VALUES =
      List.of(
          same("IA=="),
          same("QQ=="),
          same("RFJPUCBUQUJMRQ=="),
          same("3q2+7w=="),
          same(hexBase64("FF", 2000)),
          same("AA=="));

  /**
   * Empty Spanner BYTES (PostgreSQL: bytea) values are written as '', which Oracle stores as NULL
   * (as for RAW). The matrix has no Spanner PostgreSQL default for BLOB, so bytea is used.
   */
  private static final List<Row> BLOB_VALUES =
      List.of(
          row("", null),
          same(repeatBase64("", "A", 100000)),
          same(null),
          same("3q2+7w=="),
          same("AA=="));

  /**
   * The empty string is written as '', which Oracle stores as NULL. The matrix lists no Spanner
   * PostgreSQL alternative for CLOB.
   */
  private static final List<Row> CLOB_VALUES =
      List.of(
          row("", null),
          same(repeat("", "A", 100000)),
          same(null),
          same("Hello CLOB"),
          same("こんにちは世界 😀"),
          same("O'Brien"));

  /** Spanner BYTES hold the UTF-8 bytes of the text. */
  private static final List<Row> CLOB_TO_BYTES_VALUES =
      List.of(
          row("", null),
          row(repeatBase64("", "A", 100000), repeat("", "A", 100000)),
          same(null),
          row("SGVsbG8gQ0xPQg==", "Hello CLOB"),
          row("44GT44KT44Gr44Gh44Gv5LiW55WMIPCfmIA=", "こんにちは世界 😀"),
          row("TydCcmllbg==", "O'Brien"));

  /** Spanner PostgreSQL numeric can hold 1e38 and 38 nines (GoogleSQL NUMERIC cannot). */
  private static final List<Row> NUMBER_TO_NUMERIC_VALUES =
      List.of(
          row("100000000000000000000000000000000000000", "1E38"),
          row("-100000000000000000000000000000000000000", "-1E38"),
          same("0"),
          same("99999999999999999999999999999999999999"),
          same("-99999999999999999999999999999999999999"),
          same(null),
          same("123.456"),
          same("99999999999999999999999999999.999999999"),
          same("0.000000001"),
          same("-0.5"));

  /**
   * Oracle NUMERIC/DECIMAL/DEC without precision is NUMBER(*,0): fractions are rounded by Oracle.
   * Spanner PostgreSQL numeric can hold 1e38 and 38 nines.
   */
  private static final List<Row> NUMERIC_VALUES_2 =
      List.of(
          same("0"),
          same(null),
          same("12345"),
          same("-12345"),
          row("123.456", "123"),
          same("99999999999999999999999999999999999999"),
          same("-99999999999999999999999999999999999999"),
          row("100000000000000000000000000000000000000", "1E38"));

  /**
   * +/-1.79769313486231E308, NaN and Infinity are not representable in Oracle's NUMBER-based FLOAT;
   * the boundary values are +/-1E125 and 1E-130.
   */
  private static final List<Row> FLOAT_TO_NUMERIC_VALUES =
      List.of(
          same("0"),
          same(null),
          same("3.14159"),
          same("-123.456"),
          same("0.1"),
          same("12345678.5"),
          row("100000000000000000000000000000000000000", "1E38"),
          row("-100000000000000000000000000000000000000", "-1E38"));

  /**
   * NaN and Infinity are not representable in Spanner PostgreSQL numeric; 3.40282E38 is (unlike
   * GoogleSQL NUMERIC).
   */
  private static final List<Row> BINARY_FLOAT_TO_NUMERIC_VALUES_2 =
      List.of(
          row("0", "0.0"),
          same(null),
          same("3.14"),
          same("-12.5"),
          row("12345", "12345.0"),
          row("340282000000000000000000000000000000000", "3.40282E38"));

  private static final List<Row> BINARY_DOUBLE_TO_NUMERIC_VALUES_2 =
      List.of(
          row("0", "0.0"),
          same(null),
          same("3.141592654"),
          same("-12.5"),
          row("12345", "12345.0"),
          row("100000000000000000000000000000000000000", "1.0E38"));

  private static final List<Row> INTEGER_TO_NUMERIC_VALUES_2 =
      List.of(
          same("0"),
          same(null),
          same("12345"),
          same("99999999999999999999999999999999999999"),
          same("-99999999999999999999999999999999999999"),
          row("100000000000000000000000000000000000000", "1E38"));

  // ---------------------------------------------------------------------------------------------
  // Tables
  // ---------------------------------------------------------------------------------------------

  /** Type mapping tables of {@link SpannerToOracleDataTypesIT} (GoogleSQL Spanner database). */
  static List<TypeTable> googleSqlTables() {
    return List.of(
        table("varchar2_table", "VARCHAR2(4000)", "STRING(MAX)", STRING, VARCHAR2_VALUES),
        table(
            "varchar2_to_str4000_table", "VARCHAR2(4000)", "STRING(4000)", STRING, VARCHAR2_VALUES),
        table(
            "varchar2_to_bytes_table",
            "VARCHAR2(4000)",
            "BYTES(MAX)",
            STRING,
            VARCHAR2_TO_BYTES_VALUES),
        pk("varchar2_pk_table", "VARCHAR2(4000)", "STRING(MAX)", STRING, VARCHAR2_PK_VALUES),
        table("varchar_table", "VARCHAR(4000)", "STRING(MAX)", STRING, VARCHAR2_VALUES),
        table("varchar_to_str4000_table", "VARCHAR(4000)", "STRING(4000)", STRING, VARCHAR2_VALUES),
        table(
            "varchar_to_bytes_table",
            "VARCHAR(4000)",
            "BYTES(MAX)",
            STRING,
            VARCHAR2_TO_BYTES_VALUES),
        pk("varchar_pk_table", "VARCHAR(4000)", "STRING(MAX)", STRING, VARCHAR2_PK_VALUES),
        table("char_table", "CHAR(2000)", "STRING(MAX)", STRING, CHAR_VALUES),
        table("char_to_str2000_table", "CHAR(2000)", "STRING(2000)", STRING, CHAR_VALUES),
        table("char_to_bytes_table", "CHAR(2000)", "BYTES(MAX)", STRING, CHAR_TO_BYTES_VALUES),
        pk("char_pk_table", "CHAR(2000)", "STRING(MAX)", STRING, CHAR_PK_VALUES),
        table("character_table", "CHARACTER(2000)", "STRING(MAX)", STRING, CHAR_VALUES),
        table("character_to_str2000_table", "CHARACTER(2000)", "STRING(2000)", STRING, CHAR_VALUES),
        table(
            "character_to_bytes_table",
            "CHARACTER(2000)",
            "BYTES(MAX)",
            STRING,
            CHAR_TO_BYTES_VALUES),
        pk("character_pk_table", "CHARACTER(2000)", "STRING(MAX)", STRING, CHAR_PK_VALUES),
        table("nvarchar2_table", "NVARCHAR2(2000)", "STRING(MAX)", STRING, NVARCHAR2_VALUES),
        table(
            "nvarchar2_to_str2000_table",
            "NVARCHAR2(2000)",
            "STRING(2000)",
            STRING,
            NVARCHAR2_VALUES),
        table(
            "nvarchar2_to_bytes_table",
            "NVARCHAR2(2000)",
            "BYTES(MAX)",
            STRING,
            NVARCHAR2_TO_BYTES_VALUES),
        pk("nvarchar2_pk_table", "NVARCHAR2(2000)", "STRING(MAX)", STRING, NVARCHAR2_PK_VALUES),
        table("nchar_table", "NCHAR(1000)", "STRING(MAX)", STRING, NCHAR_VALUES),
        table("nchar_to_str1000_table", "NCHAR(1000)", "STRING(1000)", STRING, NCHAR_VALUES),
        table("nchar_to_bytes_table", "NCHAR(1000)", "BYTES(MAX)", STRING, NCHAR_TO_BYTES_VALUES),
        pk("nchar_pk_table", "NCHAR(1000)", "STRING(MAX)", STRING, NCHAR_PK_VALUES),
        table(
            "nchar_varying_table", "NCHAR VARYING(2000)", "STRING(MAX)", STRING, NVARCHAR2_VALUES),
        table(
            "nchar_varying_to_str2000_table",
            "NCHAR VARYING(2000)",
            "STRING(2000)",
            STRING,
            NVARCHAR2_VALUES),
        table(
            "nchar_varying_to_bytes_table",
            "NCHAR VARYING(2000)",
            "BYTES(MAX)",
            STRING,
            NVARCHAR2_TO_BYTES_VALUES),
        pk(
            "nchar_varying_pk_table",
            "NCHAR VARYING(2000)",
            "STRING(MAX)",
            STRING,
            NVARCHAR2_PK_VALUES),
        table(
            "nat_character_table", "NATIONAL CHARACTER(1000)", "STRING(MAX)", STRING, NCHAR_VALUES),
        table(
            "nat_character_to_str1000_table",
            "NATIONAL CHARACTER(1000)",
            "STRING(1000)",
            STRING,
            NCHAR_VALUES),
        table(
            "nat_character_to_bytes_table",
            "NATIONAL CHARACTER(1000)",
            "BYTES(MAX)",
            STRING,
            NCHAR_TO_BYTES_VALUES),
        pk(
            "nat_character_pk_table",
            "NATIONAL CHARACTER(1000)",
            "STRING(MAX)",
            STRING,
            NCHAR_PK_VALUES),
        table("nat_char_table", "NATIONAL CHAR(1000)", "STRING(MAX)", STRING, NCHAR_VALUES),
        table(
            "nat_char_to_str1000_table",
            "NATIONAL CHAR(1000)",
            "STRING(1000)",
            STRING,
            NCHAR_VALUES),
        table(
            "nat_char_to_bytes_table",
            "NATIONAL CHAR(1000)",
            "BYTES(MAX)",
            STRING,
            NCHAR_TO_BYTES_VALUES),
        pk("nat_char_pk_table", "NATIONAL CHAR(1000)", "STRING(MAX)", STRING, NCHAR_PK_VALUES),
        table(
            "nat_characvar_table",
            "NATIONAL CHARACTER VARYING(2000)",
            "STRING(MAX)",
            STRING,
            NVARCHAR2_VALUES),
        table(
            "nat_characvar_to_str2000_table",
            "NATIONAL CHARACTER VARYING(2000)",
            "STRING(2000)",
            STRING,
            NVARCHAR2_VALUES),
        table(
            "nat_characvar_to_bytes_table",
            "NATIONAL CHARACTER VARYING(2000)",
            "BYTES(MAX)",
            STRING,
            NVARCHAR2_TO_BYTES_VALUES),
        pk(
            "nat_characvar_pk_table",
            "NATIONAL CHARACTER VARYING(2000)",
            "STRING(MAX)",
            STRING,
            NVARCHAR2_PK_VALUES),
        table(
            "nat_charvar_table",
            "NATIONAL CHAR VARYING(2000)",
            "STRING(MAX)",
            STRING,
            NVARCHAR2_VALUES),
        table(
            "nat_charvar_to_str2000_table",
            "NATIONAL CHAR VARYING(2000)",
            "STRING(2000)",
            STRING,
            NVARCHAR2_VALUES),
        table(
            "nat_charvar_to_bytes_table",
            "NATIONAL CHAR VARYING(2000)",
            "BYTES(MAX)",
            STRING,
            NVARCHAR2_TO_BYTES_VALUES),
        pk(
            "nat_charvar_pk_table",
            "NATIONAL CHAR VARYING(2000)",
            "STRING(MAX)",
            STRING,
            NVARCHAR2_PK_VALUES),
        table("number_table", "NUMBER", "NUMERIC", NUMBER, NUMBER_VALUES),
        table("number_to_float64_table", "NUMBER", "FLOAT64", NUMBER, NUMBER_TO_FLOAT64_VALUES),
        table("number_to_str_table", "NUMBER", "STRING(MAX)", NUMBER, NUMBER_TO_STR_VALUES),
        table("number_to_int64_table", "NUMBER", "INT64", NUMBER, NUMBER_TO_INT64_VALUES),
        table("numeric_table", "NUMERIC", "NUMERIC", NUMBER, NUMERIC_VALUES),
        table("numeric_to_float64_table", "NUMERIC", "FLOAT64", NUMBER, NUMERIC_TO_FLOAT64_VALUES),
        table("numeric_to_str_table", "NUMERIC", "STRING(MAX)", NUMBER, NUMERIC_TO_STR_VALUES),
        table("numeric_to_int64_table", "NUMERIC", "INT64", NUMBER, NUMBER_TO_INT64_VALUES),
        table("decimal_table", "DECIMAL", "NUMERIC", NUMBER, NUMERIC_VALUES),
        table("decimal_to_float64_table", "DECIMAL", "FLOAT64", NUMBER, NUMERIC_TO_FLOAT64_VALUES),
        table("decimal_to_str_table", "DECIMAL", "STRING(MAX)", NUMBER, NUMERIC_TO_STR_VALUES),
        table("decimal_to_int64_table", "DECIMAL", "INT64", NUMBER, NUMBER_TO_INT64_VALUES),
        table("dec_table", "DEC", "NUMERIC", NUMBER, NUMERIC_VALUES),
        table("dec_to_float64_table", "DEC", "FLOAT64", NUMBER, NUMERIC_TO_FLOAT64_VALUES),
        table("dec_to_str_table", "DEC", "STRING(MAX)", NUMBER, NUMERIC_TO_STR_VALUES),
        table("dec_to_int64_table", "DEC", "INT64", NUMBER, NUMBER_TO_INT64_VALUES),
        table("float_table", "FLOAT", "NUMERIC", NUMBER, FLOAT_VALUES),
        table("float_to_float64_table", "FLOAT", "FLOAT64", NUMBER, FLOAT_TO_FLOAT64_VALUES),
        table("float_to_str_table", "FLOAT", "STRING(MAX)", NUMBER, FLOAT_TO_STR_VALUES),
        table("float_to_int64_table", "FLOAT", "INT64", NUMBER, FLOAT_TO_INT64_VALUES),
        table("double_prec_table", "DOUBLE PRECISION", "FLOAT64", NUMBER, FLOAT_TO_FLOAT64_VALUES),
        table("double_prec_to_numeric_table", "DOUBLE PRECISION", "NUMERIC", NUMBER, FLOAT_VALUES),
        table(
            "double_prec_to_str_table",
            "DOUBLE PRECISION",
            "STRING(MAX)",
            NUMBER,
            FLOAT_TO_STR_VALUES),
        table(
            "double_prec_to_int64_table",
            "DOUBLE PRECISION",
            "INT64",
            NUMBER,
            FLOAT_TO_INT64_VALUES),
        table("real_table", "REAL", "FLOAT64", NUMBER, FLOAT_TO_FLOAT64_VALUES),
        table("real_to_numeric_table", "REAL", "NUMERIC", NUMBER, FLOAT_VALUES),
        table("real_to_str_table", "REAL", "STRING(MAX)", NUMBER, FLOAT_TO_STR_VALUES),
        table("real_to_int64_table", "REAL", "INT64", NUMBER, FLOAT_TO_INT64_VALUES),
        table("binary_float_table", "BINARY_FLOAT", "FLOAT32", FLOAT, BINARY_FLOAT_VALUES),
        table(
            "binary_float_to_float64_table", "BINARY_FLOAT", "FLOAT64", FLOAT, BINARY_FLOAT_VALUES),
        table(
            "binary_float_to_str_table", "BINARY_FLOAT", "STRING(MAX)", FLOAT, BINARY_FLOAT_VALUES),
        table(
            "binary_float_to_numeric_table",
            "BINARY_FLOAT",
            "NUMERIC",
            FLOAT,
            BINARY_FLOAT_TO_NUMERIC_VALUES),
        table(
            "binary_float_to_int64_table",
            "BINARY_FLOAT",
            "INT64",
            FLOAT,
            BINARY_FLOAT_TO_INT64_VALUES),
        table("binary_double_table", "BINARY_DOUBLE", "FLOAT64", DOUBLE, BINARY_DOUBLE_VALUES),
        table(
            "binary_double_to_str_table",
            "BINARY_DOUBLE",
            "STRING(MAX)",
            DOUBLE,
            BINARY_DOUBLE_VALUES),
        table(
            "binary_double_to_numeric_table",
            "BINARY_DOUBLE",
            "NUMERIC",
            DOUBLE,
            BINARY_DOUBLE_TO_NUMERIC_VALUES),
        table(
            "binary_double_to_int64_table",
            "BINARY_DOUBLE",
            "INT64",
            DOUBLE,
            BINARY_FLOAT_TO_INT64_VALUES),
        table("integer_table", "INTEGER", "INT64", NUMBER, NUMBER_TO_INT64_VALUES),
        table("integer_to_numeric_table", "INTEGER", "NUMERIC", NUMBER, INTEGER_TO_NUMERIC_VALUES),
        table("integer_to_str_table", "INTEGER", "STRING(MAX)", NUMBER, INTEGER_TO_STR_VALUES),
        table("integer_to_float64_table", "INTEGER", "FLOAT64", NUMBER, INTEGER_TO_FLOAT64_VALUES),
        pk("integer_pk_table", "INTEGER", "INT64", NUMBER, INTEGER_PK_VALUES),
        table("int_table", "INT", "INT64", NUMBER, NUMBER_TO_INT64_VALUES),
        table("int_to_numeric_table", "INT", "NUMERIC", NUMBER, INTEGER_TO_NUMERIC_VALUES),
        table("int_to_str_table", "INT", "STRING(MAX)", NUMBER, INTEGER_TO_STR_VALUES),
        table("int_to_float64_table", "INT", "FLOAT64", NUMBER, INTEGER_TO_FLOAT64_VALUES),
        pk("int_pk_table", "INT", "INT64", NUMBER, INTEGER_PK_VALUES),
        table("smallint_table", "SMALLINT", "INT64", NUMBER, NUMBER_TO_INT64_VALUES),
        table(
            "smallint_to_numeric_table", "SMALLINT", "NUMERIC", NUMBER, INTEGER_TO_NUMERIC_VALUES),
        table("smallint_to_str_table", "SMALLINT", "STRING(MAX)", NUMBER, INTEGER_TO_STR_VALUES),
        table(
            "smallint_to_float64_table", "SMALLINT", "FLOAT64", NUMBER, INTEGER_TO_FLOAT64_VALUES),
        pk("smallint_pk_table", "SMALLINT", "INT64", NUMBER, INTEGER_PK_VALUES),
        table("date_table", "DATE", "TIMESTAMP", STRING, DATE_TEXT, DATE_VALUES),
        table("date_to_date_table", "DATE", "DATE", STRING, DATE_TEXT, DATE_TO_DATE_VALUES),
        table("date_to_str_table", "DATE", "STRING(MAX)", STRING, DATE_TEXT, DATE_VALUES),
        table("date_to_int64_table", "DATE", "INT64", STRING, DATE_TEXT, DATE_TO_INT64_VALUES),
        pk("date_pk_table", "DATE", "TIMESTAMP", STRING, DATE_TEXT, DATE_PK_VALUES),
        table(
            "timestamp_table", "TIMESTAMP", "TIMESTAMP", STRING, TIMESTAMP_TEXT, TIMESTAMP_VALUES),
        table(
            "timestamp_to_str_table",
            "TIMESTAMP",
            "STRING(MAX)",
            STRING,
            TIMESTAMP_TEXT,
            TIMESTAMP_VALUES),
        table(
            "timestamp_to_int64_table",
            "TIMESTAMP",
            "INT64",
            STRING,
            TIMESTAMP_TEXT,
            TIMESTAMP_TO_INT64_VALUES),
        pk(
            "timestamp_pk_table",
            "TIMESTAMP",
            "TIMESTAMP",
            STRING,
            TIMESTAMP_TEXT,
            TIMESTAMP_PK_VALUES),
        table(
            "timestamp_tz_table",
            "TIMESTAMP WITH TIME ZONE",
            "TIMESTAMP",
            STRING,
            TIMESTAMP_UTC_TEXT,
            TIMESTAMP_VALUES),
        table(
            "timestamp_tz_to_str_table",
            "TIMESTAMP WITH TIME ZONE",
            "STRING(MAX)",
            STRING,
            TIMESTAMP_UTC_TEXT,
            TIMESTAMP_VALUES),
        table(
            "timestamp_tz_to_int64_table",
            "TIMESTAMP WITH TIME ZONE",
            "INT64",
            STRING,
            TIMESTAMP_UTC_TEXT,
            TIMESTAMP_TO_INT64_VALUES),
        table(
            "timestamp_ltz_table",
            "TIMESTAMP WITH LOCAL TIME ZONE",
            "TIMESTAMP",
            STRING,
            TIMESTAMP_UTC_TEXT,
            TIMESTAMP_VALUES),
        table(
            "timestamp_ltz_to_str_table",
            "TIMESTAMP WITH LOCAL TIME ZONE",
            "STRING(MAX)",
            STRING,
            TIMESTAMP_UTC_TEXT,
            TIMESTAMP_VALUES),
        table(
            "timestamp_ltz_to_int64_table",
            "TIMESTAMP WITH LOCAL TIME ZONE",
            "INT64",
            STRING,
            TIMESTAMP_UTC_TEXT,
            TIMESTAMP_TO_INT64_VALUES),
        pk(
            "timestamp_ltz_pk_table",
            "TIMESTAMP WITH LOCAL TIME ZONE",
            "TIMESTAMP",
            STRING,
            TIMESTAMP_UTC_TEXT,
            TIMESTAMP_PK_VALUES),
        table("raw_table", "RAW", "BYTES(MAX)", BYTES, RAW_VALUES),
        table("raw_to_bytes2000_table", "RAW", "BYTES(2000)", BYTES, RAW_VALUES),
        table("raw_to_str_table", "RAW", "STRING(MAX)", BYTES, RAW_TO_STR_VALUES),
        pk("raw_pk_table", "RAW", "BYTES(MAX)", BYTES, RAW_PK_VALUES),
        table("blob_table", "BLOB", "BYTES(MAX)", BYTES, BLOB_VALUES),
        table("blob_to_str_table", "BLOB", "STRING(MAX)", BYTES, BLOB_VALUES),
        table("clob_table", "CLOB", "STRING(MAX)", STRING, CLOB_VALUES),
        table("clob_to_bytes_table", "CLOB", "BYTES(MAX)", STRING, CLOB_TO_BYTES_VALUES),
        table("nclob_table", "NCLOB", "STRING(MAX)", STRING, CLOB_VALUES),
        table("nclob_to_bytes_table", "NCLOB", "BYTES(MAX)", STRING, CLOB_TO_BYTES_VALUES));
  }

  /** Unsupported-type tables of {@link SpannerToOracleDataTypesIT}. */
  static List<UnsupportedTable> googleSqlUnsupportedTables() {
    return List.of(
        unsupported(
            "interval_year_to_month_table",
            "INTERVAL YEAR TO MONTH",
            "STRING(MAX)",
            "+999999999-11",
            "-999999999-11",
            null),
        unsupported(
            "interval_day_to_second_table",
            "INTERVAL DAY TO SECOND",
            "STRING(MAX)",
            "+999999999 23:59:59.999999999",
            "-999999999 23:59:59.999999999",
            null),
        unsupported("long_raw_table", "LONG RAW", "BYTES(MAX)", "", "QQ==", null),
        unsupported("bfile_table", "BFILE", "STRING(MAX)", "bfile('DIR', 'file.txt')", null),
        unsupported(
            "long_table",
            "LONG",
            "STRING(MAX)",
            "",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            null),
        unsupported("rowid_table", "ROWID", "STRING(MAX)", "AAAB12AADAAAAwPAAA", null),
        unsupported("urowid_table", "UROWID", "STRING(MAX)", "AAAB12AADAAAAwPAAA", null),
        unsupported("xmltype_table", "XMLType", "STRING(MAX)", "<xml></xml>", "<bad_xml", null),
        unsupported("anydata_table", "ANYDATA", "BYTES(MAX)", "BQ==", null),
        unsupported("anytype_table", "ANYTYPE", "BYTES(MAX)", "BQ==", null),
        unsupported("anydataset_table", "ANYDATASET", "BYTES(MAX)", "BQ==", null),
        unsupported("uritype_table", "URIType", "STRING(MAX)", "http://test.com", null),
        unsupported("dburitype_table", "DBUriType", "STRING(MAX)", "/PUBLIC/DUAL", null),
        unsupported("xdburitype_table", "XDBUriType", "STRING(MAX)", "/public", null),
        unsupported("httpuritype_table", "HttpUriType", "STRING(MAX)", "http://test.com", null),
        unsupported("mlslabel_table", "MLSLABEL", "STRING(MAX)", "SENSITIVE", null),
        unsupported("ref_table", "REF", "STRING(MAX)", "REF(obj)", null),
        unsupported("varray_table", "VARRAY", "ARRAY<STRING(MAX)>", "[]", "[\"A\",\"B\"]", null),
        unsupported("nested_table_table", "NESTED TABLE", "JSON", "{}", "{\"id\":1}", null),
        unsupported("object_type_table", "OBJECT TYPE", "JSON", "{\"attr1\":\"val\"}", null));
  }

  /**
   * Type mapping tables of {@link SpannerToOracleDataTypesPGDialectIT} (PostgreSQL-dialect Spanner
   * database).
   */
  static List<TypeTable> postgresTables() {
    return List.of(
        table("varchar2_table", "VARCHAR2(4000)", "varchar", STRING, VARCHAR2_VALUES),
        table(
            "varchar2_to_varchar4000_table",
            "VARCHAR2(4000)",
            "varchar(4000)",
            STRING,
            VARCHAR2_VALUES),
        table(
            "varchar2_to_bytea_table", "VARCHAR2(4000)", "bytea", STRING, VARCHAR2_TO_BYTES_VALUES),
        pk("varchar2_pk_table", "VARCHAR2(4000)", "varchar", STRING, VARCHAR2_PK_VALUES),
        table("varchar_table", "VARCHAR(4000)", "varchar", STRING, VARCHAR2_VALUES),
        table(
            "varchar_to_varchar4000_table",
            "VARCHAR(4000)",
            "varchar(4000)",
            STRING,
            VARCHAR2_VALUES),
        table("varchar_to_bytea_table", "VARCHAR(4000)", "bytea", STRING, VARCHAR2_TO_BYTES_VALUES),
        pk("varchar_pk_table", "VARCHAR(4000)", "varchar", STRING, VARCHAR2_PK_VALUES),
        table("char_table", "CHAR(2000)", "varchar", STRING, CHAR_VALUES),
        table("char_to_varchar2000_table", "CHAR(2000)", "varchar(2000)", STRING, CHAR_VALUES),
        table("char_to_bytea_table", "CHAR(2000)", "bytea", STRING, CHAR_TO_BYTES_VALUES),
        pk("char_pk_table", "CHAR(2000)", "varchar", STRING, CHAR_PK_VALUES),
        table("character_table", "CHARACTER(2000)", "varchar", STRING, CHAR_VALUES),
        table(
            "character_to_varchar2000_table",
            "CHARACTER(2000)",
            "varchar(2000)",
            STRING,
            CHAR_VALUES),
        table("character_to_bytea_table", "CHARACTER(2000)", "bytea", STRING, CHAR_TO_BYTES_VALUES),
        pk("character_pk_table", "CHARACTER(2000)", "varchar", STRING, CHAR_PK_VALUES),
        table("nvarchar2_table", "NVARCHAR2(2000)", "varchar", STRING, NVARCHAR2_VALUES),
        table(
            "nvarchar2_to_varchar2000_table",
            "NVARCHAR2(2000)",
            "varchar(2000)",
            STRING,
            NVARCHAR2_VALUES),
        table(
            "nvarchar2_to_bytea_table",
            "NVARCHAR2(2000)",
            "bytea",
            STRING,
            NVARCHAR2_TO_BYTES_VALUES),
        pk("nvarchar2_pk_table", "NVARCHAR2(2000)", "varchar", STRING, NVARCHAR2_PK_VALUES),
        table("nchar_table", "NCHAR(1000)", "varchar", STRING, NCHAR_VALUES),
        table("nchar_to_varchar1000_table", "NCHAR(1000)", "varchar(1000)", STRING, NCHAR_VALUES),
        table("nchar_to_bytea_table", "NCHAR(1000)", "bytea", STRING, NCHAR_TO_BYTES_VALUES),
        pk("nchar_pk_table", "NCHAR(1000)", "varchar", STRING, NCHAR_PK_VALUES),
        table("nchar_varying_table", "NCHAR VARYING(2000)", "varchar", STRING, NVARCHAR2_VALUES),
        table(
            "nchar_varying_to_varchar2000_table",
            "NCHAR VARYING(2000)",
            "varchar(2000)",
            STRING,
            NVARCHAR2_VALUES),
        table(
            "nchar_varying_to_bytea_table",
            "NCHAR VARYING(2000)",
            "bytea",
            STRING,
            NVARCHAR2_TO_BYTES_VALUES),
        pk("nchar_varying_pk_table", "NCHAR VARYING(2000)", "varchar", STRING, NVARCHAR2_PK_VALUES),
        table("nat_character_table", "NATIONAL CHARACTER(1000)", "varchar", STRING, NCHAR_VALUES),
        table(
            "nat_character_to_varchar1000_table",
            "NATIONAL CHARACTER(1000)",
            "varchar(1000)",
            STRING,
            NCHAR_VALUES),
        table(
            "nat_character_to_bytea_table",
            "NATIONAL CHARACTER(1000)",
            "bytea",
            STRING,
            NCHAR_TO_BYTES_VALUES),
        pk(
            "nat_character_pk_table",
            "NATIONAL CHARACTER(1000)",
            "varchar",
            STRING,
            NCHAR_PK_VALUES),
        table("nat_char_table", "NATIONAL CHAR(1000)", "varchar", STRING, NCHAR_VALUES),
        table(
            "nat_char_to_varchar1000_table",
            "NATIONAL CHAR(1000)",
            "varchar(1000)",
            STRING,
            NCHAR_VALUES),
        table(
            "nat_char_to_bytea_table",
            "NATIONAL CHAR(1000)",
            "bytea",
            STRING,
            NCHAR_TO_BYTES_VALUES),
        pk("nat_char_pk_table", "NATIONAL CHAR(1000)", "varchar", STRING, NCHAR_PK_VALUES),
        table(
            "nat_characvar_table",
            "NATIONAL CHARACTER VARYING(2000)",
            "varchar",
            STRING,
            NVARCHAR2_VALUES),
        table(
            "nat_characvar_to_varchar2000_table",
            "NATIONAL CHARACTER VARYING(2000)",
            "varchar(2000)",
            STRING,
            NVARCHAR2_VALUES),
        table(
            "nat_characvar_to_bytea_table",
            "NATIONAL CHARACTER VARYING(2000)",
            "bytea",
            STRING,
            NVARCHAR2_TO_BYTES_VALUES),
        pk(
            "nat_characvar_pk_table",
            "NATIONAL CHARACTER VARYING(2000)",
            "varchar",
            STRING,
            NVARCHAR2_PK_VALUES),
        table(
            "nat_charvar_table",
            "NATIONAL CHAR VARYING(2000)",
            "varchar",
            STRING,
            NVARCHAR2_VALUES),
        table(
            "nat_charvar_to_varchar2000_table",
            "NATIONAL CHAR VARYING(2000)",
            "varchar(2000)",
            STRING,
            NVARCHAR2_VALUES),
        table(
            "nat_charvar_to_bytea_table",
            "NATIONAL CHAR VARYING(2000)",
            "bytea",
            STRING,
            NVARCHAR2_TO_BYTES_VALUES),
        pk(
            "nat_charvar_pk_table",
            "NATIONAL CHAR VARYING(2000)",
            "varchar",
            STRING,
            NVARCHAR2_PK_VALUES),
        table("number_table", "NUMBER", "float8", NUMBER, NUMBER_TO_FLOAT64_VALUES),
        table("number_to_numeric_table", "NUMBER", "numeric", NUMBER, NUMBER_TO_NUMERIC_VALUES),
        table("number_to_varchar_table", "NUMBER", "varchar", NUMBER, NUMBER_TO_STR_VALUES),
        table("number_to_bigint_table", "NUMBER", "bigint", NUMBER, NUMBER_TO_INT64_VALUES),
        table("numeric_table", "NUMERIC", "numeric", NUMBER, NUMERIC_VALUES_2),
        table("numeric_to_float8_table", "NUMERIC", "float8", NUMBER, NUMERIC_TO_FLOAT64_VALUES),
        table("numeric_to_varchar_table", "NUMERIC", "varchar", NUMBER, NUMERIC_TO_STR_VALUES),
        table("numeric_to_bigint_table", "NUMERIC", "bigint", NUMBER, NUMBER_TO_INT64_VALUES),
        table("decimal_table", "DECIMAL", "numeric", NUMBER, NUMERIC_VALUES_2),
        table("decimal_to_float8_table", "DECIMAL", "float8", NUMBER, NUMERIC_TO_FLOAT64_VALUES),
        table("decimal_to_varchar_table", "DECIMAL", "varchar", NUMBER, NUMERIC_TO_STR_VALUES),
        table("decimal_to_bigint_table", "DECIMAL", "bigint", NUMBER, NUMBER_TO_INT64_VALUES),
        table("dec_table", "DEC", "numeric", NUMBER, NUMERIC_VALUES_2),
        table("dec_to_float8_table", "DEC", "float8", NUMBER, NUMERIC_TO_FLOAT64_VALUES),
        table("dec_to_varchar_table", "DEC", "varchar", NUMBER, NUMERIC_TO_STR_VALUES),
        table("dec_to_bigint_table", "DEC", "bigint", NUMBER, NUMBER_TO_INT64_VALUES),
        table("float_table", "FLOAT", "float8", NUMBER, FLOAT_TO_FLOAT64_VALUES),
        table("float_to_numeric_table", "FLOAT", "numeric", NUMBER, FLOAT_TO_NUMERIC_VALUES),
        table("float_to_varchar_table", "FLOAT", "varchar", NUMBER, FLOAT_TO_STR_VALUES),
        table("float_to_bigint_table", "FLOAT", "bigint", NUMBER, FLOAT_TO_INT64_VALUES),
        table("double_prec_table", "DOUBLE PRECISION", "float8", NUMBER, FLOAT_TO_FLOAT64_VALUES),
        table(
            "double_prec_to_numeric_table",
            "DOUBLE PRECISION",
            "numeric",
            NUMBER,
            FLOAT_TO_NUMERIC_VALUES),
        table(
            "double_prec_to_varchar_table",
            "DOUBLE PRECISION",
            "varchar",
            NUMBER,
            FLOAT_TO_STR_VALUES),
        table(
            "double_prec_to_bigint_table",
            "DOUBLE PRECISION",
            "bigint",
            NUMBER,
            FLOAT_TO_INT64_VALUES),
        table("real_table", "REAL", "float8", NUMBER, FLOAT_TO_FLOAT64_VALUES),
        table("real_to_numeric_table", "REAL", "numeric", NUMBER, FLOAT_TO_NUMERIC_VALUES),
        table("real_to_varchar_table", "REAL", "varchar", NUMBER, FLOAT_TO_STR_VALUES),
        table("real_to_bigint_table", "REAL", "bigint", NUMBER, FLOAT_TO_INT64_VALUES),
        table("binary_float_table", "BINARY_FLOAT", "float4", FLOAT, BINARY_FLOAT_VALUES),
        table("binary_float_to_float8_table", "BINARY_FLOAT", "float8", FLOAT, BINARY_FLOAT_VALUES),
        table(
            "binary_float_to_varchar_table", "BINARY_FLOAT", "varchar", FLOAT, BINARY_FLOAT_VALUES),
        table(
            "binary_float_to_numeric_table",
            "BINARY_FLOAT",
            "numeric",
            FLOAT,
            BINARY_FLOAT_TO_NUMERIC_VALUES_2),
        table("binary_double_table", "BINARY_DOUBLE", "float8", DOUBLE, BINARY_DOUBLE_VALUES),
        table(
            "binary_double_to_varchar_table",
            "BINARY_DOUBLE",
            "varchar",
            DOUBLE,
            BINARY_DOUBLE_VALUES),
        table(
            "binary_double_to_numeric_table",
            "BINARY_DOUBLE",
            "numeric",
            DOUBLE,
            BINARY_DOUBLE_TO_NUMERIC_VALUES_2),
        table("integer_table", "INTEGER", "bigint", NUMBER, NUMBER_TO_INT64_VALUES),
        table(
            "integer_to_numeric_table", "INTEGER", "numeric", NUMBER, INTEGER_TO_NUMERIC_VALUES_2),
        table("integer_to_varchar_table", "INTEGER", "varchar", NUMBER, INTEGER_TO_STR_VALUES),
        table("integer_to_float8_table", "INTEGER", "float8", NUMBER, INTEGER_TO_FLOAT64_VALUES),
        pk("integer_pk_table", "INTEGER", "bigint", NUMBER, INTEGER_PK_VALUES),
        table("int_table", "INT", "bigint", NUMBER, NUMBER_TO_INT64_VALUES),
        table("int_to_numeric_table", "INT", "numeric", NUMBER, INTEGER_TO_NUMERIC_VALUES_2),
        table("int_to_varchar_table", "INT", "varchar", NUMBER, INTEGER_TO_STR_VALUES),
        table("int_to_float8_table", "INT", "float8", NUMBER, INTEGER_TO_FLOAT64_VALUES),
        pk("int_pk_table", "INT", "bigint", NUMBER, INTEGER_PK_VALUES),
        table("smallint_table", "SMALLINT", "bigint", NUMBER, NUMBER_TO_INT64_VALUES),
        table(
            "smallint_to_numeric_table",
            "SMALLINT",
            "numeric",
            NUMBER,
            INTEGER_TO_NUMERIC_VALUES_2),
        table("smallint_to_varchar_table", "SMALLINT", "varchar", NUMBER, INTEGER_TO_STR_VALUES),
        table("smallint_to_float8_table", "SMALLINT", "float8", NUMBER, INTEGER_TO_FLOAT64_VALUES),
        pk("smallint_pk_table", "SMALLINT", "bigint", NUMBER, INTEGER_PK_VALUES),
        table("date_table", "DATE", "timestamptz", STRING, DATE_TEXT, DATE_VALUES),
        table("date_to_date_table", "DATE", "date", STRING, DATE_TEXT, DATE_TO_DATE_VALUES),
        table("date_to_varchar_table", "DATE", "varchar", STRING, DATE_TEXT, DATE_VALUES),
        table("date_to_bigint_table", "DATE", "bigint", STRING, DATE_TEXT, DATE_TO_INT64_VALUES),
        pk("date_pk_table", "DATE", "timestamptz", STRING, DATE_TEXT, DATE_PK_VALUES),
        table(
            "timestamp_table",
            "TIMESTAMP",
            "timestamptz",
            STRING,
            TIMESTAMP_TEXT,
            TIMESTAMP_VALUES),
        table(
            "timestamp_to_varchar_table",
            "TIMESTAMP",
            "varchar",
            STRING,
            TIMESTAMP_TEXT,
            TIMESTAMP_VALUES),
        table(
            "timestamp_to_bigint_table",
            "TIMESTAMP",
            "bigint",
            STRING,
            TIMESTAMP_TEXT,
            TIMESTAMP_TO_INT64_VALUES),
        pk(
            "timestamp_pk_table",
            "TIMESTAMP",
            "timestamptz",
            STRING,
            TIMESTAMP_TEXT,
            TIMESTAMP_PK_VALUES),
        table(
            "timestamp_tz_table",
            "TIMESTAMP WITH TIME ZONE",
            "timestamptz",
            STRING,
            TIMESTAMP_UTC_TEXT,
            TIMESTAMP_VALUES),
        table(
            "timestamp_tz_to_varchar_table",
            "TIMESTAMP WITH TIME ZONE",
            "varchar",
            STRING,
            TIMESTAMP_UTC_TEXT,
            TIMESTAMP_VALUES),
        table(
            "timestamp_tz_to_bigint_table",
            "TIMESTAMP WITH TIME ZONE",
            "bigint",
            STRING,
            TIMESTAMP_UTC_TEXT,
            TIMESTAMP_TO_INT64_VALUES),
        table(
            "timestamp_ltz_table",
            "TIMESTAMP WITH LOCAL TIME ZONE",
            "timestamptz",
            STRING,
            TIMESTAMP_UTC_TEXT,
            TIMESTAMP_VALUES),
        table(
            "timestamp_ltz_to_varchar_table",
            "TIMESTAMP WITH LOCAL TIME ZONE",
            "varchar",
            STRING,
            TIMESTAMP_UTC_TEXT,
            TIMESTAMP_VALUES),
        table(
            "timestamp_ltz_to_bigint_table",
            "TIMESTAMP WITH LOCAL TIME ZONE",
            "bigint",
            STRING,
            TIMESTAMP_UTC_TEXT,
            TIMESTAMP_TO_INT64_VALUES),
        pk(
            "timestamp_ltz_pk_table",
            "TIMESTAMP WITH LOCAL TIME ZONE",
            "timestamptz",
            STRING,
            TIMESTAMP_UTC_TEXT,
            TIMESTAMP_PK_VALUES),
        table("raw_table", "RAW", "bytea", BYTES, RAW_VALUES),
        table("raw_to_varchar_table", "RAW", "varchar", BYTES, RAW_TO_STR_VALUES),
        pk("raw_pk_table", "RAW", "bytea", BYTES, RAW_PK_VALUES),
        table("blob_table", "BLOB", "bytea", BYTES, BLOB_VALUES),
        table("blob_to_varchar_table", "BLOB", "varchar", BYTES, BLOB_VALUES),
        table("clob_table", "CLOB", "varchar", STRING, CLOB_VALUES),
        table("nclob_table", "NCLOB", "varchar", STRING, CLOB_VALUES));
  }

  /** Unsupported-type tables of {@link SpannerToOracleDataTypesPGDialectIT}. */
  static List<UnsupportedTable> postgresUnsupportedTables() {
    return List.of(
        unsupported(
            "interval_year_to_month_table",
            "INTERVAL YEAR TO MONTH",
            "varchar",
            "+999999999-11",
            "-999999999-11",
            null),
        unsupported(
            "interval_day_to_second_table",
            "INTERVAL DAY TO SECOND",
            "varchar",
            "+999999999 23:59:59.999999999",
            "-999999999 23:59:59.999999999",
            null),
        unsupported("long_raw_table", "LONG RAW", "bytea", "", "QQ==", null),
        unsupported("bfile_table", "BFILE", "varchar", "bfile('DIR', 'file.txt')", null),
        unsupported(
            "long_table",
            "LONG",
            "varchar",
            "",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            null),
        unsupported("rowid_table", "ROWID", "varchar", "AAAB12AADAAAAwPAAA", null),
        unsupported("urowid_table", "UROWID", "varchar", "AAAB12AADAAAAwPAAA", null),
        unsupported("xmltype_table", "XMLType", "varchar", "<xml></xml>", "<bad_xml", null),
        unsupported("anydata_table", "ANYDATA", "bytea", "BQ==", null),
        unsupported("anytype_table", "ANYTYPE", "bytea", "BQ==", null),
        unsupported("anydataset_table", "ANYDATASET", "bytea", "BQ==", null),
        unsupported("uritype_table", "URIType", "varchar", "http://test.com", null),
        unsupported("dburitype_table", "DBUriType", "varchar", "/PUBLIC/DUAL", null),
        unsupported("xdburitype_table", "XDBUriType", "varchar", "/public", null),
        unsupported("httpuritype_table", "HttpUriType", "varchar", "http://test.com", null),
        unsupported("mlslabel_table", "MLSLABEL", "varchar", "SENSITIVE", null),
        unsupported("ref_table", "REF", "varchar", "REF(obj)", null),
        unsupported("varray_table", "VARRAY", "varchar[]", "[]", "[\"A\",\"B\"]", null),
        unsupported("nested_table_table", "NESTED TABLE", "jsonb", "{}", "{\"id\":1}", null),
        unsupported("object_type_table", "OBJECT TYPE", "jsonb", "{\"attr1\":\"val\"}", null));
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static TypeTable table(
      String table, String sourceType, String spannerType, Compare compare, List<Row> rows) {
    return table(table, sourceType, spannerType, compare, COLUMN, rows);
  }

  private static TypeTable table(
      String table,
      String sourceType,
      String spannerType,
      Compare compare,
      String readTemplate,
      List<Row> rows) {
    return new TypeTable(table, sourceType, spannerType, false, compare, readTemplate, rows);
  }

  private static TypeTable pk(
      String table, String sourceType, String spannerType, Compare compare, List<Row> rows) {
    return pk(table, sourceType, spannerType, compare, COLUMN, rows);
  }

  private static TypeTable pk(
      String table,
      String sourceType,
      String spannerType,
      Compare compare,
      String readTemplate,
      List<Row> rows) {
    return new TypeTable(table, sourceType, spannerType, true, compare, readTemplate, rows);
  }

  private static UnsupportedTable unsupported(
      String table, String sourceType, String spannerType, String... values) {
    return new UnsupportedTable(table, sourceType, spannerType, Arrays.asList(values));
  }

  /** A row whose expected Oracle value equals the value written to Spanner. */
  private static Row same(String value) {
    return new Row(value, value);
  }

  private static Row row(String spanner, String expected) {
    return new Row(spanner, expected);
  }

  /** {@code prefix} followed by {@code unit} repeated {@code times} times. */
  private static String repeat(String prefix, String unit, int times) {
    return prefix + unit.repeat(times);
  }

  private static String repeat(String prefix, String unit, int times, String suffix) {
    return repeat(prefix, unit, times) + suffix;
  }

  /** Base64 of the UTF-8 bytes of {@link #repeat(String, String, int)}, for BYTES columns. */
  private static String repeatBase64(String prefix, String unit, int times) {
    return base64(repeat(prefix, unit, times).getBytes(StandardCharsets.UTF_8));
  }

  private static String repeatBase64(String prefix, String unit, int times, String suffix) {
    return base64(repeat(prefix, unit, times, suffix).getBytes(StandardCharsets.UTF_8));
  }

  /** Base64 of the bytes given by {@code hex} repeated {@code times} times. */
  private static String hexBase64(String hex, int times) {
    return base64(HexFormat.of().parseHex(hex.repeat(times)));
  }

  private static String base64(byte[] bytes) {
    return Base64.getEncoder().encodeToString(bytes);
  }
}
