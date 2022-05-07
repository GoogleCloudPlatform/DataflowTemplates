/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.SpannerServerResource;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;

/** {@link TestUtils} provides methods for testing. */
class TestUtils {

  static final String TEST_PROJECT = "span-cloud-testing";
  static final String TEST_SPANNER_INSTANCE = "changestream";
  static final String TEST_SPANNER_DATABASE_PREFIX = "testdbchangestreams";
  static final String TEST_SPANNER_HOST = "https://spanner.googleapis.com";
  static final String TEST_SPANNER_TABLE = "AllTypes";
  static final String TEST_SPANNER_CHANGE_STREAM = "AllTypesStream";
  static final String TEST_BIG_QUERY_DATESET = "dataset";
  static final int MAX_TABLE_NAME_LENGTH = 29;

  static final String BOOLEAN_PK_COL = "BooleanPkCol";
  static final String BYTES_PK_COL = "BytesPkCol";
  static final String DATE_PK_COL = "DatePkCol";
  static final String FLOAT64_PK_COL = "Float64PkCol";
  static final String INT64_PK_COL = "Int64PkCol";
  static final String NUMERIC_PK_COL = "NumericPkCol";
  static final String STRING_PK_COL = "StringPkCol";
  static final String TIMESTAMP_PK_COL = "TimestampPkCol";

  static final String BOOLEAN_ARRAY_COL = "BooleanArrayCol";
  static final String BYTES_ARRAY_COL = "BytesArrayCol";
  static final String DATE_ARRAY_COL = "DateArrayCol";
  static final String FLOAT64_ARRAY_COL = "Float64ArrayCol";
  static final String INT64_ARRAY_COL = "Int64ArrayCol";
  static final String NUMERIC_ARRAY_COL = "NumericArrayCol";
  static final String JSON_ARRAY_COL = "JsonArrayCol";
  static final String STRING_ARRAY_COL = "StringArrayCol";
  static final String TIMESTAMP_ARRAY_COL = "TimestampArrayCol";

  static final String BOOLEAN_COL = "BooleanCol";
  static final String BYTES_COL = "BytesCol";
  static final String DATE_COL = "DateCol";
  static final String FLOAT64_COL = "Float64Col";
  static final String INT64_COL = "Int64Col";
  static final String JSON_COL = "JsonCol";
  static final String NUMERIC_COL = "NumericCol";
  static final String STRING_COL = "StringCol";
  static final String TIMESTAMP_COL = "TimestampCol";

  static final Boolean BOOLEAN_RAW_VAL = true;
  static final ByteArray BYTES_RAW_VAL = ByteArray.copyFrom("456");
  static final Date DATE_RAW_VAL = Date.fromYearMonthDay(2022, 3, 11);
  static final Double FLOAT64_RAW_VAL = 2.5;
  static final Long INT64_RAW_VAL = 10L;
  static final String JSON_RAW_VAL = "{\"color\":\"red\"}";
  static final BigDecimal NUMERIC_RAW_VAL = BigDecimal.TEN;
  static final String STRING_RAW_VAL = "abc";
  static final Timestamp TIMESTAMP_RAW_VAL =
      Timestamp.ofTimeSecondsAndNanos(1646617853L, 972000000);

  static final Value BOOLEAN_VAL = Value.bool(BOOLEAN_RAW_VAL);
  static final Value BYTES_VAL = Value.bytes(BYTES_RAW_VAL);
  static final Value DATE_VAL = Value.date(DATE_RAW_VAL);
  static final Value FLOAT64_VAL = Value.float64(FLOAT64_RAW_VAL);
  static final Value INT64_VAL = Value.int64(INT64_RAW_VAL);
  static final Value JSON_VAL = Value.json(JSON_RAW_VAL);
  static final Value NUMERIC_VAL = Value.numeric(NUMERIC_RAW_VAL);
  static final Value STRING_VAL = Value.string(STRING_RAW_VAL);
  static final Value TIMESTAMP_VAL = Value.timestamp(TIMESTAMP_RAW_VAL);

  static final List<Boolean> BOOLEAN_ARRAY_RAW_VAL = Arrays.asList(true, false, true);
  static final List<String> BYTES_ARRAY_RAW_VAL =
      Arrays.asList(
          ByteArray.copyFrom("123").toBase64(),
          ByteArray.copyFrom("456").toBase64(),
          ByteArray.copyFrom("789").toBase64());
  static final List<Date> DATE_ARRAY_RAW_VAL =
      Arrays.asList(Date.fromYearMonthDay(2022, 1, 22), Date.fromYearMonthDay(2022, 3, 11));
  static final List<Double> FLOAT64_ARRAY_RAW_VAL =
      Arrays.asList(Double.MIN_VALUE, Double.MAX_VALUE, 0.0, 1.0, -1.0, 1.2341);
  static final List<Long> INT64_ARRAY_RAW_VAL =
      Arrays.asList(Long.MAX_VALUE, Long.MIN_VALUE, 0L, 1L, -1L);
  static final List<String> JSON_ARRAY_RAW_VAL =
      Arrays.asList("{}", "{\"color\":\"red\",\"value\":\"#f00\"}", "[]");
  static final List<BigDecimal> NUMERIC_ARRAY_RAW_VAL =
      Arrays.asList(BigDecimal.ZERO, BigDecimal.TEN, BigDecimal.valueOf(3141592, 6));
  static final List<String> STRING_ARRAY_RAW_VAL = Arrays.asList("abc", "def", "ghi");
  static final List<Timestamp> TIMESTAMP_ARRAY_RAW_VAL =
      Arrays.asList(
          Timestamp.ofTimeSecondsAndNanos(1646617853L, 972000000),
          Timestamp.ofTimeSecondsAndNanos(1646637853L, 572000000),
          Timestamp.ofTimeSecondsAndNanos(1646657853L, 772000000));

  static final List<Boolean> BOOLEAN_NULLABLE_ARRAY_RAW_VAL = addNull(BOOLEAN_ARRAY_RAW_VAL);
  static final List<String> BYTES_NULLABLE_ARRAY_RAW_VAL = addNull(BYTES_ARRAY_RAW_VAL);
  static final List<Date> DATE_NULLABLE_ARRAY_RAW_VAL = addNull(DATE_ARRAY_RAW_VAL);
  static final List<Double> FLOAT64_NULLABLE_ARRAY_RAW_VAL = addNull(FLOAT64_ARRAY_RAW_VAL);
  static final List<Long> INT64_NULLABLE_ARRAY_RAW_VAL = addNull(INT64_ARRAY_RAW_VAL);
  static final List<String> JSON_NULLABLE_ARRAY_RAW_VAL = addNull(JSON_ARRAY_RAW_VAL);
  static final List<BigDecimal> NUMERIC_NULLABLE_ARRAY_RAW_VAL = addNull(NUMERIC_ARRAY_RAW_VAL);
  static final List<String> STRING_NULLABLE_ARRAY_RAW_VAL = addNull(STRING_ARRAY_RAW_VAL);
  static final List<Timestamp> TIMESTAMP_NULLABLE_ARRAY_RAW_VAL = addNull(TIMESTAMP_ARRAY_RAW_VAL);

  static final Value BOOLEAN_NULLABLE_ARRAY_VAL = Value.boolArray(BOOLEAN_ARRAY_RAW_VAL);
  static final Value BYTES_NULLABLE_ARRAY_VAL =
      Value.bytesArray(
          Arrays.asList(
              ByteArray.copyFrom("123"),
              ByteArray.copyFrom("456"),
              ByteArray.copyFrom("789"),
              null));
  static final Value DATE_NULLABLE_ARRAY_VAL = Value.dateArray(DATE_NULLABLE_ARRAY_RAW_VAL);
  static final Value FLOAT64_NULLABLE_ARRAY_VAL =
      Value.float64Array(FLOAT64_NULLABLE_ARRAY_RAW_VAL);
  static final Value INT64_NULLABLE_ARRAY_VAL = Value.int64Array(INT64_NULLABLE_ARRAY_RAW_VAL);
  static final Value JSON_NULLABLE_ARRAY_VAL = Value.jsonArray(JSON_NULLABLE_ARRAY_RAW_VAL);
  static final Value NUMERIC_NULLABLE_ARRAY_VAL =
      Value.numericArray(NUMERIC_NULLABLE_ARRAY_RAW_VAL);
  static final Value STRING_NULLABLE_ARRAY_VAL = Value.stringArray(STRING_NULLABLE_ARRAY_RAW_VAL);
  static final Value TIMESTAMP_NULLABLE_ARRAY_VAL =
      Value.timestampArray(TIMESTAMP_NULLABLE_ARRAY_RAW_VAL);

  private static <T> List<T> addNull(List<T> list) {
    List<T> result = new ArrayList<>(list);
    result.add(null);
    return result;
  }

  private static String generateSpannerDatabaseName() {
    return TEST_SPANNER_DATABASE_PREFIX
        + "_"
        + RandomStringUtils.randomNumeric(
            MAX_TABLE_NAME_LENGTH - 1 - TEST_SPANNER_DATABASE_PREFIX.length());
  }

  // This methods does the following:
  // 1. Generate a random database name.
  // 2. Drop the database with the generated name if it already exists.
  // 3. Create a database with the generated name, the database has one table with columns of all
  // supported Spanner types, and it contains a change stream that watches this table.
  public static String createSpannerDatabase(SpannerServerResource spannerServer) throws Exception {
    String spannerDatabaseName = generateSpannerDatabaseName();
    spannerServer.dropDatabase(spannerDatabaseName);

    // spotless:off
    String createTableDdl =
        "CREATE TABLE "
        + TEST_SPANNER_TABLE
        + " ("
        + "BooleanPkCol BOOL,"
        + "BytesPkCol BYTES(1024),"
        + "DatePkCol DATE,"
        + "Float64PkCol FLOAT64,"
        + "Int64PkCol INT64,"
        + "NumericPkCol NUMERIC,"
        + "StringPkCol STRING(MAX),"
        + "TimestampPkCol TIMESTAMP OPTIONS (allow_commit_timestamp=true),"
        + "BooleanArrayCol ARRAY<BOOL>,"
        + "BytesArrayCol ARRAY<BYTES(1024)>,"
        + "DateArrayCol ARRAY<DATE>,"
        + "Float64ArrayCol ARRAY<FLOAT64>,"
        + "Int64ArrayCol ARRAY<INT64>,"
        + "JsonArrayCol ARRAY<JSON>,"
        + "NumericArrayCol ARRAY<NUMERIC>,"
        + "StringArrayCol ARRAY<STRING(1024)>,"
        + "TimestampArrayCol ARRAY<TIMESTAMP>,"
        + "BooleanCol BOOL,"
        + "BytesCol BYTES(1024),"
        + "DateCol DATE,"
        + "Float64Col FLOAT64,"
        + "Int64Col INT64,"
        + "JsonCol JSON,"
        + "NumericCol NUMERIC,"
        + "StringCol STRING(1024),"
        + "TimestampCol TIMESTAMP OPTIONS (allow_commit_timestamp=true)"
        + ") PRIMARY KEY(BooleanPkCol, BytesPkCol, DatePkCol, Float64PkCol, Int64PkCol,"
        + " NumericPkCol, StringPkCol, TimestampPkCol)";
    // spotless:on
    String createChangeStreamDdl =
        "CREATE CHANGE STREAM " + TEST_SPANNER_CHANGE_STREAM + " FOR " + TEST_SPANNER_TABLE;
    List<String> statements = Arrays.asList(createTableDdl, createChangeStreamDdl);
    spannerServer.createDatabase(spannerDatabaseName, statements);
    return spannerDatabaseName;
  }

  public static void dropSpannerDatabase(
      SpannerServerResource spannerServer, String spannerDatabaseName) {
    spannerServer.dropDatabase(spannerDatabaseName);
  }
}
