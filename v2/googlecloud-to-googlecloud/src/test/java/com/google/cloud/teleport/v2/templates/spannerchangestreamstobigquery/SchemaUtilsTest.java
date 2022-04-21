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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerColumn;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerTable;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.SpannerToBigQueryUtils;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.SpannerUtils;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link SchemaUtilsTest}. */
@RunWith(MockitoJUnitRunner.class)
public class SchemaUtilsTest {

  private static final String changeStreamName = "changeStreamName";
  @Mock private DatabaseClient mockDatabaseClient;
  @Mock private ReadContext mockReadContext;
  private List<TrackedSpannerColumn> spannerColumnsOfAllTypes;

  @Before
  public void setUp() {
    when(mockDatabaseClient.singleUse()).thenReturn(mockReadContext);
    spannerColumnsOfAllTypes =
        ImmutableList.of(
            TrackedSpannerColumn.create("BoolCol", Type.bool(), 1),
            TrackedSpannerColumn.create("BytesCol", Type.bytes(), 2),
            TrackedSpannerColumn.create("DateCol", Type.date(), 3),
            TrackedSpannerColumn.create("Float64Col", Type.float64(), 4),
            TrackedSpannerColumn.create("Int64Col", Type.int64(), 5),
            TrackedSpannerColumn.create("JsonCol", Type.json(), 6),
            TrackedSpannerColumn.create("NumericCol", Type.numeric(), 7),
            TrackedSpannerColumn.create("StringCol", Type.string(), 8),
            TrackedSpannerColumn.create("TimestampCol", Type.timestamp(), 9),
            TrackedSpannerColumn.create("BoolArrayCol", Type.array(Type.bool()), 10),
            TrackedSpannerColumn.create("BytesArrayCol", Type.array(Type.bytes()), 11),
            TrackedSpannerColumn.create("DateArrayCol", Type.array(Type.date()), 12),
            TrackedSpannerColumn.create("Float64ArrayCol", Type.array(Type.float64()), 13),
            TrackedSpannerColumn.create("Int64ArrayCol", Type.array(Type.int64()), 14),
            TrackedSpannerColumn.create("JsonArrayCol", Type.array(Type.json()), 15),
            TrackedSpannerColumn.create("NumericArrayCol", Type.array(Type.numeric()), 16),
            TrackedSpannerColumn.create("StringArrayCol", Type.array(Type.string()), 17),
            TrackedSpannerColumn.create("TimestampArrayCol", Type.array(Type.timestamp()), 18));
  }

  @Test
  public void testChangeStreamTrackAll() {
    mockInformationSchemaChangeStreamsQuery(true);
    mockInformationSchemaTablesQuery();
    mockInformationSchemaColumnsQuery();
    mockInformationSchemaKeyColumnUsageQuery();
    String sql =
        "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
            + "WHERE CHANGE_STREAM_NAME = @changeStreamName";
    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName).build()))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("TABLE_NAME", Type.string()),
                    Type.StructField.of("COLUMN_NAME", Type.string())),
                Collections.emptyList()));

    Map<String, TrackedSpannerTable> actualSpannerTableByName =
        new SpannerUtils(mockDatabaseClient, changeStreamName).getSpannerTableByName();

    List<TrackedSpannerColumn> singersPkColumns =
        ImmutableList.of(TrackedSpannerColumn.create("SingerId", Type.int64(), 1));
    List<TrackedSpannerColumn> singersNonPkColumns =
        ImmutableList.of(
            TrackedSpannerColumn.create("FirstName", Type.string(), 2),
            TrackedSpannerColumn.create("LastName", Type.string(), 3));
    Map<String, TrackedSpannerTable> expectedSpannerTableByName = new HashMap<>();
    expectedSpannerTableByName.put(
        "Singers", new TrackedSpannerTable("Singers", singersPkColumns, singersNonPkColumns));
    assertThat(actualSpannerTableByName).isEqualTo(expectedSpannerTableByName);
  }

  @Test
  public void testChangeStreamTrackOneTable() {
    mockInformationSchemaChangeStreamsQuery(false);
    mockInformationSchemaChangeStreamTablesQuery();
    mockInformationSchemaColumnsQuery();
    mockInformationSchemaKeyColumnUsageQuery();
    String sql =
        "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
            + "WHERE CHANGE_STREAM_NAME = @changeStreamName";
    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName).build()))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("TABLE_NAME", Type.string()),
                    Type.StructField.of("COLUMN_NAME", Type.string())),
                Collections.emptyList()));

    Map<String, TrackedSpannerTable> actualSpannerTableByName =
        new SpannerUtils(mockDatabaseClient, changeStreamName).getSpannerTableByName();

    List<TrackedSpannerColumn> singersPkColumns =
        ImmutableList.of(TrackedSpannerColumn.create("SingerId", Type.int64(), 1));
    List<TrackedSpannerColumn> singersNonPkColumns =
        ImmutableList.of(
            TrackedSpannerColumn.create("FirstName", Type.string(), 2),
            TrackedSpannerColumn.create("LastName", Type.string(), 3));
    Map<String, TrackedSpannerTable> expectedSpannerTableByName = new HashMap<>();
    expectedSpannerTableByName.put(
        "Singers", new TrackedSpannerTable("Singers", singersPkColumns, singersNonPkColumns));
    assertThat(actualSpannerTableByName).isEqualTo(expectedSpannerTableByName);
  }

  @Test
  public void testChangeStreamTrackTwoColumns() {
    mockInformationSchemaChangeStreamsQuery(false);
    mockInformationSchemaChangeStreamTablesQuery();
    mockInformationSchemaColumnsQuery();
    mockInformationSchemaKeyColumnUsageQuery();
    String sql =
        "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
            + "WHERE CHANGE_STREAM_NAME = @changeStreamName";
    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName).build()))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("TABLE_NAME", Type.string()),
                    Type.StructField.of("COLUMN_NAME", Type.string())),
                ImmutableList.of(
                    Struct.newBuilder()
                        .set("TABLE_NAME")
                        .to(Value.string("Singers"))
                        .set("COLUMN_NAME")
                        .to(Value.string("SingerId"))
                        .build(),
                    Struct.newBuilder()
                        .set("TABLE_NAME")
                        .to(Value.string("Singers"))
                        .set("COLUMN_NAME")
                        .to(Value.string("FirstName"))
                        .build())));

    Map<String, TrackedSpannerTable> actualSpannerTableByName =
        new SpannerUtils(mockDatabaseClient, changeStreamName).getSpannerTableByName();

    List<TrackedSpannerColumn> singersPkColumns =
        Collections.singletonList(TrackedSpannerColumn.create("SingerId", Type.int64(), 1));
    List<TrackedSpannerColumn> singersNonPkColumns =
        Collections.singletonList(TrackedSpannerColumn.create("FirstName", Type.string(), 2));
    Map<String, TrackedSpannerTable> expectedSpannerTableByName = new HashMap<>();
    expectedSpannerTableByName.put(
        "Singers", new TrackedSpannerTable("Singers", singersPkColumns, singersNonPkColumns));
    assertThat(actualSpannerTableByName).isEqualTo(expectedSpannerTableByName);
  }

  @Test
  public void testSpannerSnapshotRowToBigQueryTableRow() {
    TableRow tableRow = new TableRow();
    Boolean[] booleanArray = {true, false, true};
    ByteArray[] bytesArray = {
      ByteArray.copyFrom("123"), ByteArray.copyFrom("456"), ByteArray.copyFrom("789")
    };
    Date[] dateArray = {Date.fromYearMonthDay(2022, 1, 22), Date.fromYearMonthDay(2022, 3, 11)};
    double[] float64Array = {Double.MIN_VALUE, Double.MAX_VALUE, 0, 1, -1, 1.2341};
    long[] int64Array = {Long.MAX_VALUE, Long.MIN_VALUE, 0, 1, -1};
    String[] jsonArray = {"{}", "{\"color\":\"red\",\"value\":\"#f00\"}", "[]"};
    BigDecimal[] numericArray = {
      BigDecimal.valueOf(1, Integer.MAX_VALUE),
      BigDecimal.valueOf(1, Integer.MIN_VALUE),
      BigDecimal.ZERO,
      BigDecimal.TEN,
      BigDecimal.valueOf(3141592, 6)
    };
    String[] stringArray = {"abc", "def", "ghi"};
    Timestamp[] timestampArray = {
      Timestamp.ofTimeSecondsAndNanos(1646617853L, 972000000),
      Timestamp.ofTimeSecondsAndNanos(1646637853L, 572000000),
      Timestamp.ofTimeSecondsAndNanos(1646657853L, 772000000)
    };
    List<Type.StructField> structFields = new ArrayList<>(spannerColumnsOfAllTypes.size());
    for (TrackedSpannerColumn spannerColumn : spannerColumnsOfAllTypes) {
      structFields.add(Type.StructField.of(spannerColumn.getName(), spannerColumn.getType()));
    }
    // spotless:off
    ResultSet resultSet =
        ResultSets.forRows(
            Type.struct(structFields),
            Collections.singletonList(
                Struct.newBuilder()
                    .set("BoolCol").to(Value.bool(true))
                    .set("BytesCol").to(Value.bytes(ByteArray.copyFrom("123")))
                    .set("DateCol").to(Date.fromYearMonthDay(2022, 1, 22))
                    .set("Float64Col").to(Value.float64(1.2))
                    .set("Int64Col").to(Value.int64(20))
                    .set("JsonCol").to(Value.json("{\"color\":\"red\",\"value\":\"#f00\"}"))
                    .set("NumericCol").to(Value.numeric(BigDecimal.valueOf(123, 2)))
                    .set("StringCol").to(Value.string("abc"))
                    .set("TimestampCol").to(Timestamp.ofTimeSecondsAndNanos(1646617853L, 972000000))
                    .set("BoolArrayCol").to(Value.boolArray(Arrays.asList(booleanArray)))
                    .set("BytesArrayCol").to(Value.bytesArray(Arrays.asList(bytesArray)))
                    .set("DateArrayCol").to(Value.dateArray(Arrays.asList(dateArray)))
                    .set("Float64ArrayCol").to(Value.float64Array(float64Array))
                    .set("Int64ArrayCol").to(Value.int64Array(int64Array))
                    .set("JsonArrayCol").to(Value.jsonArray(Arrays.asList(jsonArray)))
                    .set("NumericArrayCol").to(Value.numericArray(Arrays.asList(numericArray)))
                    .set("StringArrayCol").to(Value.stringArray(Arrays.asList(stringArray)))
                    .set("TimestampArrayCol")
                    .to(Value.timestampArray(Arrays.asList(timestampArray)))
                    .build()));
    // spotless:on
    SpannerToBigQueryUtils.spannerSnapshotRowToBigQueryTableRow(
        resultSet, spannerColumnsOfAllTypes, tableRow);

    assertThat(tableRow.toString())
        .isEqualTo(
            "GenericData{classInfo=[f], {BoolCol=true, BytesCol=MTIz, DateCol=2022-01-22,"
                + " Float64Col=1.2, Int64Col=20, JsonCol={\"color\":\"red\",\"value\":\"#f00\"},"
                + " NumericCol=1.23, StringCol=abc, TimestampCol=2022-03-07T01:50:53.972000000Z,"
                + " BoolArrayCol=[true, false, true], BytesArrayCol=[MTIz, NDU2, Nzg5],"
                + " DateArrayCol=[2022-01-22, 2022-03-11], Float64ArrayCol=[4.9E-324,"
                + " 1.7976931348623157E308, 0.0, 1.0, -1.0, 1.2341],"
                + " Int64ArrayCol=[9223372036854775807, -9223372036854775808, 0, 1, -1],"
                + " JsonArrayCol=[{}, {\"color\":\"red\",\"value\":\"#f00\"}, []],"
                + " NumericArrayCol=[1E-2147483647, 1E+2147483648, 0, 10, 3.141592],"
                + " StringArrayCol=[abc, def, ghi],"
                + " TimestampArrayCol=[2022-03-07T01:50:53.972000000Z,"
                + " 2022-03-07T07:24:13.572000000Z, 2022-03-07T12:57:33.772000000Z]}}");
  }

  @Test
  public void testAppendToSpannerKey() {
    JSONObject keysJsonObject = new JSONObject();
    keysJsonObject.put("BoolCol", true);
    keysJsonObject.put("BytesCol", ByteArray.copyFrom("123").toBase64());
    keysJsonObject.put("DateCol", "2022-01-22");
    keysJsonObject.put("Float64Col", 1.2);
    keysJsonObject.put("Int64Col", 20);
    keysJsonObject.put("NumericCol", new BigDecimal(3.141592));
    keysJsonObject.put("StringCol", "abc");
    keysJsonObject.put("TimestampCol", "2022-03-07T01:50:53.972000000Z");
    Key.Builder keyBuilder = com.google.cloud.spanner.Key.newBuilder();
    for (TrackedSpannerColumn spannerColumn : spannerColumnsOfAllTypes) {
      String typeName = spannerColumn.getType().getCode().name();
      // Array and JSON are not valid Spanner key type.
      if (typeName.equals("ARRAY") || typeName.equals("JSON")) {
        continue;
      }
      SpannerUtils.appendToSpannerKey(spannerColumn, keysJsonObject, keyBuilder);
    }

    assertThat(keyBuilder.build().toString())
        .isEqualTo(
            "[true,MTIz,2022-01-22,1.2,20,"
                + "3.14159200000000016217427400988526642322540283203125,abc,"
                + "2022-03-07T01:50:53.972000000Z]");
  }

  @Test
  public void testSpannerColumnsToBigQueryIOFields() {
    String bigQueryIOFieldsStr =
        SpannerToBigQueryUtils.spannerColumnsToBigQueryIOFields(spannerColumnsOfAllTypes)
            .toString();
    // Remove redundant information.
    bigQueryIOFieldsStr =
        bigQueryIOFieldsStr.replace(
            "classInfo=[categories, collationSpec, description, fields, maxLength, mode, name,"
                + " policyTags, precision, scale, type], ",
            "");
    bigQueryIOFieldsStr = bigQueryIOFieldsStr.replace("GenericData", "");

    assertThat(bigQueryIOFieldsStr)
        .isEqualTo(
            "[{{mode=NULLABLE, name=BoolCol, type=BOOL}}, "
                + "{{mode=NULLABLE, name=BytesCol, type=BYTES}}, "
                + "{{mode=NULLABLE, name=DateCol, type=DATE}}, "
                + "{{mode=NULLABLE, name=Float64Col, type=FLOAT64}}, "
                + "{{mode=NULLABLE, name=Int64Col, type=INT64}}, "
                + "{{mode=NULLABLE, name=JsonCol, type=STRING}}, "
                + "{{mode=NULLABLE, name=NumericCol, type=NUMERIC}}, "
                + "{{mode=NULLABLE, name=StringCol, type=STRING}}, "
                + "{{mode=NULLABLE, name=TimestampCol, type=TIMESTAMP}}, "
                + "{{mode=REPEATED, name=BoolArrayCol, type=BOOL}}, "
                + "{{mode=REPEATED, name=BytesArrayCol, type=BYTES}}, "
                + "{{mode=REPEATED, name=DateArrayCol, type=DATE}}, "
                + "{{mode=REPEATED, name=Float64ArrayCol, type=FLOAT64}}, "
                + "{{mode=REPEATED, name=Int64ArrayCol, type=INT64}}, "
                + "{{mode=REPEATED, name=JsonArrayCol, type=STRING}}, "
                + "{{mode=REPEATED, name=NumericArrayCol, type=NUMERIC}}, "
                + "{{mode=REPEATED, name=StringArrayCol, type=STRING}}, "
                + "{{mode=REPEATED, name=TimestampArrayCol, type=TIMESTAMP}}]");
  }

  @Test
  public void testAddSpannerNonPkColumnsToTableRow() {
    String newValuesJson =
        "{\"BoolCol\":true,\"BytesCol\":\"ZmZm\",\"DateCol\":\"2020-12-12\",\"Float64Col\":1.3,"
            + "\"Int64Col\":\"5\","
            + "\"JsonCol\":\"{\\\"color\\\":\\\"red\\\",\\\"value\\\":\\\"#f00\\\"}\","
            + "\"NumericCol\":\"4.4\",\"StringCol\":\"abc\","
            + "\"TimestampCol\":\"2022-03-19T18:51:33.963910279Z\",\"BoolArrayCol\":[true,false],"
            + "\"BytesArrayCol\":[\"YWJj\",\"YmNk\"],"
            + "\"DateArrayCol\":[\"2021-01-22\",\"2022-01-01\"],\"Float64ArrayCol\":[1.2,4.4],"
            + "\"Int64ArrayCol\":[\"1\",\"2\"],"
            + "\"JsonArrayCol\":[\"{}\",\"{\\\"color\\\":\\\"red\\\",\\\"value\\\":\\\"#f00\\\"}\","
            + "\"[]\"],"
            + "\"NumericArrayCol\":[\"2.2\",\"3.3\"],\"StringArrayCol\":[\"a\",\"b\"],"
            + "\"TimestampArrayCol\":[\"2022-03-19T18:51:33.963910279Z\","
            + "\"2022-03-19T18:51:33.963910279Z\"]}";
    TableRow tableRow = new TableRow();
    SpannerToBigQueryUtils.addSpannerNonPkColumnsToTableRow(
        newValuesJson, spannerColumnsOfAllTypes, tableRow);

    assertThat(tableRow.toString())
        .isEqualTo(
            "GenericData{classInfo=[f], "
                + "{BoolCol=true, BytesCol=ZmZm, DateCol=2020-12-12, Float64Col=1.3, Int64Col=5, "
                + "JsonCol={\"color\":\"red\",\"value\":\"#f00\"}, NumericCol=4.4, StringCol=abc, "
                + "TimestampCol=2022-03-19T18:51:33.963910279Z, BoolArrayCol=[true, false], "
                + "BytesArrayCol=[YWJj, YmNk], DateArrayCol=[2021-01-22, 2022-01-01], "
                + "Float64ArrayCol=[1.2, 4.4], Int64ArrayCol=[1, 2], "
                + "JsonArrayCol=[{}, {\"color\":\"red\",\"value\":\"#f00\"}, []], "
                + "NumericArrayCol=[2.2, 3.3], StringArrayCol=[a, b], "
                + "TimestampArrayCol=[2022-03-19T18:51:33.963910279Z, "
                + "2022-03-19T18:51:33.963910279Z]}}");
  }

  private void mockInformationSchemaChangeStreamsQuery(boolean isTrackingAll) {
    String sql =
        "SELECT CHANGE_STREAMS.ALL FROM INFORMATION_SCHEMA.CHANGE_STREAMS "
            + "WHERE CHANGE_STREAM_NAME = @changeStreamName";

    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName).build()))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(Type.StructField.of("ALL", Type.bool())),
                Collections.singletonList(
                    Struct.newBuilder().set("ALL").to(Value.bool(isTrackingAll)).build())));
  }

  private void mockInformationSchemaTablesQuery() {
    String sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \"\"";

    when(mockReadContext.executeQuery(Statement.of(sql)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(Type.StructField.of("TABLE_NAME", Type.string())),
                ImmutableList.of(
                    Struct.newBuilder().set("TABLE_NAME").to(Value.string("Singers")).build())));
  }

  private void mockInformationSchemaColumnsQuery() {
    String sql =
        "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, SPANNER_TYPE "
            + "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME IN UNNEST (@tableNames)";
    List<String> tableNames = new ArrayList<>();
    tableNames.add("Singers");
    // spotless:off
    List<Struct> rows =
        new ArrayList<>(
            ImmutableList.of(
                Struct.newBuilder()
                    .set("TABLE_NAME").to(Value.string("Singers"))
                    .set("COLUMN_NAME").to(Value.string("SingerId"))
                    .set("ORDINAL_POSITION").to(Value.int64(1))
                    .set("SPANNER_TYPE").to(Value.string("INT64"))
                    .build(),
                Struct.newBuilder()
                    .set("TABLE_NAME").to(Value.string("Singers"))
                    .set("COLUMN_NAME").to(Value.string("FirstName"))
                    .set("ORDINAL_POSITION").to(Value.int64(2))
                    .set("SPANNER_TYPE").to(Value.string("STRING(1024)"))
                    .build(),
                Struct.newBuilder()
                    .set("TABLE_NAME").to(Value.string("Singers"))
                    .set("COLUMN_NAME").to(Value.string("LastName"))
                    .set("ORDINAL_POSITION").to(Value.int64(3))
                    .set("SPANNER_TYPE").to(Value.string("STRING"))
                    .build()));
    // spotless:on

    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql)
                .bind("tableNames")
                .to(Value.stringArray(new ArrayList<>(tableNames)))
                .build()))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("TABLE_NAME", Type.string()),
                    Type.StructField.of("COLUMN_NAME", Type.string()),
                    Type.StructField.of("ORDINAL_POSITION", Type.int64()),
                    Type.StructField.of("SPANNER_TYPE", Type.string())),
                rows));
  }

  private void mockInformationSchemaChangeStreamTablesQuery() {
    String sql =
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES "
            + "WHERE CHANGE_STREAM_NAME = @changeStreamName";

    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName).build()))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(Type.StructField.of("TABLE_NAME", Type.string())),
                Collections.singletonList(
                    Struct.newBuilder().set("TABLE_NAME").to(Value.string("Singers")).build())));
  }

  private void mockInformationSchemaKeyColumnUsageQuery() {
    String sql =
        "SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
            + "WHERE TABLE_NAME IN UNNEST (@tableNames)";
    List<String> tableNames = new ArrayList<>();
    tableNames.add("Singers");
    // spotless:off
    List<Struct> rows =
        new ArrayList<>(
            Collections.singletonList(
                Struct.newBuilder()
                    .set("TABLE_NAME").to(Value.string("Singers"))
                    .set("COLUMN_NAME").to(Value.string("SingerId"))
                    .set("CONSTRAINT_NAME").to(Value.string("PK_Singers"))
                    .build()));
    // spotless:on

    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql).bind("tableNames").to(Value.stringArray(tableNames)).build()))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("TABLE_NAME", Type.string()),
                    Type.StructField.of("COLUMN_NAME", Type.string()),
                    Type.StructField.of("CONSTRAINT_NAME", Type.string())),
                rows));
  }
}
