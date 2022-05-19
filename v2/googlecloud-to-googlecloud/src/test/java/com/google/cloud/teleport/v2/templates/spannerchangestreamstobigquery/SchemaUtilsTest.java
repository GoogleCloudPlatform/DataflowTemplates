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

import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BOOLEAN_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BOOLEAN_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BOOLEAN_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BOOLEAN_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BYTES_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BYTES_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BYTES_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BYTES_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.DATE_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.DATE_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.DATE_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.DATE_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT64_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT64_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT64_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT64_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.INT64_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.INT64_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.INT64_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.INT64_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.JSON_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.JSON_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.JSON_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.JSON_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.NUMERIC_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.NUMERIC_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.NUMERIC_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.NUMERIC_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_VAL;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.Field;
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
import java.util.ArrayList;
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
            TrackedSpannerColumn.create(BOOLEAN_COL, Type.bool(), 1),
            TrackedSpannerColumn.create(BYTES_COL, Type.bytes(), 2),
            TrackedSpannerColumn.create(DATE_COL, Type.date(), 3),
            TrackedSpannerColumn.create(FLOAT64_COL, Type.float64(), 4),
            TrackedSpannerColumn.create(INT64_COL, Type.int64(), 5),
            TrackedSpannerColumn.create(JSON_COL, Type.json(), 6),
            TrackedSpannerColumn.create(NUMERIC_COL, Type.numeric(), 7),
            TrackedSpannerColumn.create(STRING_COL, Type.string(), 8),
            TrackedSpannerColumn.create(TIMESTAMP_COL, Type.timestamp(), 9),
            TrackedSpannerColumn.create(BOOLEAN_ARRAY_COL, Type.array(Type.bool()), 10),
            TrackedSpannerColumn.create(BYTES_ARRAY_COL, Type.array(Type.bytes()), 11),
            TrackedSpannerColumn.create(DATE_ARRAY_COL, Type.array(Type.date()), 12),
            TrackedSpannerColumn.create(FLOAT64_ARRAY_COL, Type.array(Type.float64()), 13),
            TrackedSpannerColumn.create(INT64_ARRAY_COL, Type.array(Type.int64()), 14),
            TrackedSpannerColumn.create(JSON_ARRAY_COL, Type.array(Type.json()), 15),
            TrackedSpannerColumn.create(NUMERIC_ARRAY_COL, Type.array(Type.numeric()), 16),
            TrackedSpannerColumn.create(STRING_ARRAY_COL, Type.array(Type.string()), 17),
            TrackedSpannerColumn.create(TIMESTAMP_ARRAY_COL, Type.array(Type.timestamp()), 18));
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
    // spotless:off
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
        // spotless:on

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
                    .set(BOOLEAN_COL).to(BOOLEAN_VAL)
                    .set(BYTES_COL).to(BYTES_VAL)
                    .set(DATE_COL).to(DATE_VAL)
                    .set(FLOAT64_COL).to(FLOAT64_VAL)
                    .set(INT64_COL).to(INT64_VAL)
                    .set(JSON_COL).to(JSON_VAL)
                    .set(NUMERIC_COL).to(NUMERIC_VAL)
                    .set(STRING_COL).to(STRING_VAL)
                    .set(TIMESTAMP_COL).to(TIMESTAMP_VAL)
                    .set(BOOLEAN_ARRAY_COL).to(BOOLEAN_NULLABLE_ARRAY_VAL)
                    .set(BYTES_ARRAY_COL).to(BYTES_NULLABLE_ARRAY_VAL)
                    .set(DATE_ARRAY_COL).to(DATE_NULLABLE_ARRAY_VAL)
                    .set(FLOAT64_ARRAY_COL).to(FLOAT64_NULLABLE_ARRAY_VAL)
                    .set(INT64_ARRAY_COL).to(INT64_NULLABLE_ARRAY_VAL)
                    .set(JSON_ARRAY_COL).to(JSON_NULLABLE_ARRAY_VAL)
                    .set(NUMERIC_ARRAY_COL).to(NUMERIC_NULLABLE_ARRAY_VAL)
                    .set(STRING_ARRAY_COL).to(STRING_NULLABLE_ARRAY_VAL)
                    .set(TIMESTAMP_ARRAY_COL).to(TIMESTAMP_NULLABLE_ARRAY_VAL)
                    .build()));
    // spotless:on
    SpannerToBigQueryUtils.spannerSnapshotRowToBigQueryTableRow(
        resultSet, spannerColumnsOfAllTypes, tableRow);

    assertThat(tableRow.toString())
        .isEqualTo(
            "GenericData{classInfo=[f], {BooleanCol=true, BytesCol=NDU2, DateCol=2022-03-11,"
                + " Float64Col=2.5, Int64Col=10, JsonCol={\"color\":\"red\"}, NumericCol=10,"
                + " StringCol=abc, TimestampCol=2022-03-07T01:50:53.972000000Z,"
                + " BooleanArrayCol=[true, false, true], BytesArrayCol=[MTIz, NDU2, Nzg5],"
                + " DateArrayCol=[2022-01-22, 2022-03-11], Float64ArrayCol=[4.9E-324,"
                + " 1.7976931348623157E308, 0.0, 1.0, -1.0, 1.2341],"
                + " Int64ArrayCol=[9223372036854775807, -9223372036854775808, 0, 1, -1],"
                + " JsonArrayCol=[{}, {\"color\":\"red\",\"value\":\"#f00\"}, []],"
                + " NumericArrayCol=[0, 10, 3.141592], StringArrayCol=[abc, def, ghi],"
                + " TimestampArrayCol=[2022-03-07T01:50:53.972000000Z,"
                + " 2022-03-07T07:24:13.572000000Z, 2022-03-07T12:57:33.772000000Z]}}");
  }

  @Test
  public void testAppendToSpannerKey() {
    JSONObject keysJsonObject = new JSONObject();
    keysJsonObject.put(BOOLEAN_COL, BOOLEAN_VAL.getBool());
    keysJsonObject.put(BYTES_COL, BYTES_VAL.getBytes().toBase64());
    keysJsonObject.put(DATE_COL, DATE_VAL.getDate().toString());
    keysJsonObject.put(FLOAT64_COL, FLOAT64_VAL.getFloat64());
    keysJsonObject.put(INT64_COL, INT64_VAL.getInt64());
    keysJsonObject.put(NUMERIC_COL, NUMERIC_VAL.getNumeric());
    keysJsonObject.put(STRING_COL, STRING_VAL.getString());
    keysJsonObject.put(TIMESTAMP_COL, TIMESTAMP_VAL.toString());
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
        .isEqualTo("[true,NDU2,2022-03-11,2.5,10,10,abc,2022-03-07T01:50:53.972000000Z]");
  }

  @Test
  public void testSpannerColumnsToBigQueryIOFields() {
    List<TableFieldSchema> tableFields =
        ImmutableList.of(
            new TableFieldSchema()
                .setName(BOOLEAN_COL)
                .setMode(Field.Mode.NULLABLE.name())
                .setType("BOOL"),
            new TableFieldSchema()
                .setName(BYTES_COL)
                .setMode(Field.Mode.NULLABLE.name())
                .setType("BYTES"),
            new TableFieldSchema()
                .setName(DATE_COL)
                .setMode(Field.Mode.NULLABLE.name())
                .setType("DATE"),
            new TableFieldSchema()
                .setName(FLOAT64_COL)
                .setMode(Field.Mode.NULLABLE.name())
                .setType("FLOAT64"),
            new TableFieldSchema()
                .setName(INT64_COL)
                .setMode(Field.Mode.NULLABLE.name())
                .setType("INT64"),
            new TableFieldSchema()
                .setName(JSON_COL)
                .setMode(Field.Mode.NULLABLE.name())
                .setType("JSON"),
            new TableFieldSchema()
                .setName(NUMERIC_COL)
                .setMode(Field.Mode.NULLABLE.name())
                .setType("NUMERIC"),
            new TableFieldSchema()
                .setName(STRING_COL)
                .setMode(Field.Mode.NULLABLE.name())
                .setType("STRING"),
            new TableFieldSchema()
                .setName(TIMESTAMP_COL)
                .setMode(Field.Mode.NULLABLE.name())
                .setType("TIMESTAMP"),
            new TableFieldSchema()
                .setName(BOOLEAN_ARRAY_COL)
                .setMode(Field.Mode.REPEATED.name())
                .setType("BOOL"),
            new TableFieldSchema()
                .setName(BYTES_ARRAY_COL)
                .setMode(Field.Mode.REPEATED.name())
                .setType("BYTES"),
            new TableFieldSchema()
                .setName(DATE_ARRAY_COL)
                .setMode(Field.Mode.REPEATED.name())
                .setType("DATE"),
            new TableFieldSchema()
                .setName(FLOAT64_ARRAY_COL)
                .setMode(Field.Mode.REPEATED.name())
                .setType("FLOAT64"),
            new TableFieldSchema()
                .setName(INT64_ARRAY_COL)
                .setMode(Field.Mode.REPEATED.name())
                .setType("INT64"),
            new TableFieldSchema()
                .setName(JSON_ARRAY_COL)
                .setMode(Field.Mode.REPEATED.name())
                .setType("JSON"),
            new TableFieldSchema()
                .setName(NUMERIC_ARRAY_COL)
                .setMode(Field.Mode.REPEATED.name())
                .setType("NUMERIC"),
            new TableFieldSchema()
                .setName(STRING_ARRAY_COL)
                .setMode(Field.Mode.REPEATED.name())
                .setType("STRING"),
            new TableFieldSchema()
                .setName(TIMESTAMP_ARRAY_COL)
                .setMode(Field.Mode.REPEATED.name())
                .setType("TIMESTAMP"));

    assertThat(SpannerToBigQueryUtils.spannerColumnsToBigQueryIOFields(spannerColumnsOfAllTypes))
        .isEqualTo(tableFields);
  }

  @Test
  public void testAddSpannerNonPkColumnsToTableRow() throws Exception {
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
            "GenericData{classInfo=[f], {BytesCol=ZmZm, DateCol=2020-12-12, Float64Col=1.3,"
                + " Int64Col=5, JsonCol={\"color\":\"red\",\"value\":\"#f00\"}, NumericCol=4.4,"
                + " StringCol=abc, TimestampCol=2022-03-19T18:51:33.963910279Z,"
                + " BytesArrayCol=[YWJj, YmNk], DateArrayCol=[2021-01-22, 2022-01-01],"
                + " Float64ArrayCol=[1.2, 4.4], Int64ArrayCol=[1, 2], JsonArrayCol=[{},"
                + " {\"color\":\"red\",\"value\":\"#f00\"}, []], NumericArrayCol=[2.2, 3.3],"
                + " StringArrayCol=[a, b], TimestampArrayCol=[2022-03-19T18:51:33.963910279Z,"
                + " 2022-03-19T18:51:33.963910279Z]}}");
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
