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
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.PG_JSON_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.PG_NUMERIC_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_VAL;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.Field;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Options.ReadQueryUpdateTransactionOption;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerColumn;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerTable;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.SpannerChangeStreamsUtils;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.SpannerToBigQueryUtils;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link SchemaUtilsTest}. */
@RunWith(MockitoJUnitRunner.class)
public class SchemaUtilsTest {

  private static final String changeStreamName = "changeStreamName";
  private static final Options.RpcPriority rpcPriority = Options.RpcPriority.HIGH;
  private static ReadQueryUpdateTransactionOption priorityQueryOption =
      Options.priority(rpcPriority);
  @Mock private DatabaseClient mockDatabaseClient;
  @Mock private ReadContext mockReadContext;
  private List<TrackedSpannerColumn> spannerColumnsOfAllTypes;
  private Timestamp now = Timestamp.now();
  private MockedStatic<Options> mockedOptions;

  @Before
  public void setUp() {
    when(mockDatabaseClient.singleUse()).thenReturn(mockReadContext);
    when(mockDatabaseClient.singleUse(TimestampBound.ofReadTimestamp(now)))
        .thenReturn(mockReadContext);
    spannerColumnsOfAllTypes =
        ImmutableList.of(
            TrackedSpannerColumn.create(BOOLEAN_COL, Type.bool(), 1, 1),
            TrackedSpannerColumn.create(BYTES_COL, Type.bytes(), 2, 2),
            TrackedSpannerColumn.create(DATE_COL, Type.date(), 3, 3),
            TrackedSpannerColumn.create(FLOAT64_COL, Type.float64(), 4, 4),
            TrackedSpannerColumn.create(INT64_COL, Type.int64(), 5, 5),
            TrackedSpannerColumn.create(JSON_COL, Type.json(), 6, -1),
            TrackedSpannerColumn.create(NUMERIC_COL, Type.numeric(), 7, 6),
            TrackedSpannerColumn.create(STRING_COL, Type.string(), 8, 7),
            TrackedSpannerColumn.create(TIMESTAMP_COL, Type.timestamp(), 9, 8),
            TrackedSpannerColumn.create(BOOLEAN_ARRAY_COL, Type.array(Type.bool()), 10, -1),
            TrackedSpannerColumn.create(BYTES_ARRAY_COL, Type.array(Type.bytes()), 11, -1),
            TrackedSpannerColumn.create(DATE_ARRAY_COL, Type.array(Type.date()), 12, -1),
            TrackedSpannerColumn.create(FLOAT64_ARRAY_COL, Type.array(Type.float64()), 13, -1),
            TrackedSpannerColumn.create(INT64_ARRAY_COL, Type.array(Type.int64()), 14, -1),
            TrackedSpannerColumn.create(JSON_ARRAY_COL, Type.array(Type.json()), 15, -1),
            TrackedSpannerColumn.create(NUMERIC_ARRAY_COL, Type.array(Type.numeric()), 16, -1),
            TrackedSpannerColumn.create(STRING_ARRAY_COL, Type.array(Type.string()), 17, -1),
            TrackedSpannerColumn.create(TIMESTAMP_ARRAY_COL, Type.array(Type.timestamp()), 18, -1));
    mockedOptions = Mockito.mockStatic(Options.class);
    mockedOptions
        .when(() -> Options.priority(any(Options.RpcPriority.class)))
        .thenReturn(priorityQueryOption);
  }

  @After
  public void tearDown() {
    if (mockedOptions != null) {
      mockedOptions.close();
    }
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
            Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName).build(),
            Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("TABLE_NAME", Type.string()),
                    Type.StructField.of("COLUMN_NAME", Type.string())),
                Collections.emptyList()));

    Map<String, TrackedSpannerTable> actualSpannerTableByName =
        new SpannerChangeStreamsUtils(
                mockDatabaseClient, changeStreamName, Dialect.GOOGLE_STANDARD_SQL, rpcPriority, now)
            .getSpannerTableByName();

    List<TrackedSpannerColumn> singersPkColumns =
        ImmutableList.of(TrackedSpannerColumn.create("SingerId", Type.int64(), 1, 1));
    List<TrackedSpannerColumn> singersNonPkColumns =
        ImmutableList.of(
            TrackedSpannerColumn.create("FirstName", Type.string(), 2, -1),
            TrackedSpannerColumn.create("LastName", Type.string(), 3, -1));
    Map<String, TrackedSpannerTable> expectedSpannerTableByName = new HashMap<>();
    expectedSpannerTableByName.put(
        "Singers", new TrackedSpannerTable("Singers", singersPkColumns, singersNonPkColumns));
    assertThat(actualSpannerTableByName).isEqualTo(expectedSpannerTableByName);
  }

  @Test
  public void testChangeStreamTrackAllPostgres() {
    mockInformationSchemaChangeStreamsQueryPostgres(true);
    mockInformationSchemaTablesQueryPostgres();
    mockInformationSchemaColumnsQueryPostgres();
    mockInformationSchemaKeyColumnUsageQueryPostgres();
    String sql =
        "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
            + "WHERE CHANGE_STREAM_NAME = $1";
    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql).bind("p1").to(changeStreamName).build(),
            Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("table_name", Type.string()),
                    Type.StructField.of("column_name", Type.string())),
                Collections.emptyList()));

    Map<String, TrackedSpannerTable> actualSpannerTableByName =
        new SpannerChangeStreamsUtils(
                mockDatabaseClient, changeStreamName, Dialect.POSTGRESQL, rpcPriority, now)
            .getSpannerTableByName();

    List<TrackedSpannerColumn> singersPkColumns =
        ImmutableList.of(TrackedSpannerColumn.create("SingerId", Type.int64(), 1, 1));
    List<TrackedSpannerColumn> singersNonPkColumns =
        ImmutableList.of(
            TrackedSpannerColumn.create("FirstName", Type.string(), 2, -1),
            TrackedSpannerColumn.create("LastName", Type.string(), 3, -1));
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
            Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName).build(),
            Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("TABLE_NAME", Type.string()),
                    Type.StructField.of("COLUMN_NAME", Type.string())),
                Collections.emptyList()));

    Map<String, TrackedSpannerTable> actualSpannerTableByName =
        new SpannerChangeStreamsUtils(
                mockDatabaseClient, changeStreamName, Dialect.GOOGLE_STANDARD_SQL, rpcPriority)
            .getSpannerTableByName();

    List<TrackedSpannerColumn> singersPkColumns =
        ImmutableList.of(TrackedSpannerColumn.create("SingerId", Type.int64(), 1, 1));
    List<TrackedSpannerColumn> singersNonPkColumns =
        ImmutableList.of(
            TrackedSpannerColumn.create("FirstName", Type.string(), 2, -1),
            TrackedSpannerColumn.create("LastName", Type.string(), 3, -1));
    Map<String, TrackedSpannerTable> expectedSpannerTableByName = new HashMap<>();
    expectedSpannerTableByName.put(
        "Singers", new TrackedSpannerTable("Singers", singersPkColumns, singersNonPkColumns));
    assertThat(actualSpannerTableByName).isEqualTo(expectedSpannerTableByName);
  }

  @Test
  public void testChangeStreamTrackOneTablePostgres() {
    mockInformationSchemaChangeStreamsQueryPostgres(false);
    mockInformationSchemaChangeStreamTablesQueryPostgres();
    mockInformationSchemaColumnsQueryPostgres();
    mockInformationSchemaKeyColumnUsageQueryPostgres();
    String sql =
        "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
            + "WHERE CHANGE_STREAM_NAME = $1";
    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql).bind("p1").to(changeStreamName).build(),
            Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("TABLE_NAME", Type.string()),
                    Type.StructField.of("COLUMN_NAME", Type.string())),
                Collections.emptyList()));

    Map<String, TrackedSpannerTable> actualSpannerTableByName =
        new SpannerChangeStreamsUtils(
                mockDatabaseClient, changeStreamName, Dialect.POSTGRESQL, rpcPriority)
            .getSpannerTableByName();

    List<TrackedSpannerColumn> singersPkColumns =
        ImmutableList.of(TrackedSpannerColumn.create("SingerId", Type.int64(), 1, 1));
    List<TrackedSpannerColumn> singersNonPkColumns =
        ImmutableList.of(
            TrackedSpannerColumn.create("FirstName", Type.string(), 2, -1),
            TrackedSpannerColumn.create("LastName", Type.string(), 3, -1));
    Map<String, TrackedSpannerTable> expectedSpannerTableByName = new HashMap<>();
    expectedSpannerTableByName.put(
        "Singers", new TrackedSpannerTable("Singers", singersPkColumns, singersNonPkColumns));
    assertThat(actualSpannerTableByName).isEqualTo(expectedSpannerTableByName);
  }

  @Test
  public void testChangeStreamPrimaryKeyReverseOrder() {
    mockInformationSchemaChangeStreamsQuery(true);
    mockInformationSchemaTablesQuery();

    // Mock the query to INFORMATION_SCHEMA.COLUMNS.
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
                    .set("TABLE_NAME")
                    .to(Value.string("Singers"))
                    .set("COLUMN_NAME")
                    .to(Value.string("SingerId1"))
                    .set("ORDINAL_POSITION")
                    .to(Value.int64(1))
                    .set("SPANNER_TYPE")
                    .to(Value.string("INT64"))
                    .build(),
                Struct.newBuilder()
                    .set("TABLE_NAME")
                    .to(Value.string("Singers"))
                    .set("COLUMN_NAME")
                    .to(Value.string("SingerId2"))
                    .set("ORDINAL_POSITION")
                    .to(Value.int64(2))
                    .set("SPANNER_TYPE")
                    .to(Value.string("INT64"))
                    .build()));
    // spotless:on
    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql)
                .bind("tableNames")
                .to(Value.stringArray(new ArrayList<>(tableNames)))
                .build(),
            Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("TABLE_NAME", Type.string()),
                    Type.StructField.of("COLUMN_NAME", Type.string()),
                    Type.StructField.of("ORDINAL_POSITION", Type.int64()),
                    Type.StructField.of("SPANNER_TYPE", Type.string())),
                rows));

    // Mock the query to INFORMATION_SCHEMA.KEY_COLUMN_USAGE.
    sql =
        "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, CONSTRAINT_NAME FROM"
            + " INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME IN UNNEST (@tableNames)";
    // spotless:off
    rows =
        new ArrayList<>(
            ImmutableList.of(
                Struct.newBuilder()
                    .set("TABLE_NAME")
                    .to(Value.string("Singers"))
                    .set("COLUMN_NAME")
                    .to(Value.string("SingerId2"))
                    .set("ORDINAL_POSITION")
                    .to(Value.int64(1))
                    .set("CONSTRAINT_NAME")
                    .to(Value.string("PK_Singers"))
                    .build(),
                Struct.newBuilder()
                    .set("TABLE_NAME")
                    .to(Value.string("Singers"))
                    .set("COLUMN_NAME")
                    .to(Value.string("SingerId1"))
                    .set("ORDINAL_POSITION")
                    .to(Value.int64(2))
                    .set("CONSTRAINT_NAME")
                    .to(Value.string("PK_Singers"))
                    .build()));
    // spotless:on
    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql).bind("tableNames").to(Value.stringArray(tableNames)).build(),
            Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("TABLE_NAME", Type.string()),
                    Type.StructField.of("COLUMN_NAME", Type.string()),
                    Type.StructField.of("ORDINAL_POSITION", Type.int64()),
                    Type.StructField.of("CONSTRAINT_NAME", Type.string())),
                rows));

    // Mock the query to INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS.
    sql =
        "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
            + "WHERE CHANGE_STREAM_NAME = @changeStreamName";
    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName).build(),
            Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("TABLE_NAME", Type.string()),
                    Type.StructField.of("COLUMN_NAME", Type.string())),
                Collections.emptyList()));

    Map<String, TrackedSpannerTable> actualSpannerTableByName =
        new SpannerChangeStreamsUtils(
                mockDatabaseClient, changeStreamName, Dialect.GOOGLE_STANDARD_SQL, rpcPriority, now)
            .getSpannerTableByName();

    List<TrackedSpannerColumn> singersPkColumns =
        ImmutableList.of(
            TrackedSpannerColumn.create("SingerId2", Type.int64(), 2, 1),
            TrackedSpannerColumn.create("SingerId1", Type.int64(), 1, 2));
    List<TrackedSpannerColumn> singersNonPkColumns = Collections.emptyList();
    Map<String, TrackedSpannerTable> expectedSpannerTableByName = new HashMap<>();
    expectedSpannerTableByName.put(
        "Singers", new TrackedSpannerTable("Singers", singersPkColumns, singersNonPkColumns));
    assertThat(actualSpannerTableByName).isEqualTo(expectedSpannerTableByName);
  }

  @Test
  public void testChangeStreamTrackOneColumn() {
    mockInformationSchemaChangeStreamsQuery(false);
    mockInformationSchemaChangeStreamTablesQuery();
    mockInformationSchemaColumnsQuery();
    mockInformationSchemaKeyColumnUsageQuery();
    String sql =
        "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
            + "WHERE CHANGE_STREAM_NAME = @changeStreamName";
    // spotless:off
    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName).build(), Options.priority(rpcPriority)))
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
        new SpannerChangeStreamsUtils(
                mockDatabaseClient, changeStreamName, Dialect.GOOGLE_STANDARD_SQL, rpcPriority)
            .getSpannerTableByName();

    List<TrackedSpannerColumn> singersPkColumns =
        Collections.singletonList(TrackedSpannerColumn.create("SingerId", Type.int64(), 1, 1));
    List<TrackedSpannerColumn> singersNonPkColumns =
        Collections.singletonList(TrackedSpannerColumn.create("FirstName", Type.string(), 2, -1));
    Map<String, TrackedSpannerTable> expectedSpannerTableByName = new HashMap<>();
    expectedSpannerTableByName.put(
        "Singers", new TrackedSpannerTable("Singers", singersPkColumns, singersNonPkColumns));
    assertThat(actualSpannerTableByName).isEqualTo(expectedSpannerTableByName);
  }

  @Test
  public void testChangeStreamTrackOneColumnPostgres() {
    mockInformationSchemaChangeStreamsQueryPostgres(false);
    mockInformationSchemaChangeStreamTablesQueryPostgres();
    mockInformationSchemaColumnsQueryPostgres();
    mockInformationSchemaKeyColumnUsageQueryPostgres();
    String sql =
        "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS "
            + "WHERE CHANGE_STREAM_NAME = $1";
    // spotless:off
    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql).bind("p1").to(changeStreamName).build(), Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("table_name", Type.string()),
                    Type.StructField.of("column_name", Type.string())),
                ImmutableList.of(
                    Struct.newBuilder()
                        .set("table_name")
                        .to(Value.string("Singers"))
                        .set("column_name")
                        .to(Value.string("SingerId"))
                        .build(),
                    Struct.newBuilder()
                        .set("table_name")
                        .to(Value.string("Singers"))
                        .set("column_name")
                        .to(Value.string("FirstName"))
                        .build())));
    // spotless:on

    Map<String, TrackedSpannerTable> actualSpannerTableByName =
        new SpannerChangeStreamsUtils(
                mockDatabaseClient, changeStreamName, Dialect.POSTGRESQL, rpcPriority)
            .getSpannerTableByName();

    List<TrackedSpannerColumn> singersPkColumns =
        Collections.singletonList(TrackedSpannerColumn.create("SingerId", Type.int64(), 1, 1));
    List<TrackedSpannerColumn> singersNonPkColumns =
        Collections.singletonList(TrackedSpannerColumn.create("FirstName", Type.string(), 2, -1));
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
                    .set(BOOLEAN_COL)
                    .to(BOOLEAN_VAL)
                    .set(BYTES_COL)
                    .to(BYTES_VAL)
                    .set(DATE_COL)
                    .to(DATE_VAL)
                    .set(FLOAT64_COL)
                    .to(FLOAT64_VAL)
                    .set(INT64_COL)
                    .to(INT64_VAL)
                    .set(JSON_COL)
                    .to(JSON_VAL)
                    .set(NUMERIC_COL)
                    .to(NUMERIC_VAL)
                    .set(STRING_COL)
                    .to(STRING_VAL)
                    .set(TIMESTAMP_COL)
                    .to(TIMESTAMP_VAL)
                    .set(BOOLEAN_ARRAY_COL)
                    .to(BOOLEAN_NULLABLE_ARRAY_VAL)
                    .set(BYTES_ARRAY_COL)
                    .to(BYTES_NULLABLE_ARRAY_VAL)
                    .set(DATE_ARRAY_COL)
                    .to(DATE_NULLABLE_ARRAY_VAL)
                    .set(FLOAT64_ARRAY_COL)
                    .to(FLOAT64_NULLABLE_ARRAY_VAL)
                    .set(INT64_ARRAY_COL)
                    .to(INT64_NULLABLE_ARRAY_VAL)
                    .set(JSON_ARRAY_COL)
                    .to(JSON_NULLABLE_ARRAY_VAL)
                    .set(NUMERIC_ARRAY_COL)
                    .to(NUMERIC_NULLABLE_ARRAY_VAL)
                    .set(STRING_ARRAY_COL)
                    .to(STRING_NULLABLE_ARRAY_VAL)
                    .set(TIMESTAMP_ARRAY_COL)
                    .to(TIMESTAMP_NULLABLE_ARRAY_VAL)
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
      SpannerChangeStreamsUtils.appendToSpannerKey(spannerColumn, keysJsonObject, keyBuilder);
    }

    assertThat(keyBuilder.build().toString())
        .isEqualTo("[true,NDU2,2022-03-11,2.5,10,10,abc,2022-03-07T01:50:53.972000000Z]");
  }

  @Test
  public void testTableRowColumnsToBigQueryIOFields() {
    TableRow tableRow = new TableRow();
    tableRow.put(BOOLEAN_COL, true);
    tableRow.put("_type_" + BOOLEAN_COL, "BOOL");
    tableRow.put(BYTES_COL, "");
    tableRow.put("_type_" + BYTES_COL, "BYTES");
    tableRow.put(DATE_COL, "");
    tableRow.put("_type_" + DATE_COL, "DATE");
    tableRow.put(FLOAT64_COL, "");
    tableRow.put("_type_" + FLOAT64_COL, "FLOAT64");
    tableRow.put(INT64_COL, "");
    tableRow.put("_type_" + INT64_COL, "INT64");
    tableRow.put(JSON_COL, "");
    tableRow.put("_type_" + JSON_COL, "JSON");
    tableRow.put(PG_JSON_COL, "");
    tableRow.put("_type_" + PG_JSON_COL, "PG_JSONB");
    tableRow.put(NUMERIC_COL, "");
    tableRow.put("_type_" + NUMERIC_COL, "NUMERIC");
    tableRow.put(PG_NUMERIC_COL, "");
    tableRow.put("_type_" + PG_NUMERIC_COL, "PG_NUMERIC");
    tableRow.put(STRING_COL, "");
    tableRow.put("_type_" + STRING_COL, "STRING");
    tableRow.put(TIMESTAMP_COL, "");
    tableRow.put("_type_" + TIMESTAMP_COL, "TIMESTAMP");
    tableRow.put(BOOLEAN_ARRAY_COL, "");
    tableRow.put("_type_" + BOOLEAN_ARRAY_COL, "ARRAY<BOOL>");
    tableRow.put(BYTES_ARRAY_COL, "");
    tableRow.put("_type_" + BYTES_ARRAY_COL, "ARRAY<BYTES>");
    tableRow.put(DATE_ARRAY_COL, "");
    tableRow.put("_type_" + DATE_ARRAY_COL, "ARRAY<DATE>");
    tableRow.put(FLOAT64_ARRAY_COL, "");
    tableRow.put("_type_" + FLOAT64_ARRAY_COL, "ARRAY<FLOAT64>");
    tableRow.put(INT64_ARRAY_COL, "");
    tableRow.put("_type_" + INT64_ARRAY_COL, "ARRAY<INT64>");
    tableRow.put(JSON_ARRAY_COL, "");
    tableRow.put("_type_" + JSON_ARRAY_COL, "ARRAY<JSON>");
    tableRow.put(NUMERIC_ARRAY_COL, "");
    tableRow.put("_type_" + NUMERIC_ARRAY_COL, "ARRAY<NUMERIC>");
    tableRow.put(STRING_ARRAY_COL, "");
    tableRow.put("_type_" + STRING_ARRAY_COL, "ARRAY<STRING>");
    tableRow.put(TIMESTAMP_ARRAY_COL, "");
    tableRow.put("_type_" + TIMESTAMP_ARRAY_COL, "ARRAY<TIMESTAMP>");

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
                .setName(PG_JSON_COL)
                .setMode(Field.Mode.NULLABLE.name())
                .setType("JSON"),
            new TableFieldSchema()
                .setName(NUMERIC_COL)
                .setMode(Field.Mode.NULLABLE.name())
                .setType("NUMERIC"),
            new TableFieldSchema()
                .setName(PG_NUMERIC_COL)
                .setMode(Field.Mode.NULLABLE.name())
                .setType("STRING"),
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
    assertThat(SpannerToBigQueryUtils.tableRowColumnsToBigQueryIOFields(tableRow, false))
        .isEqualTo(tableFields);
  }

  @Test
  public void testAddSpannerNonPkColumnsToTableRow() throws Exception {
    String newValuesJson =
        "{\"BooleanCol\":true,\"BytesCol\":\"ZmZm\",\"DateCol\":\"2020-12-12\",\"Float64Col\":1.3,"
            + "\"Int64Col\":\"5\","
            + "\"JsonCol\":\"{\\\"color\\\":\\\"red\\\",\\\"value\\\":\\\"#f00\\\"}\","
            + "\"NumericCol\":\"4.4\",\"StringCol\":\"abc\","
            + "\"TimestampCol\":\"2022-03-19T18:51:33.963910279Z\",\"BooleanArrayCol\":[true,false],"
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
        newValuesJson, spannerColumnsOfAllTypes, tableRow, ModType.INSERT);

    assertThat(tableRow.toString())
        .isEqualTo(
            "GenericData{classInfo=[f], {BooleanCol=true, _type_BooleanCol=BOOL, BytesCol=ZmZm, _type_BytesCol=BYTES, DateCol=2020-12-12,"
                + " _type_DateCol=DATE, Float64Col=1.3, _type_Float64Col=FLOAT64, Int64Col=5,"
                + " _type_Int64Col=INT64, JsonCol={\"color\":\"red\",\"value\":\"#f00\"},"
                + " _type_JsonCol=JSON, NumericCol=4.4, _type_NumericCol=NUMERIC, StringCol=abc,"
                + " _type_StringCol=STRING, TimestampCol=2022-03-19T18:51:33.963910279Z,"
                + " _type_TimestampCol=TIMESTAMP, BooleanArrayCol=[true, false], _type_BooleanArrayCol=ARRAY<BOOL>, BytesArrayCol=[YWJj, YmNk],"
                + " _type_BytesArrayCol=ARRAY<BYTES>, DateArrayCol=[2021-01-22,"
                + " 2022-01-01], _type_DateArrayCol=ARRAY<DATE>, Float64ArrayCol=[1.2, 4.4],"
                + " _type_Float64ArrayCol=ARRAY<FLOAT64>, Int64ArrayCol=[1, 2],"
                + " _type_Int64ArrayCol=ARRAY<INT64>,"
                + " JsonArrayCol=[{}, {\"color\":\"red\",\"value\":\"#f00\"}, []],"
                + " _type_JsonArrayCol=ARRAY<JSON>,"
                + " NumericArrayCol=[2.2, 3.3], _type_NumericArrayCol=ARRAY<NUMERIC>,"
                + " StringArrayCol=[a, b], _type_StringArrayCol=ARRAY<STRING>,"
                + " TimestampArrayCol=[2022-03-19T18:51:33.963910279Z,"
                + " 2022-03-19T18:51:33.963910279Z], _type_TimestampArrayCol=ARRAY<TIMESTAMP>}}");
  }

  @Test
  public void testAddSpannerNonPkColumnsToTableRowForDelete() throws Exception {
    String newValuesJson = "";
    TableRow tableRow = new TableRow();
    SpannerToBigQueryUtils.addSpannerNonPkColumnsToTableRow(
        newValuesJson, spannerColumnsOfAllTypes, tableRow, ModType.DELETE);

    assertThat(tableRow.toString())
        .isEqualTo(
            "GenericData{classInfo=[f], {BooleanCol=null, _type_BooleanCol=BOOL, BytesCol=null, _type_BytesCol=BYTES, DateCol=null,"
                + " _type_DateCol=DATE, Float64Col=null, _type_Float64Col=FLOAT64, Int64Col=null,"
                + " _type_Int64Col=INT64, JsonCol=null,"
                + " _type_JsonCol=JSON, NumericCol=null, _type_NumericCol=NUMERIC, StringCol=null,"
                + " _type_StringCol=STRING, TimestampCol=null,"
                + " _type_TimestampCol=TIMESTAMP, BooleanArrayCol=null, _type_BooleanArrayCol=ARRAY<BOOL>, BytesArrayCol=null,"
                + " _type_BytesArrayCol=ARRAY<BYTES>, DateArrayCol=null, _type_DateArrayCol=ARRAY<DATE>, Float64ArrayCol=null,"
                + " _type_Float64ArrayCol=ARRAY<FLOAT64>, Int64ArrayCol=null,"
                + " _type_Int64ArrayCol=ARRAY<INT64>,"
                + " JsonArrayCol=null, _type_JsonArrayCol=ARRAY<JSON>,"
                + " NumericArrayCol=null, _type_NumericArrayCol=ARRAY<NUMERIC>,"
                + " StringArrayCol=null, _type_StringArrayCol=ARRAY<STRING>,"
                + " TimestampArrayCol=null, _type_TimestampArrayCol=ARRAY<TIMESTAMP>}}");
  }

  @Test
  public void testAddSpannerNonPkColumnsToTableRowForNewRowOldValuesUpdate() throws Exception {
    String newValuesJson = "{\"BooleanCol\":true,\"BytesCol\":\"ZmZm\"}";
    TableRow tableRow = new TableRow();
    SpannerToBigQueryUtils.addSpannerNonPkColumnsToTableRow(
        newValuesJson, spannerColumnsOfAllTypes, tableRow, ModType.UPDATE);

    assertThat(tableRow.toString())
        .isEqualTo(
            "GenericData{classInfo=[f], {BooleanCol=true, _type_BooleanCol=BOOL, BytesCol=ZmZm, _type_BytesCol=BYTES, DateCol=null,"
                + " _type_DateCol=DATE, Float64Col=null, _type_Float64Col=FLOAT64, Int64Col=null,"
                + " _type_Int64Col=INT64, JsonCol=null,"
                + " _type_JsonCol=JSON, NumericCol=null, _type_NumericCol=NUMERIC, StringCol=null,"
                + " _type_StringCol=STRING, TimestampCol=null,"
                + " _type_TimestampCol=TIMESTAMP, BooleanArrayCol=null, _type_BooleanArrayCol=ARRAY<BOOL>, BytesArrayCol=null,"
                + " _type_BytesArrayCol=ARRAY<BYTES>, DateArrayCol=null, _type_DateArrayCol=ARRAY<DATE>, Float64ArrayCol=null,"
                + " _type_Float64ArrayCol=ARRAY<FLOAT64>, Int64ArrayCol=null,"
                + " _type_Int64ArrayCol=ARRAY<INT64>,"
                + " JsonArrayCol=null, _type_JsonArrayCol=ARRAY<JSON>,"
                + " NumericArrayCol=null, _type_NumericArrayCol=ARRAY<NUMERIC>,"
                + " StringArrayCol=null, _type_StringArrayCol=ARRAY<STRING>,"
                + " TimestampArrayCol=null, _type_TimestampArrayCol=ARRAY<TIMESTAMP>}}");
  }

  private void mockInformationSchemaChangeStreamsQuery(boolean isTrackingAll) {
    String sql =
        "SELECT CHANGE_STREAMS.ALL FROM INFORMATION_SCHEMA.CHANGE_STREAMS "
            + "WHERE CHANGE_STREAM_NAME = @changeStreamName";

    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName).build(),
            Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(Type.StructField.of("ALL", Type.bool())),
                Collections.singletonList(
                    Struct.newBuilder().set("ALL").to(Value.bool(isTrackingAll)).build())));
  }

  private void mockInformationSchemaChangeStreamsQueryPostgres(boolean isTrackingAll) {
    String sql =
        "SELECT CHANGE_STREAMS.all FROM INFORMATION_SCHEMA.CHANGE_STREAMS "
            + "WHERE CHANGE_STREAM_NAME = $1";

    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql).bind("p1").to(changeStreamName).build(),
            Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(Type.StructField.of("all", Type.string())),
                Collections.singletonList(
                    Struct.newBuilder()
                        .set("all")
                        .to(Value.string(isTrackingAll ? "YES" : "NO"))
                        .build())));
  }

  private void mockInformationSchemaTablesQuery() {
    String sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \"\"";

    when(mockReadContext.executeQuery(Statement.of(sql), Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(Type.StructField.of("TABLE_NAME", Type.string())),
                ImmutableList.of(
                    Struct.newBuilder().set("TABLE_NAME").to(Value.string("Singers")).build())));
  }

  private void mockInformationSchemaTablesQueryPostgres() {
    String sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'public'";

    when(mockReadContext.executeQuery(Statement.of(sql), Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(Type.StructField.of("table_name", Type.string())),
                ImmutableList.of(
                    Struct.newBuilder().set("table_name").to(Value.string("Singers")).build())));
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
                    .set("TABLE_NAME")
                    .to(Value.string("Singers"))
                    .set("COLUMN_NAME")
                    .to(Value.string("SingerId"))
                    .set("ORDINAL_POSITION")
                    .to(Value.int64(1))
                    .set("SPANNER_TYPE")
                    .to(Value.string("INT64"))
                    .build(),
                Struct.newBuilder()
                    .set("TABLE_NAME")
                    .to(Value.string("Singers"))
                    .set("COLUMN_NAME")
                    .to(Value.string("FirstName"))
                    .set("ORDINAL_POSITION")
                    .to(Value.int64(2))
                    .set("SPANNER_TYPE")
                    .to(Value.string("STRING(1024)"))
                    .build(),
                Struct.newBuilder()
                    .set("TABLE_NAME")
                    .to(Value.string("Singers"))
                    .set("COLUMN_NAME")
                    .to(Value.string("LastName"))
                    .set("ORDINAL_POSITION")
                    .to(Value.int64(3))
                    .set("SPANNER_TYPE")
                    .to(Value.string("STRING"))
                    .build()));
    // spotless:on

    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql)
                .bind("tableNames")
                .to(Value.stringArray(new ArrayList<>(tableNames)))
                .build(),
            Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("TABLE_NAME", Type.string()),
                    Type.StructField.of("COLUMN_NAME", Type.string()),
                    Type.StructField.of("ORDINAL_POSITION", Type.int64()),
                    Type.StructField.of("SPANNER_TYPE", Type.string())),
                rows));
  }

  private void mockInformationSchemaColumnsQueryPostgres() {
    StringBuilder sqlStringBuilder =
        new StringBuilder(
            "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, SPANNER_TYPE "
                + "FROM INFORMATION_SCHEMA.COLUMNS");
    sqlStringBuilder.append(" WHERE TABLE_NAME = ANY (Array[");
    sqlStringBuilder.append("'Singers'");
    sqlStringBuilder.append("])");
    // spotless:off
    List<Struct> rows =
        new ArrayList<>(
            ImmutableList.of(
                Struct.newBuilder()
                    .set("table_name")
                    .to(Value.string("Singers"))
                    .set("column_name")
                    .to(Value.string("SingerId"))
                    .set("ordinal_position")
                    .to(Value.int64(1))
                    .set("spanner_type")
                    .to(Value.string("bigint"))
                    .build(),
                Struct.newBuilder()
                    .set("table_name")
                    .to(Value.string("Singers"))
                    .set("column_name")
                    .to(Value.string("FirstName"))
                    .set("ordinal_position")
                    .to(Value.int64(2))
                    .set("spanner_type")
                    .to(Value.string("character varying(1024)"))
                    .build(),
                Struct.newBuilder()
                    .set("table_name")
                    .to(Value.string("Singers"))
                    .set("column_name")
                    .to(Value.string("LastName"))
                    .set("ordinal_position")
                    .to(Value.int64(3))
                    .set("spanner_type")
                    .to(Value.string("character varying"))
                    .build()));
    // spotless:on

    when(mockReadContext.executeQuery(
            Statement.newBuilder(sqlStringBuilder.toString()).build(),
            Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("table_name", Type.string()),
                    Type.StructField.of("column_name", Type.string()),
                    Type.StructField.of("ordinal_position", Type.int64()),
                    Type.StructField.of("spanner_type", Type.string())),
                rows));
  }

  private void mockInformationSchemaChangeStreamTablesQuery() {
    String sql =
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES "
            + "WHERE CHANGE_STREAM_NAME = @changeStreamName";

    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql).bind("changeStreamName").to(changeStreamName).build(),
            Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(Type.StructField.of("TABLE_NAME", Type.string())),
                Collections.singletonList(
                    Struct.newBuilder().set("TABLE_NAME").to(Value.string("Singers")).build())));
  }

  private void mockInformationSchemaChangeStreamTablesQueryPostgres() {
    String sql =
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES "
            + "WHERE CHANGE_STREAM_NAME = $1";

    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql).bind("p1").to(changeStreamName).build(),
            Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(Type.StructField.of("table_name", Type.string())),
                Collections.singletonList(
                    Struct.newBuilder().set("table_name").to(Value.string("Singers")).build())));
  }

  private void mockInformationSchemaKeyColumnUsageQuery() {
    String sql =
        "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, CONSTRAINT_NAME FROM"
            + " INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME IN UNNEST (@tableNames)";
    List<String> tableNames = new ArrayList<>();
    tableNames.add("Singers");
    // spotless:off
    List<Struct> rows =
        new ArrayList<>(
            Collections.singletonList(
                Struct.newBuilder()
                    .set("TABLE_NAME")
                    .to(Value.string("Singers"))
                    .set("COLUMN_NAME")
                    .to(Value.string("SingerId"))
                    .set("ORDINAL_POSITION")
                    .to(Value.int64(1))
                    .set("CONSTRAINT_NAME")
                    .to(Value.string("PK_Singers"))
                    .build()));
    // spotless:on

    when(mockReadContext.executeQuery(
            Statement.newBuilder(sql).bind("tableNames").to(Value.stringArray(tableNames)).build(),
            Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("TABLE_NAME", Type.string()),
                    Type.StructField.of("COLUMN_NAME", Type.string()),
                    Type.StructField.of("ORDINAL_POSITION", Type.int64()),
                    Type.StructField.of("CONSTRAINT_NAME", Type.string())),
                rows));
  }

  private void mockInformationSchemaKeyColumnUsageQueryPostgres() {
    StringBuilder sqlStringBuilder =
        new StringBuilder(
            "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, CONSTRAINT_NAME FROM"
                + " INFORMATION_SCHEMA.KEY_COLUMN_USAGE");
    sqlStringBuilder.append(" WHERE TABLE_NAME = ANY (Array[");
    sqlStringBuilder.append("'Singers'");
    sqlStringBuilder.append("])");
    // spotless:off
    List<Struct> rows =
        new ArrayList<>(
            Collections.singletonList(
                Struct.newBuilder()
                    .set("table_name")
                    .to(Value.string("Singers"))
                    .set("column_name")
                    .to(Value.string("SingerId"))
                    .set("ordinal_position")
                    .to(Value.int64(1))
                    .set("constraint_name")
                    .to(Value.string("PK_singers"))
                    .build()));
    // spotless:on

    when(mockReadContext.executeQuery(
            Statement.newBuilder(sqlStringBuilder.toString()).build(),
            Options.priority(rpcPriority)))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("table_name", Type.string()),
                    Type.StructField.of("column_name", Type.string()),
                    Type.StructField.of("ordinal_position", Type.int64()),
                    Type.StructField.of("constraint_name", Type.string())),
                rows));
  }

  @Test
  public void testCleanSpannerType() {
    // STRING -> STRING
    assertThat(SpannerToBigQueryUtils.cleanSpannerType("STRING")).isEqualTo("STRING");
    // NUMERIC<PG_NUMERIC> -> NUMERIC
    assertThat(SpannerToBigQueryUtils.cleanSpannerType("NUMERIC<PG_NUMERIC>")).isEqualTo("NUMERIC");
    // ARRAY<NUMERIC<PG_NUMERIC>> -> ARRAY<NUMERIC>
    assertThat(SpannerToBigQueryUtils.cleanSpannerType("ARRAY<NUMERIC<PG_NUMERIC>>"))
        .isEqualTo("ARRAY<NUMERIC>");
  }
}
