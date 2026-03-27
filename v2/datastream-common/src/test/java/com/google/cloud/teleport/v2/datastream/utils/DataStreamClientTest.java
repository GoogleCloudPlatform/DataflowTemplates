/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.v2.datastream.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.google.api.services.datastream.v1.model.OracleTable;
import com.google.api.services.datastream.v1.model.SourceConfig;
import com.google.api.services.datastream.v1.model.SqlServerColumn;
import com.google.api.services.datastream.v1.model.SqlServerRdbms;
import com.google.api.services.datastream.v1.model.SqlServerSchema;
import com.google.api.services.datastream.v1.model.SqlServerTable;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import org.junit.Ignore;
import org.junit.Test;

/** Test cases for the {@link DataStreamClient} class. */
public class DataStreamClientTest {

  /** Test whether {@link DataStreamClient#getParentFromConnectionProfileName(String)}} regex. */
  @Test
  public void testDataStreamClientGetParent() throws IOException, GeneralSecurityException {
    String projectId = "my-project";
    String connProfileName = "projects/402074789819/locations/us-central1/connectionProfiles/cp-1";
    String expectedParent = "projects/402074789819/locations/us-central1";

    DataStreamClient datastream = new DataStreamClient(null);
    String parent = datastream.getParentFromConnectionProfileName(connProfileName);

    assertEquals(parent, expectedParent);
  }

  /**
   * Test whether {@link DataStreamClient#getSourceConnectionProfile(String)} gets a Streams source
   * connection profile name.
   */
  @Ignore
  @Test
  public void testDataStreamClientGetSourceConnectionProfileName()
      throws IOException, GeneralSecurityException {
    String projectId = "dataflow-bigstream-integration";
    String streamName =
        "projects/dataflow-bigstream-integration/locations/us-central1/streams/dfstream1";
    String connProfileName = "projects/402074789819/locations/us-central1/connectionProfiles/cp-1";

    DataStreamClient datastream = new DataStreamClient(null);

    SourceConfig sourceConnProfile = datastream.getSourceConnectionProfile(streamName);
    String sourceConnProfileName = sourceConnProfile.getSourceConnectionProfile();

    assertEquals(sourceConnProfileName, connProfileName);
  }

  /**
   * Test whether {@link DataStreamClient#discoverOracleTableSchema(String, String, String,
   * SourceConfig)} gets a Streams source connection profile name.
   */
  @Ignore
  @Test
  public void testDataStreamClientDiscoverOracleTableSchema()
      throws IOException, GeneralSecurityException {
    String projectId = "dataflow-bigstream-integration";
    String streamName =
        "projects/dataflow-bigstream-integration/locations/us-central1/streams/dfstream1";
    String schemaName = "HR";
    String tableName = "JOBS";

    DataStreamClient datastream = new DataStreamClient(null);
    SourceConfig sourceConnProfile = datastream.getSourceConnectionProfile(streamName);
    OracleTable table =
        datastream.discoverOracleTableSchema(streamName, schemaName, tableName, sourceConnProfile);

    String columnName = table.getOracleColumns().get(0).getColumn();
    Boolean isPrimaryKey = table.getOracleColumns().get(0).getPrimaryKey();

    assertEquals(columnName, "JOB_TITLE");
    assertEquals(isPrimaryKey, null);
  }

  // --- SQL Server: convertSqlServerToBigQueryColumnType tests ---

  @Test
  public void testConvertSqlServerToBigQueryColumnType_stringTypes() throws IOException {
    DataStreamClient datastream = new DataStreamClient(null);
    String[] stringTypes = {"CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "TEXT", "NTEXT",
        "UNIQUEIDENTIFIER", "XML"};

    for (String type : stringTypes) {
      SqlServerColumn column = new SqlServerColumn().setDataType(type);
      assertEquals(
          "Expected STRING for type: " + type,
          StandardSQLTypeName.STRING,
          datastream.convertSqlServerToBigQueryColumnType(column));
    }
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_stringTypesLowerCase() throws IOException {
    DataStreamClient datastream = new DataStreamClient(null);
    String[] stringTypes = {"char", "nchar", "varchar", "nvarchar", "text", "ntext",
        "uniqueidentifier", "xml"};

    for (String type : stringTypes) {
      SqlServerColumn column = new SqlServerColumn().setDataType(type);
      assertEquals(
          "Expected STRING for type: " + type,
          StandardSQLTypeName.STRING,
          datastream.convertSqlServerToBigQueryColumnType(column));
    }
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_boolType() throws IOException {
    DataStreamClient datastream = new DataStreamClient(null);
    SqlServerColumn column = new SqlServerColumn().setDataType("BIT");

    assertEquals(StandardSQLTypeName.BOOL,
        datastream.convertSqlServerToBigQueryColumnType(column));
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_int64Types() throws IOException {
    DataStreamClient datastream = new DataStreamClient(null);
    String[] intTypes = {"TINYINT", "SMALLINT", "INT", "BIGINT"};

    for (String type : intTypes) {
      SqlServerColumn column = new SqlServerColumn().setDataType(type);
      assertEquals(
          "Expected INT64 for type: " + type,
          StandardSQLTypeName.INT64,
          datastream.convertSqlServerToBigQueryColumnType(column));
    }
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_float64Types() throws IOException {
    DataStreamClient datastream = new DataStreamClient(null);
    String[] floatTypes = {"FLOAT", "REAL"};

    for (String type : floatTypes) {
      SqlServerColumn column = new SqlServerColumn().setDataType(type);
      assertEquals(
          "Expected FLOAT64 for type: " + type,
          StandardSQLTypeName.FLOAT64,
          datastream.convertSqlServerToBigQueryColumnType(column));
    }
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_bigNumericTypes() throws IOException {
    DataStreamClient datastream = new DataStreamClient(null);
    String[] numericTypes = {"DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY"};

    for (String type : numericTypes) {
      SqlServerColumn column = new SqlServerColumn().setDataType(type);
      assertEquals(
          "Expected BIGNUMERIC for type: " + type,
          StandardSQLTypeName.BIGNUMERIC,
          datastream.convertSqlServerToBigQueryColumnType(column));
    }
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_bytesTypes() throws IOException {
    DataStreamClient datastream = new DataStreamClient(null);
    String[] bytesTypes = {"BINARY", "VARBINARY", "IMAGE"};

    for (String type : bytesTypes) {
      SqlServerColumn column = new SqlServerColumn().setDataType(type);
      assertEquals(
          "Expected BYTES for type: " + type,
          StandardSQLTypeName.BYTES,
          datastream.convertSqlServerToBigQueryColumnType(column));
    }
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_dateType() throws IOException {
    DataStreamClient datastream = new DataStreamClient(null);
    SqlServerColumn column = new SqlServerColumn().setDataType("DATE");

    assertEquals(StandardSQLTypeName.DATE,
        datastream.convertSqlServerToBigQueryColumnType(column));
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_timeType() throws IOException {
    DataStreamClient datastream = new DataStreamClient(null);
    SqlServerColumn column = new SqlServerColumn().setDataType("TIME");

    assertEquals(StandardSQLTypeName.TIME,
        datastream.convertSqlServerToBigQueryColumnType(column));
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_timestampTypes() throws IOException {
    DataStreamClient datastream = new DataStreamClient(null);
    String[] timestampTypes = {"DATETIME", "DATETIME2", "SMALLDATETIME", "DATETIMEOFFSET"};

    for (String type : timestampTypes) {
      SqlServerColumn column = new SqlServerColumn().setDataType(type);
      assertEquals(
          "Expected TIMESTAMP for type: " + type,
          StandardSQLTypeName.TIMESTAMP,
          datastream.convertSqlServerToBigQueryColumnType(column));
    }
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_unknownTypeDefaultsToString()
      throws IOException {
    DataStreamClient datastream = new DataStreamClient(null);
    SqlServerColumn column = new SqlServerColumn().setDataType("UNKNOWN_TYPE");

    assertEquals(StandardSQLTypeName.STRING,
        datastream.convertSqlServerToBigQueryColumnType(column));
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_timestampPatternMatch() throws IOException {
    DataStreamClient datastream = new DataStreamClient(null);
    String[] timestampPatterns = {"TIMESTAMP", "TIMESTAMP(3)", "TIMESTAMP(6)"};

    for (String type : timestampPatterns) {
      SqlServerColumn column = new SqlServerColumn().setDataType(type);
      assertEquals(
          "Expected TIMESTAMP for pattern: " + type,
          StandardSQLTypeName.TIMESTAMP,
          datastream.convertSqlServerToBigQueryColumnType(column));
    }
  }

  // --- SQL Server: buildSqlServerRdbmsForTable tests (via reflection) ---

  @Test
  public void testBuildSqlServerRdbmsForTable()
      throws IOException, NoSuchMethodException, InvocationTargetException,
          IllegalAccessException {
    DataStreamClient datastream = new DataStreamClient(null);

    Method buildMethod =
        DataStreamClient.class.getDeclaredMethod(
            "buildSqlServerRdbmsForTable", String.class, String.class);
    buildMethod.setAccessible(true);

    String schemaName = "dbo";
    String tableName = "Customers";
    SqlServerRdbms rdbms =
        (SqlServerRdbms) buildMethod.invoke(datastream, schemaName, tableName);

    assertNotNull(rdbms);
    assertNotNull(rdbms.getSchemas());
    assertEquals(1, rdbms.getSchemas().size());

    SqlServerSchema schema = rdbms.getSchemas().get(0);
    assertEquals(schemaName, schema.getSchema());
    assertNotNull(schema.getTables());
    assertEquals(1, schema.getTables().size());

    SqlServerTable table = schema.getTables().get(0);
    assertEquals(tableName, table.getTable());
  }

  @Test
  public void testBuildSqlServerRdbmsForTable_specialCharacters()
      throws IOException, NoSuchMethodException, InvocationTargetException,
          IllegalAccessException {
    DataStreamClient datastream = new DataStreamClient(null);

    Method buildMethod =
        DataStreamClient.class.getDeclaredMethod(
            "buildSqlServerRdbmsForTable", String.class, String.class);
    buildMethod.setAccessible(true);

    String schemaName = "my-schema.v2";
    String tableName = "my_table-name.2";
    SqlServerRdbms rdbms =
        (SqlServerRdbms) buildMethod.invoke(datastream, schemaName, tableName);

    assertNotNull(rdbms);
    SqlServerSchema schema = rdbms.getSchemas().get(0);
    assertEquals(schemaName, schema.getSchema());

    SqlServerTable table = schema.getTables().get(0);
    assertEquals(tableName, table.getTable());
  }

  // --- SQL Server: getSqlServerPrimaryKeys tests (with mocked discoverSqlServerTableSchema) ---

  @Test
  public void testGetSqlServerPrimaryKeys_returnsPrimaryKeyColumns() throws IOException {
    DataStreamClient datastream = spy(new DataStreamClient(null));

    String streamName = "projects/my-project/locations/us-central1/streams/my-stream";
    String schemaName = "dbo";
    String tableName = "Orders";
    SourceConfig sourceConnProfile = new SourceConfig();

    SqlServerColumn pkColumn1 =
        new SqlServerColumn().setColumn("OrderId").setPrimaryKey(true).setDataType("INT");
    SqlServerColumn pkColumn2 =
        new SqlServerColumn().setColumn("LineId").setPrimaryKey(true).setDataType("INT");
    SqlServerColumn nonPkColumn =
        new SqlServerColumn().setColumn("Description").setPrimaryKey(false).setDataType("VARCHAR");
    SqlServerColumn nullPkColumn =
        new SqlServerColumn().setColumn("Amount").setDataType("DECIMAL");

    SqlServerTable mockTable =
        new SqlServerTable()
            .setTable(tableName)
            .setColumns(Arrays.asList(pkColumn1, pkColumn2, nonPkColumn, nullPkColumn));

    doReturn(mockTable)
        .when(datastream)
        .discoverSqlServerTableSchema(
            eq(streamName), eq(schemaName), eq(tableName), eq(sourceConnProfile));

    List<String> primaryKeys =
        datastream.getSqlServerPrimaryKeys(
            streamName, schemaName, tableName, sourceConnProfile);

    assertEquals(2, primaryKeys.size());
    assertEquals("OrderId", primaryKeys.get(0));
    assertEquals("LineId", primaryKeys.get(1));
  }

  @Test
  public void testGetSqlServerPrimaryKeys_noPrimaryKeys() throws IOException {
    DataStreamClient datastream = spy(new DataStreamClient(null));

    String streamName = "projects/my-project/locations/us-central1/streams/my-stream";
    String schemaName = "dbo";
    String tableName = "AuditLog";
    SourceConfig sourceConnProfile = new SourceConfig();

    SqlServerColumn col1 =
        new SqlServerColumn().setColumn("LogEntry").setPrimaryKey(false).setDataType("VARCHAR");
    SqlServerColumn col2 =
        new SqlServerColumn().setColumn("Timestamp").setPrimaryKey(false).setDataType("DATETIME");

    SqlServerTable mockTable =
        new SqlServerTable()
            .setTable(tableName)
            .setColumns(Arrays.asList(col1, col2));

    doReturn(mockTable)
        .when(datastream)
        .discoverSqlServerTableSchema(
            eq(streamName), eq(schemaName), eq(tableName), eq(sourceConnProfile));

    List<String> primaryKeys =
        datastream.getSqlServerPrimaryKeys(
            streamName, schemaName, tableName, sourceConnProfile);

    assertTrue(primaryKeys.isEmpty());
  }

  @Test
  public void testGetSqlServerPrimaryKeys_allPrimaryKeys() throws IOException {
    DataStreamClient datastream = spy(new DataStreamClient(null));

    String streamName = "projects/my-project/locations/us-central1/streams/my-stream";
    String schemaName = "dbo";
    String tableName = "CompositeKeyTable";
    SourceConfig sourceConnProfile = new SourceConfig();

    SqlServerColumn col1 =
        new SqlServerColumn().setColumn("Key1").setPrimaryKey(true).setDataType("INT");
    SqlServerColumn col2 =
        new SqlServerColumn().setColumn("Key2").setPrimaryKey(true).setDataType("INT");

    SqlServerTable mockTable =
        new SqlServerTable()
            .setTable(tableName)
            .setColumns(Arrays.asList(col1, col2));

    doReturn(mockTable)
        .when(datastream)
        .discoverSqlServerTableSchema(
            eq(streamName), eq(schemaName), eq(tableName), eq(sourceConnProfile));

    List<String> primaryKeys =
        datastream.getSqlServerPrimaryKeys(
            streamName, schemaName, tableName, sourceConnProfile);

    assertEquals(2, primaryKeys.size());
    assertEquals("Key1", primaryKeys.get(0));
    assertEquals("Key2", primaryKeys.get(1));
  }

  @Test
  public void testGetSqlServerPrimaryKeys_nullPrimaryKeyField() throws IOException {
    DataStreamClient datastream = spy(new DataStreamClient(null));

    String streamName = "projects/my-project/locations/us-central1/streams/my-stream";
    String schemaName = "dbo";
    String tableName = "NullPkTable";
    SourceConfig sourceConnProfile = new SourceConfig();

    SqlServerColumn colWithNullPk =
        new SqlServerColumn().setColumn("ColA").setDataType("INT");
    SqlServerColumn colWithFalsePk =
        new SqlServerColumn().setColumn("ColB").setPrimaryKey(false).setDataType("VARCHAR");
    SqlServerColumn colWithTruePk =
        new SqlServerColumn().setColumn("ColC").setPrimaryKey(true).setDataType("INT");

    SqlServerTable mockTable =
        new SqlServerTable()
            .setTable(tableName)
            .setColumns(Arrays.asList(colWithNullPk, colWithFalsePk, colWithTruePk));

    doReturn(mockTable)
        .when(datastream)
        .discoverSqlServerTableSchema(
            eq(streamName), eq(schemaName), eq(tableName), eq(sourceConnProfile));

    List<String> primaryKeys =
        datastream.getSqlServerPrimaryKeys(
            streamName, schemaName, tableName, sourceConnProfile);

    assertEquals(1, primaryKeys.size());
    assertEquals("ColC", primaryKeys.get(0));
  }
}
