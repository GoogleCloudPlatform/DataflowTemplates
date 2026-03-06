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

import com.google.api.services.datastream.v1.model.OracleTable;
import com.google.api.services.datastream.v1.model.SourceConfig;
import com.google.api.services.datastream.v1.model.SqlServerColumn;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.junit.Ignore;
import org.junit.Test;

/** Test cases for the {@link SchemaUtils} class. */
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

  @Test
  public void testConvertSqlServerToBigQueryColumnType_stringTypes()
      throws IOException, GeneralSecurityException {
    DataStreamClient datastream = new DataStreamClient(null);

    String[] stringTypes = {"CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "TEXT", "NTEXT", "XML", "UNIQUEIDENTIFIER", "SYSNAME"};
    for (String type : stringTypes) {
      SqlServerColumn column = new SqlServerColumn().setDataType(type);
      assertEquals(
          "Expected STRING for type " + type,
          StandardSQLTypeName.STRING,
          datastream.convertSqlServerToBigQueryColumnType(column));
    }
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_integerTypes()
      throws IOException, GeneralSecurityException {
    DataStreamClient datastream = new DataStreamClient(null);

    String[] intTypes = {"TINYINT", "SMALLINT", "INT", "BIGINT"};
    for (String type : intTypes) {
      SqlServerColumn column = new SqlServerColumn().setDataType(type);
      assertEquals(
          "Expected INT64 for type " + type,
          StandardSQLTypeName.INT64,
          datastream.convertSqlServerToBigQueryColumnType(column));
    }
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_floatTypes()
      throws IOException, GeneralSecurityException {
    DataStreamClient datastream = new DataStreamClient(null);

    String[] floatTypes = {"FLOAT", "REAL"};
    for (String type : floatTypes) {
      SqlServerColumn column = new SqlServerColumn().setDataType(type);
      assertEquals(
          "Expected FLOAT64 for type " + type,
          StandardSQLTypeName.FLOAT64,
          datastream.convertSqlServerToBigQueryColumnType(column));
    }
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_numericTypes()
      throws IOException, GeneralSecurityException {
    DataStreamClient datastream = new DataStreamClient(null);

    String[] numericTypes = {"DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY"};
    for (String type : numericTypes) {
      SqlServerColumn column = new SqlServerColumn().setDataType(type);
      assertEquals(
          "Expected BIGNUMERIC for type " + type,
          StandardSQLTypeName.BIGNUMERIC,
          datastream.convertSqlServerToBigQueryColumnType(column));
    }
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_bitType()
      throws IOException, GeneralSecurityException {
    DataStreamClient datastream = new DataStreamClient(null);

    SqlServerColumn column = new SqlServerColumn().setDataType("BIT");
    assertEquals(StandardSQLTypeName.BOOL, datastream.convertSqlServerToBigQueryColumnType(column));
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_binaryTypes()
      throws IOException, GeneralSecurityException {
    DataStreamClient datastream = new DataStreamClient(null);

    String[] binaryTypes = {"BINARY", "VARBINARY", "IMAGE"};
    for (String type : binaryTypes) {
      SqlServerColumn column = new SqlServerColumn().setDataType(type);
      assertEquals(
          "Expected BYTES for type " + type,
          StandardSQLTypeName.BYTES,
          datastream.convertSqlServerToBigQueryColumnType(column));
    }
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_dateTimeTypes()
      throws IOException, GeneralSecurityException {
    DataStreamClient datastream = new DataStreamClient(null);

    SqlServerColumn dateCol = new SqlServerColumn().setDataType("DATE");
    assertEquals(StandardSQLTypeName.DATE, datastream.convertSqlServerToBigQueryColumnType(dateCol));

    SqlServerColumn timeCol = new SqlServerColumn().setDataType("TIME");
    assertEquals(StandardSQLTypeName.TIME, datastream.convertSqlServerToBigQueryColumnType(timeCol));

    String[] datetimeTypes = {"DATETIME", "DATETIME2", "SMALLDATETIME"};
    for (String type : datetimeTypes) {
      SqlServerColumn column = new SqlServerColumn().setDataType(type);
      assertEquals(
          "Expected DATETIME for type " + type,
          StandardSQLTypeName.DATETIME,
          datastream.convertSqlServerToBigQueryColumnType(column));
    }

    SqlServerColumn dtoCol = new SqlServerColumn().setDataType("DATETIMEOFFSET");
    assertEquals(
        StandardSQLTypeName.TIMESTAMP,
        datastream.convertSqlServerToBigQueryColumnType(dtoCol));
  }

  @Test
  public void testConvertSqlServerToBigQueryColumnType_unknownType()
      throws IOException, GeneralSecurityException {
    DataStreamClient datastream = new DataStreamClient(null);

    SqlServerColumn column = new SqlServerColumn().setDataType("UNKNOWN_TYPE");
    assertEquals(
        StandardSQLTypeName.STRING,
        datastream.convertSqlServerToBigQueryColumnType(column));
  }
}
