/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.datastream.values.DatastreamRow;
import com.google.cloud.teleport.v2.datastream.values.DmlInfo;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.Test;

/** Test cases for DDL generation in {@link DatastreamToDML} subclasses. */
public class DatastreamToDDLTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testPostgresCreateTableSql() {
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);
    String schemaName = "public";
    String tableName = "users";
    List<String> primaryKeys = Arrays.asList("id");
    Map<String, String> sourceSchema = new HashMap<>();
    sourceSchema.put("id", "BIGINT");
    sourceSchema.put("name", "TEXT");

    String actual = dml.getCreateTableSql(null, schemaName, tableName, primaryKeys, sourceSchema);

    assertTrue(actual.contains("CREATE TABLE IF NOT EXISTS \"public\".\"users\""));
    assertTrue(actual.contains("\"id\" BIGINT"));
    assertTrue(actual.contains("\"name\" TEXT"));
    assertTrue(actual.contains("PRIMARY KEY (\"id\")"));
  }

  @Test
  public void testPostgresAddColumnSql() {
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);
    String expected = "ALTER TABLE \"public\".\"users\" ADD COLUMN \"age\" INTEGER;";
    String actual = dml.getAddColumnSql(null, "public", "users", "age", "INTEGER");
    assertEquals(expected, actual);
  }

  @Test
  public void testMySqlCreateTableSql() {
    DatastreamToMySQLDML dml = DatastreamToMySQLDML.of(null);
    String catalogName = "mydb";
    String tableName = "users";
    List<String> primaryKeys = Arrays.asList("id");
    Map<String, String> sourceSchema = new HashMap<>();
    sourceSchema.put("id", "BIGINT");
    sourceSchema.put("name", "TEXT");

    String actual = dml.getCreateTableSql(catalogName, null, tableName, primaryKeys, sourceSchema);

    assertTrue(actual.contains("CREATE TABLE IF NOT EXISTS `mydb`.`users`"));
    assertTrue(actual.contains("`id` BIGINT"));
    assertTrue(actual.contains("`name` TEXT"));
    assertTrue(actual.contains("PRIMARY KEY (`id`)"));
  }

  @Test
  public void testMySqlAddColumnSql() {
    DatastreamToMySQLDML dml = DatastreamToMySQLDML.of(null);
    String expected = "ALTER TABLE `mydb`.`users` ADD COLUMN `age` INTEGER;";
    String actual = dml.getAddColumnSql("mydb", null, "users", "age", "INTEGER");
    assertEquals(expected, actual);
  }

  @Test
  public void testPostgresDestinationType() {
    DatastreamToPostgresDML dml = DatastreamToPostgresDML.of(null);
    assertEquals("BIGINT", dml.getDestinationType("INT64", null, null));
    assertEquals("BOOLEAN", dml.getDestinationType("BOOL", null, null));
    assertEquals("TIMESTAMP WITH TIME ZONE", dml.getDestinationType("TIMESTAMP", null, null));
    assertEquals("TEXT", dml.getDestinationType("STRING", null, null));
  }

  @Test
  public void testMySqlDestinationType() {
    DatastreamToMySQLDML dml = DatastreamToMySQLDML.of(null);
    assertEquals("BIGINT", dml.getDestinationType("INT64", null, null));
    assertEquals("BOOLEAN", dml.getDestinationType("BOOL", null, null));
    assertEquals("TIMESTAMP", dml.getDestinationType("TIMESTAMP", null, null));
    assertEquals("TEXT", dml.getDestinationType("STRING", null, null));
  }

  /**
   * Tests that {@link DatastreamRow#formatStringTemplate} correctly handles JsonNode sources. This
   * verifies that {_metadata_stream} is correctly replaced.
   */
  @Test
  public void testFormatStringTemplate_withJsonNode() throws IOException {
    String json =
        "{\"_metadata_stream\": \"projects/p1/locations/l1/streams/s1\", \"data\": \"val\"}";
    JsonNode node = MAPPER.readTree(json);
    DatastreamRow row = DatastreamRow.of(node);

    String template = "{_metadata_stream}";
    String formatted = row.formatStringTemplate(template);

    assertThat(formatted).isEqualTo("projects/p1/locations/l1/streams/s1");
  }

  /**
   * Tests that {@link DatastreamToDML#convertJsonToDmlInfo} uses destination primary keys for state
   * key generation when source primary keys are missing (common in Postgres).
   */
  @Test
  public void testConvertJsonToDmlInfo_usesDestinationPkForStateKey() throws IOException {
    // Arrange
    String json =
        "{"
            + "\"id\": 123,"
            + "\"_metadata_schema\": \"public\","
            + "\"_metadata_table\": \"users\","
            + "\"_metadata_deleted\": false"
            + "}";
    JsonNode node = MAPPER.readTree(json);

    // Create a concrete implementation for testing
    DatastreamToPostgresDML spyDml =
        new DatastreamToPostgresDML(null) {
          @Override
          public Map<String, String> getTableSchema(String catalog, String schema, String table) {
            Map<String, String> schemaMap = new HashMap<>();
            schemaMap.put("id", "INTEGER");
            return schemaMap;
          }

          @Override
          public java.util.List<String> getPrimaryKeys(
              String catalog, String schema, String table, JsonNode row) {
            return Arrays.asList("id");
          }
        };

    // Act
    DmlInfo dmlInfo = spyDml.convertJsonToDmlInfo(node, json);

    // Assert
    // The state key should include the PK value '123'
    // Format: schema.table:pk1-pk2...
    assertThat(dmlInfo.getStateWindowKey()).isEqualTo("public.users:123");
  }

  /** Tests the lowercased retry fallback in {@link DatastreamToDML.JdbcTableCache}. */
  @Test
  public void testJdbcTableCache_postgreSqlLowercaseFallback() throws SQLException {
    // Arrange
    DataSource mockDs = mock(DataSource.class);
    Connection mockConn = mock(Connection.class);
    DatabaseMetaData mockMeta = mock(DatabaseMetaData.class);
    ResultSet mockRsEmpty = mock(ResultSet.class);
    ResultSet mockRsFound = mock(ResultSet.class);

    when(mockDs.getConnection()).thenReturn(mockConn);
    when(mockConn.getMetaData()).thenReturn(mockMeta);
    when(mockMeta.getDatabaseProductName()).thenReturn("PostgreSQL");

    // First call (exact case) returns nothing
    when(mockMeta.getColumns(null, "MY_SCHEMA", "MY_TABLE", null)).thenReturn(mockRsEmpty);
    when(mockRsEmpty.next()).thenReturn(false);

    // Second call (lowercased) returns columns
    when(mockMeta.getColumns(null, "my_schema", "my_table", null)).thenReturn(mockRsFound);
    when(mockRsFound.next()).thenReturn(true, false);
    when(mockRsFound.getString("COLUMN_NAME")).thenReturn("id");
    when(mockRsFound.getString("TYPE_NAME")).thenReturn("INTEGER");

    // Override the dml behavior for the test to provide the mock DataSource
    DatastreamToPostgresDML spyDml =
        new DatastreamToPostgresDML(null) {
          @Override
          public DataSource getDataSource() {
            return mockDs;
          }
        };
    DatastreamToDML.JdbcTableCache cache = new DatastreamToDML.JdbcTableCache(spyDml);

    // Act
    Map<String, String> schema = cache.getObjectValue(Arrays.asList("", "MY_SCHEMA", "MY_TABLE"));

    // Assert
    assertThat(schema).containsKey("id");
    assertThat(schema.get("id")).isEqualTo("INTEGER");
  }
}
