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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/** Test cases for DDL generation in {@link DatastreamToDML} subclasses. */
public class DatastreamToDDLTest {

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
}
