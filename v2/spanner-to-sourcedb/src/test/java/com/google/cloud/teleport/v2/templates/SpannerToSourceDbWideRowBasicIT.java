/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link SpannerToSourceDb} Flex template for all data types. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
@Ignore("This test is disabled currently")
public class SpannerToSourceDbWideRowBasicIT extends SpannerToSourceDbITBase {
  private static final String testName = "test_" + System.currentTimeMillis();

  @Test
  public void testAssert5000TablesPerDatabase() throws Exception {
    String databaseName = "rr-main-db-test-" + testName;
    SpannerResourceManager spannerResourceManagerForTables =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    List<String> createTableQueries = getTablesCreatedDdlQueryStrings(5000);

    for (int i = 0; i < createTableQueries.size(); i += 100) {
      int end = Math.min(i + 100, createTableQueries.size());
      spannerResourceManagerForTables.executeDdlStatements(createTableQueries.subList(i, end));
    }

    String query = "SELECT COUNT(*) AS table_count FROM INFORMATION_SCHEMA.TABLES";
    ImmutableList<Struct> results = spannerResourceManagerForTables.runQuery(query);
    assertFalse(results.isEmpty());
    long tableCount = results.get(0).getLong(0);
    assertEquals(5000, tableCount);
    ResourceManagerUtils.cleanResources(spannerResourceManagerForTables);
  }

  private static @NotNull List<String> getTablesCreatedDdlQueryStrings(int size) {
    List<String> createTableQueries = new ArrayList<>();
    for (int tableNum = 1; tableNum <= size; tableNum++) {
      String tableName = "Table_" + tableNum;
      createTableQueries.add(
          String.format(
              "CREATE TABLE %s (\n"
                  + "  Id INT64 NOT NULL,\n"
                  + "  Name STRING(100)\n"
                  + ") PRIMARY KEY (Id)",
              tableName));
    }
    return createTableQueries;
  }

  @Test
  public void testCreateDatabaseAndTableWith1024Columns() throws Exception {
    String databaseName = "rr-main-table-per-columns-" + testName; // 🔹 Ensure unique DB name
    SpannerResourceManager spannerResourceManagerForColumnsPerTable =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    StringBuilder createTableQuery = new StringBuilder("CREATE TABLE TestTable (\n");
    createTableQuery.append("    Id INT64 NOT NULL,\n");
    for (int i = 1; i < 1024; i++) {
      createTableQuery.append("    Col_").append(i).append(" STRING(100),\n");
    }
    createTableQuery.setLength(createTableQuery.length() - 2);
    createTableQuery.append("\n) PRIMARY KEY (Id)");

    spannerResourceManagerForColumnsPerTable.executeDdlStatement(createTableQuery.toString());

    String query =
        "SELECT COUNT(*) AS column_count FROM INFORMATION_SCHEMA.COLUMNS "
            + "WHERE TABLE_NAME = 'TestTable' AND TABLE_CATALOG = '' AND TABLE_SCHEMA = ''";

    ImmutableList<Struct> results = spannerResourceManagerForColumnsPerTable.runQuery(query);
    assertFalse(results.isEmpty());
    long columnCount = results.get(0).getLong(0);
    assertEquals(1024L, columnCount);
    ResourceManagerUtils.cleanResources(spannerResourceManagerForColumnsPerTable);
  }

  @Test
  public void testCreateTableWithMoreThan1024ColumnsShouldFail() {
    String databaseName = "rr-main-table-per-columns-exception-" + testName;
    SpannerResourceManager spannerResourceManagerForColumns =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    StringBuilder createTableQuery = new StringBuilder("CREATE TABLE InvalidTable (\n");
    createTableQuery.append("    Id INT64 NOT NULL,\n");
    for (int i = 1; i <= 1024; i++) {
      createTableQuery.append("    Col_").append(i).append(" STRING(100),\n");
    }
    createTableQuery.append(") PRIMARY KEY (Id)");
    Exception thrownException = null;
    try {
      spannerResourceManagerForColumns.executeDdlStatement(createTableQuery.toString());
    } catch (Exception e) {
      thrownException = e;
    }

    assertNotNull("Expected SpannerException due to exceeding column limit", thrownException);
    assertTrue(
        "Exception message should mention column limit",
        thrownException.getMessage().contains("too many columns"));

    ResourceManagerUtils.cleanResources(spannerResourceManagerForColumns);
  }

  @Test
  public void testCreateMoreThan5000TablesShouldFail() {
    String databaseName = "rr-main-db-exceed-tables-" + testName;
    SpannerResourceManager spannerResourceManagerForTables =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    List<String> createTableQueries = getTablesCreatedDdlQueryStrings(5001);
    Exception thrownException = null;
    try {
      spannerResourceManagerForTables.executeDdlStatements(createTableQueries);
    } catch (Exception e) {
      thrownException = e;
    }
    assertNotNull("Expected SpannerException due to exceeding table limit", thrownException);
    assertTrue(
        "Exception message should mention table limit",
        thrownException.getMessage().contains("too many tables"));
    ResourceManagerUtils.cleanResources(spannerResourceManagerForTables);
  }

  @Test
  public void testInsertValidCellSize_10MiB() {
    String databaseName = "rr-main-db-cell-size-valid-" + testName;
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    String createTableQuery =
        "CREATE TABLE LargeDataTest ("
            + "  Id INT64 NOT NULL,"
            + "  LargeColumn STRING(MAX),"
            + ") PRIMARY KEY (Id)";
    spannerResourceManager.executeDdlStatement(createTableQuery);

    String validData = "A".repeat(2_621_440);

    spannerResourceManager.write(
        Mutation.newInsertBuilder("LargeDataTest")
            .set("Id")
            .to(1)
            .set("LargeColumn")
            .to(validData)
            .build());

    String query = "SELECT LENGTH(LargeColumn) FROM LargeDataTest WHERE Id = 1";
    ImmutableList<Struct> results = spannerResourceManager.runQuery(query);
    assertFalse(results.isEmpty());
    long columnSize = results.get(0).getLong(0);
    assertEquals(2_621_440, columnSize);
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void testInsertInvalidCellSize_Exceeds10MiB_ShouldFail() {
    String databaseName = "rr-main-db-cell-size-invalid-" + testName;
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    String createTableQuery =
        "CREATE TABLE LargeDataTest ("
            + "  Id INT64 NOT NULL,"
            + "  LargeColumn STRING(MAX),"
            + ") PRIMARY KEY (Id)";
    spannerResourceManager.executeDdlStatement(createTableQuery);

    String invalidData = "A".repeat(10 * 1024 * 1024 + 1);

    Exception thrownException = null;
    try {
      spannerResourceManager.write(
          Mutation.newInsertBuilder("LargeDataTest")
              .set("Id")
              .to(2)
              .set("LargeColumn")
              .to(invalidData)
              .build());
    } catch (Exception e) {
      thrownException = e;
    }

    assertNotNull("Expected SpannerException due to exceeding cell size limit", thrownException);
    assertTrue(
        "Exception message should mention cell size limit",
        thrownException.getMessage().contains("too large"));
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void testInsertValidStringSize_2621440Characters() {
    String databaseName = "rr-main-db-string-size-valid-" + testName;
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    String createTableQuery =
        "CREATE TABLE LargeStringTest ("
            + "  Id INT64 NOT NULL,"
            + "  LargeColumn STRING(2621440),"
            + ") PRIMARY KEY (Id)";
    spannerResourceManager.executeDdlStatement(createTableQuery);

    String validData = "A".repeat(2_621_440);

    spannerResourceManager.write(
        Mutation.newInsertBuilder("LargeStringTest")
            .set("Id")
            .to(1)
            .set("LargeColumn")
            .to(validData)
            .build());

    String query = "SELECT LENGTH(LargeColumn) FROM LargeStringTest WHERE Id = 1";
    ImmutableList<Struct> results = spannerResourceManager.runQuery(query);
    assertFalse(results.isEmpty());
    long columnSize = results.get(0).getLong(0);
    assertEquals(2_621_440, columnSize);

    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void testInsertInvalidStringSize_Exceeds2621440Characters_ShouldFail() {
    String databaseName = "rr-main-db-string-size-invalid-" + testName;
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    String createTableQuery =
        "CREATE TABLE LargeStringTest ("
            + "  Id INT64 NOT NULL,"
            + "  LargeColumn STRING(2621440),"
            + ") PRIMARY KEY (Id)";
    spannerResourceManager.executeDdlStatement(createTableQuery);

    String invalidData = "A".repeat(2_621_441);

    Exception thrownException = null;
    try {
      spannerResourceManager.write(
          Mutation.newInsertBuilder("LargeStringTest")
              .set("Id")
              .to(2)
              .set("LargeColumn")
              .to(invalidData)
              .build());
    } catch (Exception e) {
      thrownException = e;
    }

    assertNotNull(
        "Expected SpannerException due to exceeding string column limit", thrownException);
    assertTrue(
        "Exception message should mention column size limit",
        thrownException
            .getMessage()
            .contains("New value exceeds the maximum size limit for this column"));
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void testInsertValidPrimaryKeySize_8KB() {
    String databaseName = "rr-main-db-key-valid-" + testName;
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    String createTableQuery =
        "CREATE TABLE LargeKeyTest ("
            + "  LargeKey STRING(8192) NOT NULL,"
            + "  Value STRING(MAX)"
            + ") PRIMARY KEY (LargeKey)";
    spannerResourceManager.executeDdlStatement(createTableQuery);

    String validKey = "K".repeat(8192);

    spannerResourceManager.write(
        Mutation.newInsertBuilder("LargeKeyTest")
            .set("LargeKey")
            .to(validKey)
            .set("Value")
            .to("Some Data")
            .build());

    String query = "SELECT LENGTH(LargeKey) FROM LargeKeyTest WHERE LargeKey = @validKey";
    ImmutableList<Struct> results =
        spannerResourceManager.runQuery(
            Statement.newBuilder(query).bind("validKey").to(validKey).build().getSql());
    assertFalse(results.isEmpty());
    long keySize = results.get(0).getLong(0);
    assertEquals(8192, keySize);

    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void testInsertInvalidPrimaryKeySize_Exceeds8KB_ShouldFail() {
    String databaseName = "rr-main-db-key-invalid-" + testName;
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder(databaseName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    String createTableQuery =
        "CREATE TABLE LargeKeyTest ("
            + "  LargeKey STRING(8193) NOT NULL,"
            + "  Value STRING(MAX)"
            + ") PRIMARY KEY (LargeKey)";

    Exception thrownException = null;
    try {
      spannerResourceManager.executeDdlStatement(createTableQuery);
    } catch (Exception e) {
      thrownException = e;
    }

    assertNotNull("Expected SpannerException due to exceeding key size limit", thrownException);
    assertTrue(
        "Exception message should mention primary key size limit",
        thrownException.getMessage().contains("Key size exceeds limit"));

    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }
}
