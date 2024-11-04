/*
 * Copyright (C) 2024 Google LLC
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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.ForeignKey;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidOptionsException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TableSelectorTest {

  private Ddl spannerDdl;

  private Ddl spannerDdlWithExtraTable;

  private Ddl spannerDdlWithLessTable;

  @Before
  public void setup() {
    spannerDdl =
        Ddl.builder()
            .createTable("new_cart")
            .column("new_quantity")
            .int64()
            .notNull()
            .endColumn()
            .column("new_user_id")
            .string()
            .size(10)
            .endColumn()
            .primaryKey()
            .asc("new_user_id")
            .asc("new_quantity")
            .end()
            .endTable()
            .createTable("new_people")
            .column("synth_id")
            .int64()
            .notNull()
            .endColumn()
            .column("new_name")
            .string()
            .size(10)
            .endColumn()
            .primaryKey()
            .asc("synth_id")
            .end()
            .endTable()
            .build();

    spannerDdlWithExtraTable =
        Ddl.builder()
            .createTable("new_cart")
            .column("new_quantity")
            .int64()
            .notNull()
            .endColumn()
            .column("new_user_id")
            .string()
            .size(10)
            .endColumn()
            .primaryKey()
            .asc("new_user_id")
            .asc("new_quantity")
            .end()
            .endTable()
            .createTable("new_people")
            .column("synth_id")
            .int64()
            .notNull()
            .endColumn()
            .column("new_name")
            .string()
            .size(10)
            .endColumn()
            .primaryKey()
            .asc("synth_id")
            .end()
            .endTable()
            .createTable("extra_table")
            .column("synth_id")
            .int64()
            .notNull()
            .endColumn()
            .column("new_name")
            .string()
            .size(10)
            .endColumn()
            .primaryKey()
            .asc("synth_id")
            .end()
            .endTable()
            .build();

    spannerDdlWithLessTable =
        Ddl.builder()
            .createTable("new_cart")
            .column("new_quantity")
            .int64()
            .notNull()
            .endColumn()
            .column("new_user_id")
            .string()
            .size(10)
            .endColumn()
            .primaryKey()
            .asc("new_user_id")
            .asc("new_quantity")
            .end()
            .endTable()
            .build();
  }

  private SourceDbToSpannerOptions createOptionsHelper(String sessionFile, String tables) {
    SourceDbToSpannerOptions mockOptions =
        mock(SourceDbToSpannerOptions.class, Mockito.withSettings().serializable());
    when(mockOptions.getSessionFilePath()).thenReturn(sessionFile);
    when(mockOptions.getTables()).thenReturn(tables);
    return mockOptions;
  }

  @Test
  public void listTablesToMigrateIdentity() {
    SourceDbToSpannerOptions mockOptions = createOptionsHelper("", "");
    ISchemaMapper schemaMapper = PipelineController.getSchemaMapper(mockOptions, spannerDdl);
    TableSelector tableSelector =
        new TableSelector(mockOptions.getTables(), spannerDdl, schemaMapper);
    List<String> tables = tableSelector.getSrcTablesToMigrate();
    List<String> ddlTables =
        spannerDdl.allTables().stream().map(t -> t.name()).collect(Collectors.toList());
    assertEquals(2, tables.size());
    assertTrue(ddlTables.containsAll(tables));
  }

  @Test
  public void listTablesToMigrateIdentityOverride() {
    SourceDbToSpannerOptions mockOptions = createOptionsHelper("", "new_cart");
    ISchemaMapper schemaMapper = PipelineController.getSchemaMapper(mockOptions, spannerDdl);
    TableSelector tableSelector =
        new TableSelector(mockOptions.getTables(), spannerDdl, schemaMapper);
    List<String> tables = tableSelector.getSrcTablesToMigrate();
    List<String> ddlTables =
        spannerDdl.allTables().stream().map(t -> t.name()).collect(Collectors.toList());
    assertEquals(1, tables.size());
    assertTrue(ddlTables.containsAll(tables));
  }

  @Test
  public void listTablesToMigrateSession() {
    SourceDbToSpannerOptions mockOptions =
        createOptionsHelper(
            Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
                .toString(),
            "cart,people");
    ISchemaMapper schemaMapper = PipelineController.getSchemaMapper(mockOptions, spannerDdl);
    TableSelector tableSelector =
        new TableSelector(mockOptions.getTables(), spannerDdl, schemaMapper);
    List<String> tables = tableSelector.getSrcTablesToMigrate();

    assertEquals(2, tables.size());
    assertTrue(tables.contains("cart"));
    assertTrue(tables.contains("people"));
  }

  @Test
  public void listTablesToMigrateSessionOverride() {
    SourceDbToSpannerOptions mockOptions =
        createOptionsHelper(
            Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
                .toString(),
            "cart");
    ISchemaMapper schemaMapper = PipelineController.getSchemaMapper(mockOptions, spannerDdl);
    TableSelector tableSelector =
        new TableSelector(mockOptions.getTables(), spannerDdl, schemaMapper);
    List<String> tables = tableSelector.getSrcTablesToMigrate();

    assertEquals(1, tables.size());
    assertTrue(tables.contains("cart"));
  }

  @Test(expected = InvalidOptionsException.class)
  public void listTablesToMigrateSessionOverrideInvalid() {
    SourceDbToSpannerOptions mockOptions =
        createOptionsHelper(
            Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
                .toString(),
            "asd");
    ISchemaMapper schemaMapper = PipelineController.getSchemaMapper(mockOptions, spannerDdl);
    TableSelector tableSelector =
        new TableSelector(mockOptions.getTables(), spannerDdl, schemaMapper);
    List<String> tables = tableSelector.getSrcTablesToMigrate();
  }

  @Test
  public void spannerTablesToMigrateSession() {
    SourceDbToSpannerOptions mockOptions =
        createOptionsHelper(
            Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
                .toString(),
            "cart,people");
    ISchemaMapper schemaMapper =
        PipelineController.getSchemaMapper(mockOptions, spannerDdlWithExtraTable);
    TableSelector tableSelector =
        new TableSelector(mockOptions.getTables(), spannerDdl, schemaMapper);
    List<String> tables = tableSelector.getSpTablesToMigrate();

    assertEquals(2, tables.size());
    assertTrue(tables.contains("new_cart"));
    assertTrue(tables.contains("new_people"));
  }

  @Test
  public void spannerTablesToMigrateSessionWithExtraTable() {
    SourceDbToSpannerOptions mockOptions =
        createOptionsHelper(
            Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
                .toString(),
            "cart,people");
    ISchemaMapper schemaMapper =
        PipelineController.getSchemaMapper(mockOptions, spannerDdlWithExtraTable);
    TableSelector tableSelector =
        new TableSelector(mockOptions.getTables(), spannerDdl, schemaMapper);
    List<String> tables = tableSelector.getSpTablesToMigrate();

    assertEquals(2, tables.size());
    assertTrue(tables.contains("new_cart"));
    assertTrue(tables.contains("new_people"));
  }

  @Test
  public void spannerTablesToMigrateSessionWithLessTable() {
    SourceDbToSpannerOptions mockOptions =
        createOptionsHelper(
            Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
                .toString(),
            "cart,people");
    ISchemaMapper schemaMapper =
        PipelineController.getSchemaMapper(mockOptions, spannerDdlWithLessTable);
    TableSelector tableSelector =
        new TableSelector(mockOptions.getTables(), spannerDdlWithLessTable, schemaMapper);
    List<String> tables = tableSelector.getSpTablesToMigrate();

    assertEquals(1, tables.size());
    assertTrue(tables.contains("new_cart"));
  }

  @Test
  public void spannerTablesToMigrateSessionWithLessTables() {
    SourceDbToSpannerOptions mockOptions =
        createOptionsHelper(
            Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
                .toString(),
            "cart");
    ISchemaMapper schemaMapper =
        PipelineController.getSchemaMapper(mockOptions, spannerDdlWithExtraTable);
    TableSelector tableSelector =
        new TableSelector(mockOptions.getTables(), spannerDdlWithExtraTable, schemaMapper);
    List<String> tables = tableSelector.getSpTablesToMigrate();

    assertEquals(1, tables.size());
    assertTrue(tables.contains("new_cart"));
  }

  @Test
  public void testLevelOrderedTables() {
    // Test 1: Linear dag
    SourceDbToSpannerOptions mockOptions = createOptionsHelper("", "");
    List<List<String>> dagDependencies =
        Arrays.asList(
            Arrays.asList("t3", "t1"), Arrays.asList("t1", "t4"), Arrays.asList("t4", "t2"));
    List<String> dagTableNames = Arrays.asList("t1", "t2", "t3", "t4");

    runTableOrderTest(
        "dag",
        "",
        dagTableNames,
        dagDependencies,
        Arrays.asList(
            Arrays.asList("t2"), Arrays.asList("t4"), Arrays.asList("t1"), Arrays.asList("t3")));

    // Test 2: Diamond shaped
    List<List<String>> diamondDependencies =
        Arrays.asList(
            Arrays.asList("t1", "t3"),
            Arrays.asList("t1", "t2"),
            Arrays.asList("t2", "t4"),
            Arrays.asList("t3", "t4"));
    List<String> diamondTableNames = Arrays.asList("t1", "t2", "t3", "t4");
    runTableOrderTest(
        "diamond",
        "",
        diamondTableNames,
        diamondDependencies,
        Arrays.asList(Arrays.asList("t4"), Arrays.asList("t2", "t3"), Arrays.asList("t1")));

    // Test 3: Empty Dependency List
    runTableOrderTest(
        "no_dependency",
        "",
        Arrays.asList("t1", "t2"),
        new ArrayList<>(),
        Arrays.asList(Arrays.asList("t1", "t2")));
    // Test 4: Single Table (No Dependencies)
    runTableOrderTest(
        "single_table",
        "",
        Arrays.asList("t1"),
        new ArrayList<>(),
        Arrays.asList(Arrays.asList("t1")));

    // Test 5: Disconnected Components
    List<List<String>> disconnectDependencies = Arrays.asList(Arrays.asList("t2", "t1"));
    List<String> disconnectedTableNames = Arrays.asList("t1", "t2", "t3");
    runTableOrderTest(
        "disconnected",
        "",
        disconnectedTableNames,
        disconnectDependencies,
        Arrays.asList(Arrays.asList("t1", "t3"), Arrays.asList("t2")));
  }

  @Test
  public void testLevelOrderedTablesComplex() {
    // Test 6: Complex Graph
    List<List<String>> complexDependencies =
        Arrays.asList(
            Arrays.asList("t14", "t15"),
            Arrays.asList("t14", "t8"),
            Arrays.asList("t14", "t13"),
            Arrays.asList("t13", "t8"),
            Arrays.asList("t15", "t13"),
            Arrays.asList("t8", "t4"),
            Arrays.asList("t8", "t7"),
            Arrays.asList("t8", "t12"),
            Arrays.asList("t4", "t3"),
            Arrays.asList("t7", "t6"),
            Arrays.asList("t7", "t10"),
            Arrays.asList("t12", "t11"),
            Arrays.asList("t6", "t5"),
            Arrays.asList("t10", "t6"),
            Arrays.asList("t10", "t9"),
            Arrays.asList("t9", "t5"),
            Arrays.asList("t5", "t3"),
            Arrays.asList("t11", "t3"),
            Arrays.asList("t3", "t2"),
            Arrays.asList("t3", "t1"),
            Arrays.asList("t2", "t1"));
    List<String> complexTableNames =
        Arrays.asList(
            "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10", "t11", "t12", "t13", "t14",
            "t15");
    List<List<String>> expectedOutput =
        Arrays.asList(
            Arrays.asList("t1"),
            Arrays.asList("t2"),
            Arrays.asList("t3"),
            Arrays.asList("t4", "t5", "t11"),
            Arrays.asList("t6", "t9", "t12"),
            Arrays.asList("t10"),
            Arrays.asList("t7"),
            Arrays.asList("t8"),
            Arrays.asList("t13"),
            Arrays.asList("t15"),
            Arrays.asList("t14"));
    runTableOrderTest("complex", "", complexTableNames, complexDependencies, expectedOutput);
  }

  @Test
  public void testLevelOrderSpecialTableName() {
    // Test 1: Linear dag - Capital Table names
    SourceDbToSpannerOptions mockOptions = createOptionsHelper("", "");
    List<List<String>> dagDependencies =
        Arrays.asList(
            Arrays.asList("T3", "T1"), Arrays.asList("T1", "T4"), Arrays.asList("T4", "T2"));
    List<String> dagTableNames = Arrays.asList("T1", "T2", "T3", "T4");

    runTableOrderTest(
        "dag",
        "",
        dagTableNames,
        dagDependencies,
        Arrays.asList(
            Arrays.asList("T2"), Arrays.asList("T4"), Arrays.asList("T1"), Arrays.asList("T3")));
  }

  @Test(expected = Exception.class)
  public void testLevelOrderedTablesNoMatch() {
    runTableOrderTest(
        "no tables", "", Arrays.asList(), new ArrayList<>(), Arrays.asList(Arrays.asList()));
  }

  @Test(expected = Exception.class)
  public void testLevelOrderedTablesCyclic() {
    // Test 7: Cyclic Dependency
    SourceDbToSpannerOptions mockOptions = createOptionsHelper("", "");

    List<List<String>> dependencies =
        Arrays.asList(
            Arrays.asList("t1", "t2"), Arrays.asList("t2", "t3"), Arrays.asList("t3", "t1"));
    List<String> tableNames = Arrays.asList("t1", "t2", "t3");
    Ddl ddl = generateDdlFromDAG(tableNames, dependencies);
    ISchemaMapper schemaMapper = PipelineController.getSchemaMapper(mockOptions, ddl);
    TableSelector tableSelector =
        new TableSelector(tableNames.stream().collect(Collectors.joining(",")), ddl, schemaMapper);
    tableSelector.levelOrderedSpannerTables();
  }

  private void runTableOrderTest(
      String testName,
      String configuredTables,
      List<String> ddlTables,
      List<List<String>> dependencies,
      List<List<String>> expectedOutput) {
    SourceDbToSpannerOptions mockOptions = createOptionsHelper("", configuredTables);
    Ddl ddl = generateDdlFromDAG(ddlTables, dependencies);
    ISchemaMapper schemaMapper = PipelineController.getSchemaMapper(mockOptions, ddl);
    TableSelector tableSelector =
        new TableSelector(ddlTables.stream().collect(Collectors.joining(",")), ddl, schemaMapper);
    Map<Integer, List<String>> tableLevelMap = tableSelector.levelOrderedSpannerTables();
    for (int i = 0; i < expectedOutput.size(); i++) {
      assertEquals(
          testName + "_level_" + i, expectedOutput.get(i).size(), tableLevelMap.get(i).size());
      assertTrue(testName + "_level_" + i, tableLevelMap.get(i).containsAll(expectedOutput.get(i)));
    }
    assertEquals(expectedOutput.size(), tableLevelMap.size());
  }

  /**
   * Referenced from DdlTest - To figure a cleaner way to share the code Generates a Spanner DDL
   * object representing a schema from a directed acyclic graph (DAG) of table dependencies.
   *
   * <p>This method creates a simplified DDL with minimal schema information. Each table has only an
   * "id" column as the primary key. Foreign key relationships are established based on the provided
   * dependencies.
   *
   * @param tableNames A list of table names in the schema.
   * @param dependencies A list of foreign key dependencies, where each dependency is represented as
   *     a list of two strings: the child table name and the parent table name, where child
   *     references the parent.
   */
  private Ddl generateDdlFromDAG(List<String> tableNames, List<List<String>> dependencies) {
    Ddl.Builder builder = Ddl.builder();
    for (String tableName : tableNames) {
      Table.Builder tableBuilder =
          builder
              .createTable(tableName)
              .column("id")
              .int64()
              .endColumn()
              .primaryKey()
              .asc("id")
              .end();

      // Add foreign keys based on dependencies.
      List<ForeignKey> fks = new ArrayList<>();
      for (List<String> dependency : dependencies) {
        if (dependency.get(0).equals(tableName)) {
          String parentTable = dependency.get(1);

          ForeignKey.Builder fkBuilder =
              ForeignKey.builder(Dialect.GOOGLE_STANDARD_SQL)
                  .name("fk_" + tableName + "_" + parentTable)
                  .table(tableName)
                  .referencedTable(parentTable);
          fkBuilder.columnsBuilder().add("id");
          fkBuilder.referencedColumnsBuilder().add("id");
          fks.add(fkBuilder.build());
        }
      }
      tableBuilder.foreignKeys(ImmutableList.copyOf(fks)).endTable();
    }

    return builder.build();
  }
}
