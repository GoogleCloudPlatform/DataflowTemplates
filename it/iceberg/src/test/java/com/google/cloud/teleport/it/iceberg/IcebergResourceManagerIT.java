/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.teleport.it.iceberg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.beam.it.testcontainers.TestContainersIntegrationTest;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for Iceberg Resource Manager. */
@Category(TestContainersIntegrationTest.class)
@RunWith(JUnit4.class)
@Ignore(
    "https://github.com/GoogleCloudPlatform/DataflowTemplates/issues/3224") // re-enable after tests
// fixed
public class IcebergResourceManagerIT {

  private static String warehouseLocation;
  private static IcebergResourceManager resourceManager;
  private static String testNamespace;
  private static String catalog;

  @Before
  public void setUp() throws IOException {
    String testId = UUID.randomUUID().toString();
    java.nio.file.Path warehouseDirectory = Files.createTempDirectory("test-warehouse");
    warehouseLocation = "file:" + warehouseDirectory.toString();
    testNamespace = "namespace";
    catalog = "default";

    resourceManager =
        IcebergResourceManager.builder(testId)
            .setCatalogName(catalog)
            .setCatalogProperties(Map.of("warehouse", warehouseLocation, "type", "hadoop"))
            .build();
  }

  @After
  public void tearDown() {
    if (resourceManager != null) {
      resourceManager.cleanupAll();
    }
  }

  @Test
  public void testCreateNamespace() {
    assertTrue(resourceManager.createNamespace(testNamespace));
    assertTrue(resourceManager.namespaceExists(testNamespace));
    assertFalse(resourceManager.createNamespace(testNamespace));
  }

  @Test
  public void testNamespaceExists() {
    assertFalse(resourceManager.namespaceExists(testNamespace));
    resourceManager.createNamespace(testNamespace);
    assertTrue(resourceManager.namespaceExists(testNamespace));
  }

  @Test
  public void testListNamespaces() {
    String namespace1 = testNamespace + "_1";
    String namespace2 = testNamespace + "_2";

    resourceManager.createNamespace(namespace1);
    resourceManager.createNamespace(namespace2);

    Set<String> namespaces = resourceManager.listNamespaces();
    assertTrue(namespaces.contains(namespace1));
    assertTrue(namespaces.contains(namespace2));
  }

  @Test
  public void testDropNamespace() {
    resourceManager.createNamespace(testNamespace);
    assertTrue(resourceManager.namespaceExists(testNamespace));
    assertTrue(resourceManager.dropNamespace(testNamespace, false));
    assertFalse(resourceManager.namespaceExists(testNamespace));
    assertFalse(resourceManager.dropNamespace(testNamespace, false));
  }

  @Test
  public void testDropNamespaceCascade() {
    resourceManager.createNamespace(testNamespace);
    String tableName = testNamespace + ".test_table_cascade";
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
    resourceManager.createTable(tableName, schema);

    assertTrue(resourceManager.namespaceExists(testNamespace));
    assertNotNull(resourceManager.loadTable(tableName));

    assertTrue(resourceManager.dropNamespace(testNamespace, true));
    assertFalse(resourceManager.namespaceExists(testNamespace));
    assertThrows(IcebergResourceManagerException.class, () -> resourceManager.loadTable(tableName));
  }

  @Test
  public void testCreateTable() {
    resourceManager.createNamespace(testNamespace);
    String tableName = testNamespace + ".test_table_create";
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    Table table = resourceManager.createTable(tableName, schema);

    assertNotNull(table);
    assertEquals(catalog + "." + tableName, table.name());
    assertEquals(schema.asStruct(), table.schema().asStruct());
  }

  @Test
  public void testLoadTable() {
    resourceManager.createNamespace(testNamespace);
    String tableName = testNamespace + ".test_table_load";
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    resourceManager.createTable(tableName, schema);
    Table loadedTable = resourceManager.loadTable(tableName);

    assertNotNull(loadedTable);
    assertEquals(catalog + "." + tableName, loadedTable.name());
    assertEquals(schema.asStruct(), loadedTable.schema().asStruct());
  }

  @Test
  public void testLoadNonExistentTableThrowsException() {
    resourceManager.createNamespace(testNamespace);
    String tableName = testNamespace + ".non_existent_table";
    assertThrows(IcebergResourceManagerException.class, () -> resourceManager.loadTable(tableName));
  }

  @Test
  public void testWriteAndReadRecords() {
    resourceManager.createNamespace(testNamespace);
    String tableName = testNamespace + ".test_table_write_read";
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
    resourceManager.createTable(tableName, schema);

    Map<String, Object> record1 = Map.of("id", 1L, "name", "record1");
    Map<String, Object> record2 = Map.of("id", 2L, "name", "record2");
    List<Map<String, Object>> recordsToWrite = List.of(record1, record2);

    resourceManager.write(tableName, recordsToWrite);

    List<Record> readRecords = resourceManager.read(tableName);

    assertNotNull(readRecords);
    assertEquals(2, readRecords.size());

    readRecords.sort(Comparator.comparingLong(r -> (Long) r.getField("id")));
    // Verify records
    Record actualRecord1 = readRecords.get(0);
    Record actualRecord2 = readRecords.get(1);

    assertEquals(1L, actualRecord1.getField("id"));
    assertEquals("record1", actualRecord1.getField("name"));
    assertEquals(2L, actualRecord2.getField("id"));
    assertEquals("record2", actualRecord2.getField("name"));
  }

  @Test
  public void testCleanupAll() {
    String namespace1 = testNamespace + "_cleanup_1";
    String namespace2 = testNamespace + "_cleanup_2";
    resourceManager.createNamespace(namespace1);
    resourceManager.createNamespace(namespace2);

    String tableName1 = namespace1 + ".test_table_cleanup_1";
    String tableName2 = namespace2 + ".test_table_cleanup_2";
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    resourceManager.createTable(tableName1, schema);
    resourceManager.createTable(tableName2, schema);

    // Verify tables and namespaces exist before cleanup
    assertTrue(resourceManager.namespaceExists(namespace1));
    assertTrue(resourceManager.namespaceExists(namespace2));
    assertNotNull(resourceManager.loadTable(tableName1));
    assertNotNull(resourceManager.loadTable(tableName2));

    resourceManager.cleanupAll();

    // Verify tables and namespaces are dropped after cleanup
    assertFalse(resourceManager.namespaceExists(namespace1));
    assertFalse(resourceManager.namespaceExists(namespace2));
    assertThrows(
        IcebergResourceManagerException.class, () -> resourceManager.loadTable(tableName1));
    assertThrows(
        IcebergResourceManagerException.class, () -> resourceManager.loadTable(tableName2));

    // Verify warehouse directory is empty or deleted
    File warehouseDir = new File(warehouseLocation.replace("file:", ""));
    assertTrue(warehouseDir.exists());
    assertEquals(0, warehouseDir.listFiles().length);
  }

  @Test
  public void testWriteEmptyRecords() {
    resourceManager.createNamespace(testNamespace);
    String tableName = testNamespace + ".test_table_write_empty";
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
    resourceManager.createTable(tableName, schema);

    resourceManager.write(tableName, Collections.emptyList());

    List<Record> readRecords = resourceManager.read(tableName);
    assertTrue(readRecords.isEmpty());
  }

  @Test
  public void testWriteToNonExistentTableThrowsException() {
    resourceManager.createNamespace(testNamespace);
    String tableName = testNamespace + ".non_existent_table_for_write";
    List<Map<String, Object>> recordsToWrite = List.of(Map.of("id", 1L, "name", "record1"));

    // Expect IcebergResourceManagerException because loadTable will fail
    assertThrows(
        IcebergResourceManagerException.class,
        () -> resourceManager.write(tableName, recordsToWrite));
  }

  @Test
  public void testReadFromNonExistentTableThrowsException() {
    resourceManager.createNamespace(testNamespace);
    String tableName = testNamespace + ".non_existent_table_for_read";

    // Expect IcebergResourceManagerException because loadTable will fail
    assertThrows(IcebergResourceManagerException.class, () -> resourceManager.read(tableName));
  }
}
