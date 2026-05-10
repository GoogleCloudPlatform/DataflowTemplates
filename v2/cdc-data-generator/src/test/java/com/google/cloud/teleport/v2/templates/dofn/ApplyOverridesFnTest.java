/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.dofn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.model.SchemaConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jasonclawson.jackson.dataformat.hocon.HoconFactory;
import java.util.Arrays;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ApplyOverridesFnTest {

  private SchemaConfig parseConfig(String hoconContent) throws Exception {
    ObjectMapper mapper = new ObjectMapper(new HoconFactory());
    return mapper.readValue(hoconContent, SchemaConfig.class);
  }

  @Test
  public void testProcessElement_NoConfigPath() throws Exception {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                ImmutableMap.of(
                    "table1",
                    DataGeneratorTable.builder()
                        .name("table1")
                        .columns(ImmutableList.of())
                        .primaryKeys(ImmutableList.of())
                        .foreignKeys(ImmutableList.of())
                        .uniqueKeys(ImmutableList.of())
                        .build()))
            .build();

    ApplyOverridesFn fn = new ApplyOverridesFn(null, 100, 10, 1);
    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    ArgumentCaptor<DataGeneratorSchema> captor = ArgumentCaptor.forClass(DataGeneratorSchema.class);

    fn.processElement(schema, receiver);

    verify(receiver).output(captor.capture());
    DataGeneratorSchema resolvedSchema = captor.getValue();
    assertNotNull(resolvedSchema);
    DataGeneratorTable table = resolvedSchema.tables().get("table1");
    assertNotNull(table);
    assertEquals(100L, (long) table.insertQps());
    assertEquals(10L, (long) table.updateQps());
    assertEquals(1L, (long) table.deleteQps());
  }

  @Test
  public void testProcessElement_ConfigWithNullTables() throws Exception {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                ImmutableMap.of(
                    "table1",
                    DataGeneratorTable.builder()
                        .name("table1")
                        .columns(ImmutableList.of())
                        .primaryKeys(ImmutableList.of())
                        .foreignKeys(ImmutableList.of())
                        .uniqueKeys(ImmutableList.of())
                        .build()))
            .build();

    SchemaConfig configWithNullTables = new SchemaConfig();
    ApplyOverridesFn fn = new ApplyOverridesFn(configWithNullTables, 100, 10, 1);
    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    ArgumentCaptor<DataGeneratorSchema> captor = ArgumentCaptor.forClass(DataGeneratorSchema.class);

    fn.processElement(schema, receiver);

    verify(receiver).output(captor.capture());
    DataGeneratorSchema resolvedSchema = captor.getValue();
    assertNotNull(resolvedSchema);
    DataGeneratorTable table = resolvedSchema.tables().get("table1");
    assertNotNull(table);
    assertEquals(100L, (long) table.insertQps());
  }

  @Test
  public void testProcessElement_WithTableOverrides() throws Exception {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                ImmutableMap.of(
                    "table1",
                    DataGeneratorTable.builder()
                        .name("table1")
                        .columns(ImmutableList.of())
                        .primaryKeys(ImmutableList.of())
                        .foreignKeys(ImmutableList.of())
                        .uniqueKeys(ImmutableList.of())
                        .build()))
            .build();

    String hoconContent = "tables {\n  table1 {\n    insertQps = 500\n  }\n}\n";
    SchemaConfig config = parseConfig(hoconContent);

    ApplyOverridesFn fn = new ApplyOverridesFn(config, 100, 10, 1);
    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    ArgumentCaptor<DataGeneratorSchema> captor = ArgumentCaptor.forClass(DataGeneratorSchema.class);

    fn.processElement(schema, receiver);

    verify(receiver).output(captor.capture());
    DataGeneratorSchema resolvedSchema = captor.getValue();
    DataGeneratorTable table = resolvedSchema.tables().get("table1");
    assertEquals(500L, (long) table.insertQps());
    assertEquals(10L, (long) table.updateQps());
    assertEquals(1L, (long) table.deleteQps());
  }

  @Test
  public void testProcessElement_WithColumnOverrides() throws Exception {
    DataGeneratorColumn col =
        DataGeneratorColumn.builder()
            .name("email")
            .logicalType(LogicalType.STRING)
            .isNullable(true)
            .isPrimaryKey(false)
            .isGenerated(false)
            .build();
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                ImmutableMap.of(
                    "table1",
                    DataGeneratorTable.builder()
                        .name("table1")
                        .columns(ImmutableList.of(col))
                        .primaryKeys(ImmutableList.of())
                        .foreignKeys(ImmutableList.of())
                        .uniqueKeys(ImmutableList.of())
                        .build()))
            .build();

    String hoconContent =
        "tables {\n  table1 {\n    columns {\n      email {\n        fakerExpression = \"#{internet.emailAddress}\"\n        skip = true\n      }\n    }\n  }\n}\n";
    SchemaConfig config = parseConfig(hoconContent);

    ApplyOverridesFn fn = new ApplyOverridesFn(config, 100, 10, 1);
    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    ArgumentCaptor<DataGeneratorSchema> captor = ArgumentCaptor.forClass(DataGeneratorSchema.class);

    fn.processElement(schema, receiver);

    verify(receiver).output(captor.capture());
    DataGeneratorSchema resolvedSchema = captor.getValue();
    DataGeneratorTable table = resolvedSchema.tables().get("table1");
    DataGeneratorColumn updatedCol = table.columns().get(0);
    assertEquals("#{internet.emailAddress}", updatedCol.fakerExpression());
    assertTrue(updatedCol.isSkipped());
  }

  @Test
  public void testProcessElement_SkipPrimaryKeyThrows() throws Exception {
    DataGeneratorColumn col =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isPrimaryKey(true)
            .isGenerated(false)
            .build();
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                ImmutableMap.of(
                    "table1",
                    DataGeneratorTable.builder()
                        .name("table1")
                        .columns(ImmutableList.of(col))
                        .primaryKeys(ImmutableList.of("id"))
                        .foreignKeys(ImmutableList.of())
                        .uniqueKeys(ImmutableList.of())
                        .build()))
            .build();

    String hoconContent =
        "tables {\n  table1 {\n    columns {\n      id {\n        skip = true\n      }\n    }\n  }\n}\n";
    SchemaConfig config = parseConfig(hoconContent);

    ApplyOverridesFn fn = new ApplyOverridesFn(config, 100, 10, 1);
    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);

    assertThrows(RuntimeException.class, () -> fn.processElement(schema, receiver));
  }

  @Test
  public void testProcessElement_WithForeignKeys() throws Exception {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                ImmutableMap.of(
                    "table1",
                    DataGeneratorTable.builder()
                        .name("table1")
                        .columns(ImmutableList.of())
                        .primaryKeys(ImmutableList.of())
                        .foreignKeys(ImmutableList.of())
                        .uniqueKeys(ImmutableList.of())
                        .build()))
            .build();

    String hoconContent =
        "tables {\n  table1 {\n    foreignKeys = [\n      {\n        name = \"fk1\"\n        referencedTable = \"table2\"\n        keyColumns = [\"col1\"]\n        referencedColumns = [\"col2\"]\n      }\n    ]\n  }\n}\n";
    SchemaConfig config = parseConfig(hoconContent);

    ApplyOverridesFn fn = new ApplyOverridesFn(config, 100, 10, 1);
    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    ArgumentCaptor<DataGeneratorSchema> captor = ArgumentCaptor.forClass(DataGeneratorSchema.class);

    fn.processElement(schema, receiver);

    verify(receiver).output(captor.capture());
    DataGeneratorSchema resolvedSchema = captor.getValue();
    DataGeneratorTable table = resolvedSchema.tables().get("table1");
    assertEquals(1, table.foreignKeys().size());
    DataGeneratorForeignKey fk = table.foreignKeys().get(0);
    assertEquals("fk1", fk.name());
    assertEquals("table2", fk.referencedTable());
    assertEquals(Arrays.asList("col1"), fk.keyColumns());
    assertEquals(Arrays.asList("col2"), fk.referencedColumns());
  }

  @Test
  public void testProcessElement_FkConflictThrows() throws Exception {
    DataGeneratorForeignKey existingFk =
        DataGeneratorForeignKey.builder()
            .name("fk1")
            .referencedTable("table2")
            .keyColumns(ImmutableList.of("col1"))
            .referencedColumns(ImmutableList.of("col2"))
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                ImmutableMap.of(
                    "table1",
                    DataGeneratorTable.builder()
                        .name("table1")
                        .columns(ImmutableList.of())
                        .primaryKeys(ImmutableList.of())
                        .foreignKeys(ImmutableList.of(existingFk))
                        .uniqueKeys(ImmutableList.of())
                        .build()))
            .build();

    String hoconContent =
        "tables {\n  table1 {\n    foreignKeys = [\n      {\n        name = \"fk1\"\n        referencedTable = \"table3\"\n        keyColumns = [\"col1\"]\n        referencedColumns = [\"col2\"]\n      }\n    ]\n  }\n}\n";
    SchemaConfig config = parseConfig(hoconContent);

    ApplyOverridesFn fn = new ApplyOverridesFn(config, 100, 10, 1);
    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);

    assertThrows(IllegalArgumentException.class, () -> fn.processElement(schema, receiver));
  }

  @Test
  public void testProcessElement_FkEquivalentNoOp() throws Exception {
    DataGeneratorForeignKey existingFk =
        DataGeneratorForeignKey.builder()
            .name("fk1")
            .referencedTable("table2")
            .keyColumns(ImmutableList.of("col1"))
            .referencedColumns(ImmutableList.of("col2"))
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                ImmutableMap.of(
                    "table1",
                    DataGeneratorTable.builder()
                        .name("table1")
                        .columns(ImmutableList.of())
                        .primaryKeys(ImmutableList.of())
                        .foreignKeys(ImmutableList.of(existingFk))
                        .uniqueKeys(ImmutableList.of())
                        .build()))
            .build();

    String hoconContent =
        "tables {\n  table1 {\n    foreignKeys = [\n      {\n        name = \"fk1\"\n        referencedTable = \"table2\"\n        keyColumns = [\"col1\"]\n        referencedColumns = [\"col2\"]\n      }\n    ]\n  }\n}\n";
    SchemaConfig config = parseConfig(hoconContent);

    ApplyOverridesFn fn = new ApplyOverridesFn(config, 100, 10, 1);
    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    ArgumentCaptor<DataGeneratorSchema> captor = ArgumentCaptor.forClass(DataGeneratorSchema.class);

    fn.processElement(schema, receiver);

    verify(receiver).output(captor.capture());
    DataGeneratorSchema resolvedSchema = captor.getValue();
    DataGeneratorTable table = resolvedSchema.tables().get("table1");
    assertEquals(1, table.foreignKeys().size());
  }

  @Test
  public void testProcessElement_UnknownTableOverrideWarns() throws Exception {
    DataGeneratorSchema schema = DataGeneratorSchema.builder().tables(ImmutableMap.of()).build();

    String hoconContent = "tables {\n  unknown_table {\n    insertQps = 500\n  }\n}\n";
    SchemaConfig config = parseConfig(hoconContent);

    ApplyOverridesFn fn = new ApplyOverridesFn(config, 100, 10, 1);
    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    ArgumentCaptor<DataGeneratorSchema> captor = ArgumentCaptor.forClass(DataGeneratorSchema.class);

    fn.processElement(schema, receiver);

    verify(receiver).output(captor.capture());
    DataGeneratorSchema resolvedSchema = captor.getValue();
    assertTrue(resolvedSchema.tables().isEmpty());
  }

  @Test
  public void testProcessElement_WithUpdateAndDeleteQpsOverrides() throws Exception {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                ImmutableMap.of(
                    "table1",
                    DataGeneratorTable.builder()
                        .name("table1")
                        .columns(ImmutableList.of())
                        .primaryKeys(ImmutableList.of())
                        .foreignKeys(ImmutableList.of())
                        .uniqueKeys(ImmutableList.of())
                        .build()))
            .build();

    String hoconContent = "tables {\n  table1 {\n    updateQps = 20\n    deleteQps = 5\n  }\n}\n";
    SchemaConfig config = parseConfig(hoconContent);

    ApplyOverridesFn fn = new ApplyOverridesFn(config, 100, 10, 1);
    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    ArgumentCaptor<DataGeneratorSchema> captor = ArgumentCaptor.forClass(DataGeneratorSchema.class);

    fn.processElement(schema, receiver);

    verify(receiver).output(captor.capture());
    DataGeneratorSchema resolvedSchema = captor.getValue();
    DataGeneratorTable table = resolvedSchema.tables().get("table1");
    assertEquals(100L, (long) table.insertQps());
    assertEquals(20L, (long) table.updateQps());
    assertEquals(5L, (long) table.deleteQps());
  }

  @Test
  public void testApplyColumnOverrides_SkipFalseOnPrimaryKey() throws Exception {
    DataGeneratorColumn col =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isPrimaryKey(true)
            .isGenerated(false)
            .build();
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                ImmutableMap.of(
                    "table1",
                    DataGeneratorTable.builder()
                        .name("table1")
                        .columns(ImmutableList.of(col))
                        .primaryKeys(ImmutableList.of("id"))
                        .foreignKeys(ImmutableList.of())
                        .uniqueKeys(ImmutableList.of())
                        .build()))
            .build();

    String hoconContent =
        "tables {\n  table1 {\n    columns {\n      id {\n        skip = false\n      }\n    }\n  }\n}\n";
    SchemaConfig config = parseConfig(hoconContent);

    ApplyOverridesFn fn = new ApplyOverridesFn(config, 100, 10, 1);
    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    ArgumentCaptor<DataGeneratorSchema> captor = ArgumentCaptor.forClass(DataGeneratorSchema.class);

    fn.processElement(schema, receiver);

    verify(receiver).output(captor.capture());
    DataGeneratorSchema resolvedSchema = captor.getValue();
    DataGeneratorTable table = resolvedSchema.tables().get("table1");
    DataGeneratorColumn updatedCol = table.columns().get(0);
    assertTrue(!updatedCol.isSkipped());
  }

  @Test
  public void testConstructorAssignment() throws Exception {
    String hoconContent = "tables {\n  table1 {\n    insertQps = 777\n  }\n}\n";
    SchemaConfig config = parseConfig(hoconContent);

    ApplyOverridesFn fn = new ApplyOverridesFn(config, 100, 10, 1);

    java.lang.reflect.Field field = ApplyOverridesFn.class.getDeclaredField("schemaConfig");
    field.setAccessible(true);
    SchemaConfig assignedConfig = (SchemaConfig) field.get(fn);

    assertNotNull(assignedConfig);
    assertNotNull(assignedConfig.getTables());
    assertTrue(assignedConfig.getTables().containsKey("table1"));
    assertEquals(Integer.valueOf(777), assignedConfig.getTables().get("table1").getInsertQps());
  }
}
