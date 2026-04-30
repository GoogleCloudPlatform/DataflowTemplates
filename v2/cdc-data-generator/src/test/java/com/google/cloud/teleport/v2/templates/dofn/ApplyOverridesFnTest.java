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

import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.model.SchemaConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

public class ApplyOverridesFnTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testProcessElement_NoConfigPath() {
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
  public void testProcessElement_WithTableOverrides() {
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

    SchemaConfig schemaConfig = new SchemaConfig();
    SchemaConfig.TableConfig tableConfig = new SchemaConfig.TableConfig();
    tableConfig.setInsertQps(500);
    schemaConfig.setTables(ImmutableMap.of("table1", tableConfig));

    ApplyOverridesFn fn = new ApplyOverridesFn(schemaConfig, 100, 10, 1);
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
  public void testProcessElement_WithColumnOverrides() {
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

    SchemaConfig schemaConfig = new SchemaConfig();
    SchemaConfig.TableConfig tableConfig = new SchemaConfig.TableConfig();
    SchemaConfig.ColumnConfig colConfig = new SchemaConfig.ColumnConfig();
    colConfig.setFakerExpression("#{internet.emailAddress}");
    colConfig.setSkip(true);
    tableConfig.setColumns(ImmutableMap.of("email", colConfig));
    schemaConfig.setTables(ImmutableMap.of("table1", tableConfig));

    ApplyOverridesFn fn = new ApplyOverridesFn(schemaConfig, 100, 10, 1);
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
  public void testProcessElement_SkipPrimaryKeyThrows() {
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

    SchemaConfig schemaConfig = new SchemaConfig();
    SchemaConfig.TableConfig tableConfig = new SchemaConfig.TableConfig();
    SchemaConfig.ColumnConfig colConfig = new SchemaConfig.ColumnConfig();
    colConfig.setSkip(true);
    tableConfig.setColumns(ImmutableMap.of("id", colConfig));
    schemaConfig.setTables(ImmutableMap.of("table1", tableConfig));

    ApplyOverridesFn fn = new ApplyOverridesFn(schemaConfig, 100, 10, 1);
    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);

    assertThrows(RuntimeException.class, () -> fn.processElement(schema, receiver));
  }

  @Test
  public void testProcessElement_WithForeignKeys() {
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

    SchemaConfig schemaConfig = new SchemaConfig();
    SchemaConfig.TableConfig tableConfig = new SchemaConfig.TableConfig();
    SchemaConfig.ForeignKeyConfig fkConfig = new SchemaConfig.ForeignKeyConfig();
    fkConfig.setName("fk1");
    fkConfig.setReferencedTable("table2");
    fkConfig.setKeyColumns(Arrays.asList("col1"));
    fkConfig.setReferencedColumns(Arrays.asList("col2"));
    tableConfig.setForeignKeys(Arrays.asList(fkConfig));
    schemaConfig.setTables(ImmutableMap.of("table1", tableConfig));

    ApplyOverridesFn fn = new ApplyOverridesFn(schemaConfig, 100, 10, 1);
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
}
