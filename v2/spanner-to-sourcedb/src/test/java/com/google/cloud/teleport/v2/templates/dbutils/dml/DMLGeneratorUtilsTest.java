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
package com.google.cloud.teleport.v2.templates.dbutils.dml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.common.collect.ImmutableList;
import java.util.Map;
import java.util.NoSuchElementException;
import org.json.JSONObject;
import org.junit.Test;

public class DMLGeneratorUtilsTest {

  @Test
  public void testConvertBase64ToRawHex_Null() {
    assertNull(DMLGeneratorUtils.convertBase64ToRawHex(null));
  }

  @Test
  public void testConvertBase64ToRawHex_Empty() {
    assertEquals("", DMLGeneratorUtils.convertBase64ToRawHex(""));
  }

  @Test
  public void testConvertBase64ToRawHex_Invalid() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DMLGeneratorUtils.convertBase64ToRawHex("invalid base64!"));
  }

  @Test
  public void testConvertBase64ToRawHex_Valid() {
    // "Hello" in base64 is "SGVsbG8="
    // "Hello" in hex is "48656c6c6f"
    assertEquals("48656c6c6f", DMLGeneratorUtils.convertBase64ToRawHex("SGVsbG8="));
  }

  @Test
  public void testGetColumnValues() {
    ISchemaMapper schemaMapper = mock(ISchemaMapper.class);
    Table spannerTable = mock(Table.class);
    SourceTable sourceTable = mock(SourceTable.class);
    SourceColumn sourceCol = mock(SourceColumn.class);
    Column spannerCol = mock(Column.class);

    when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of());
    when(sourceTable.columns()).thenReturn(ImmutableList.of(sourceCol));
    when(sourceCol.name()).thenReturn("col1");
    when(sourceCol.isGenerated()).thenReturn(false);

    when(schemaMapper.getSpannerColumnName("", "src_table", "col1")).thenReturn("spanner_col1");
    when(sourceTable.name()).thenReturn("src_table");
    when(spannerTable.column("spanner_col1")).thenReturn(spannerCol);
    when(spannerCol.name()).thenReturn("spanner_col1");

    JSONObject newValuesJson = new JSONObject();
    newValuesJson.put("spanner_col1", "val1");

    DMLGeneratorUtils.ColumnValueMapper mapper =
        (spannerColDef, sourceColDef, valuesJson, timezoneOffset) -> {
          assertEquals(spannerCol, spannerColDef);
          assertEquals(sourceCol, sourceColDef);
          assertEquals(newValuesJson, valuesJson);
          assertEquals("+00:00", timezoneOffset);
          return "mapped_val";
        };

    Map<String, String> response =
        DMLGeneratorUtils.getColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            newValuesJson,
            new JSONObject(),
            "+00:00",
            null,
            mapper);

    assertEquals(1, response.size());
    assertEquals("mapped_val", response.get("col1"));
  }

  @Test
  public void testGetPkColumnValues() {
    ISchemaMapper schemaMapper = mock(ISchemaMapper.class);
    Table spannerTable = mock(Table.class);
    SourceTable sourceTable = mock(SourceTable.class);
    SourceColumn sourceCol = mock(SourceColumn.class);
    Column spannerCol = mock(Column.class);

    when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of("col1"));
    when(sourceTable.column("col1")).thenReturn(sourceCol);
    when(sourceCol.name()).thenReturn("col1");
    when(sourceCol.isGenerated()).thenReturn(false);

    when(schemaMapper.getSpannerColumnName("", "src_table", "col1")).thenReturn("spanner_col1");
    when(sourceTable.name()).thenReturn("src_table");
    when(spannerTable.column("spanner_col1")).thenReturn(spannerCol);
    when(spannerCol.name()).thenReturn("spanner_col1");

    JSONObject keyValuesJson = new JSONObject();
    keyValuesJson.put("spanner_col1", "val1");

    DMLGeneratorUtils.ColumnValueMapper mapper =
        (spannerColDef, sourceColDef, valuesJson, timezoneOffset) -> {
          assertEquals(spannerCol, spannerColDef);
          assertEquals(sourceCol, sourceColDef);
          assertEquals(keyValuesJson, valuesJson);
          assertEquals("+00:00", timezoneOffset);
          return "mapped_val";
        };

    Map<String, String> response =
        DMLGeneratorUtils.getPkColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            new JSONObject(),
            keyValuesJson,
            "+00:00",
            null,
            mapper);

    assertEquals(1, response.size());
    assertEquals("mapped_val", response.get("col1"));
  }

  @Test
  public void testGetPkColumnValues_SourceColDefNull() {
    ISchemaMapper schemaMapper = mock(ISchemaMapper.class);
    Table spannerTable = mock(Table.class);
    SourceTable sourceTable = mock(SourceTable.class);

    when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of("col1"));
    when(sourceTable.column("col1")).thenReturn(null);

    Map<String, String> response =
        DMLGeneratorUtils.getPkColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            new JSONObject(),
            new JSONObject(),
            "+00:00",
            null,
            null);

    assertNull(response);
  }

  @Test
  public void testGetPkColumnValues_SpannerColNameThrowsNoSuchElement() {
    ISchemaMapper schemaMapper = mock(ISchemaMapper.class);
    Table spannerTable = mock(Table.class);
    SourceTable sourceTable = mock(SourceTable.class);
    SourceColumn sourceCol = mock(SourceColumn.class);

    when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of("col1"));
    when(sourceTable.column("col1")).thenReturn(sourceCol);
    when(sourceCol.isGenerated()).thenReturn(false);
    when(sourceTable.name()).thenReturn("src_table");

    when(schemaMapper.getSpannerColumnName("", "src_table", "col1"))
        .thenThrow(new NoSuchElementException("Not found"));

    Map<String, String> response =
        DMLGeneratorUtils.getPkColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            new JSONObject(),
            new JSONObject(),
            "+00:00",
            null,
            null);

    assertTrue(response.isEmpty());
  }

  @Test
  public void testGetPkColumnValues_SpannerColNameNull() {
    ISchemaMapper schemaMapper = mock(ISchemaMapper.class);
    Table spannerTable = mock(Table.class);
    SourceTable sourceTable = mock(SourceTable.class);
    SourceColumn sourceCol = mock(SourceColumn.class);

    when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of("col1"));
    when(sourceTable.column("col1")).thenReturn(sourceCol);
    when(sourceCol.isGenerated()).thenReturn(false);
    when(sourceTable.name()).thenReturn("src_table");

    when(schemaMapper.getSpannerColumnName("", "src_table", "col1")).thenReturn(null);

    Map<String, String> response =
        DMLGeneratorUtils.getPkColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            new JSONObject(),
            new JSONObject(),
            "+00:00",
            null,
            null);

    assertTrue(response.isEmpty());
  }

  @Test
  public void testGetPkColumnValues_SpannerColDefNull() {
    ISchemaMapper schemaMapper = mock(ISchemaMapper.class);
    Table spannerTable = mock(Table.class);
    SourceTable sourceTable = mock(SourceTable.class);
    SourceColumn sourceCol = mock(SourceColumn.class);

    when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of("col1"));
    when(sourceTable.column("col1")).thenReturn(sourceCol);
    when(sourceCol.isGenerated()).thenReturn(false);
    when(sourceTable.name()).thenReturn("src_table");

    when(schemaMapper.getSpannerColumnName("", "src_table", "col1")).thenReturn("spanner_col1");
    when(spannerTable.column("spanner_col1")).thenReturn(null);

    Map<String, String> response =
        DMLGeneratorUtils.getPkColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            new JSONObject(),
            new JSONObject(),
            "+00:00",
            null,
            null);

    assertTrue(response.isEmpty());
  }

  @Test
  public void testGetPkColumnValues_CompositePk_OnePkFailsToMap() {
    ISchemaMapper schemaMapper = mock(ISchemaMapper.class);
    Table spannerTable = mock(Table.class);
    SourceTable sourceTable = mock(SourceTable.class);
    SourceColumn sourceCol1 = mock(SourceColumn.class);
    SourceColumn sourceCol2 = mock(SourceColumn.class);
    Column spannerCol1 = mock(Column.class);

    when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of("col1", "col2"));
    when(sourceTable.column("col1")).thenReturn(sourceCol1);
    when(sourceCol1.name()).thenReturn("col1");
    when(sourceCol1.isGenerated()).thenReturn(false);
    when(sourceTable.column("col2")).thenReturn(sourceCol2);
    when(sourceCol2.name()).thenReturn("col2");
    when(sourceCol2.isGenerated()).thenReturn(false);
    when(sourceTable.name()).thenReturn("src_table");

    // col1 maps successfully
    when(schemaMapper.getSpannerColumnName("", "src_table", "col1")).thenReturn("spanner_col1");
    when(spannerTable.column("spanner_col1")).thenReturn(spannerCol1);
    when(spannerCol1.name()).thenReturn("spanner_col1");

    // col2 fails to map (returns null)
    when(schemaMapper.getSpannerColumnName("", "src_table", "col2")).thenReturn(null);

    JSONObject keyValuesJson = new JSONObject();
    keyValuesJson.put("spanner_col1", "val1");

    DMLGeneratorUtils.ColumnValueMapper mapper =
        (spannerColDef, sourceColDef, valuesJson, timezoneOffset) -> {
          assertEquals(spannerCol1, spannerColDef);
          assertEquals(sourceCol1, sourceColDef);
          assertEquals(keyValuesJson, valuesJson);
          assertEquals("+00:00", timezoneOffset);
          return "mapped_val1";
        };

    Map<String, String> response =
        DMLGeneratorUtils.getPkColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            new JSONObject(),
            keyValuesJson,
            "+00:00",
            null,
            mapper);

    assertEquals(1, response.size());
    assertEquals("mapped_val1", response.get("col1"));
    assertFalse(response.containsKey("col2"));
  }
}
