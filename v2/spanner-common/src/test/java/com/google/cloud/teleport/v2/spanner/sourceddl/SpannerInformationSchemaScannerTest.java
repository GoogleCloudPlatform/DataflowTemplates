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
package com.google.cloud.teleport.v2.spanner.sourceddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerInformationSchemaScannerTest {

  private SpannerInformationSchemaScanner scanner;
  private SpannerConfig spannerConfig;

  @Before
  public void setUp() {
    spannerConfig =
        SpannerConfig.create()
            .withProjectId(StaticValueProvider.of("test-project"))
            .withInstanceId(StaticValueProvider.of("test-instance"))
            .withDatabaseId(StaticValueProvider.of("test-db"));
    scanner = new SpannerInformationSchemaScanner(spannerConfig);
  }

  @Test
  public void testConvertDdlToSourceSchema_simpleTable() {
    Ddl ddl =
        Ddl.builder()
            .createTable("Singers")
            .column("SingerId")
            .int64()
            .notNull()
            .endColumn()
            .column("FirstName")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("SingerId")
            .end()
            .endTable()
            .build();

    SourceSchema schema = scanner.convertDdlToSourceSchema(ddl);

    assertEquals("test-db", schema.databaseName());
    assertEquals(SourceDatabaseType.SPANNER, schema.sourceType());
    assertEquals(1, schema.tables().size());

    SourceTable table = schema.tables().get("Singers");
    assertEquals("Singers", table.name());
    assertEquals(2, table.columns().size());
    assertEquals(1, table.primaryKeyColumns().size());
    assertEquals("SingerId", table.primaryKeyColumns().get(0));

    SourceColumn idCol =
        table.columns().stream().filter(c -> c.name().equals("SingerId")).findFirst().get();
    assertEquals("INT64", idCol.type());
    assertFalse(idCol.isNullable());
    assertTrue(idCol.isPrimaryKey());

    SourceColumn nameCol =
        table.columns().stream().filter(c -> c.name().equals("FirstName")).findFirst().get();
    assertEquals("STRING", nameCol.type());
    assertTrue(nameCol.isNullable());
    assertFalse(nameCol.isPrimaryKey());
  }

  @Test
  public void testConvertTable_variousTypes() {
    Table spannerTable =
        Table.builder()
            .name("AllTypes")
            .column("bool_col")
            .bool()
            .endColumn()
            .column("int_col")
            .int64()
            .endColumn()
            .column("float_col")
            .float64()
            .endColumn()
            .column("str_col")
            .string()
            .max()
            .endColumn()
            .column("bytes_col")
            .bytes()
            .max()
            .endColumn()
            .column("date_col")
            .date()
            .endColumn()
            .column("ts_col")
            .timestamp()
            .endColumn()
            .column("num_col")
            .numeric()
            .endColumn()
            .column("json_col")
            .json()
            .endColumn()
            .column("arr_col")
            .type(Type.array(Type.int64()))
            .endColumn()
            .primaryKey()
            .asc("int_col")
            .end()
            .build();

    SourceTable sourceTable = scanner.convertTable(spannerTable);

    assertEquals("AllTypes", sourceTable.name());

    Map<String, SourceColumn> cols = new java.util.HashMap<>();
    sourceTable.columns().forEach(c -> cols.put(c.name(), c));

    assertEquals("BOOL", cols.get("bool_col").type());
    assertEquals("INT64", cols.get("int_col").type());
    assertEquals("FLOAT64", cols.get("float_col").type());
    assertEquals("STRING", cols.get("str_col").type());
    assertEquals("BYTES", cols.get("bytes_col").type());
    assertEquals("DATE", cols.get("date_col").type());
    assertEquals("TIMESTAMP", cols.get("ts_col").type());
    assertEquals("NUMERIC", cols.get("num_col").type());
    assertEquals("JSON", cols.get("json_col").type());
    assertEquals("ARRAY<INT64>", cols.get("arr_col").type());
  }

  @Test
  public void testConvertTable_generatedColumn() {
    Table spannerTable =
        Table.builder()
            .name("GenTable")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("gen_col")
            .int64()
            .generatedAs("id * 2")
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .build();

    SourceTable sourceTable = scanner.convertTable(spannerTable);

    SourceColumn genCol =
        sourceTable.columns().stream().filter(c -> c.name().equals("gen_col")).findFirst().get();
    assertTrue(genCol.isGenerated());

    SourceColumn idCol =
        sourceTable.columns().stream().filter(c -> c.name().equals("id")).findFirst().get();
    assertFalse(idCol.isGenerated());
  }

  @Test
  public void testSpannerTypeToString() {
    assertEquals("BOOL", SpannerInformationSchemaScanner.spannerTypeToString(Type.bool()));
    assertEquals("INT64", SpannerInformationSchemaScanner.spannerTypeToString(Type.int64()));
    assertEquals("FLOAT64", SpannerInformationSchemaScanner.spannerTypeToString(Type.float64()));
    assertEquals("STRING", SpannerInformationSchemaScanner.spannerTypeToString(Type.string()));
    assertEquals("BYTES", SpannerInformationSchemaScanner.spannerTypeToString(Type.bytes()));
    assertEquals("DATE", SpannerInformationSchemaScanner.spannerTypeToString(Type.date()));
    assertEquals(
        "TIMESTAMP", SpannerInformationSchemaScanner.spannerTypeToString(Type.timestamp()));
    assertEquals("NUMERIC", SpannerInformationSchemaScanner.spannerTypeToString(Type.numeric()));
    assertEquals("JSON", SpannerInformationSchemaScanner.spannerTypeToString(Type.json()));

    assertEquals(
        "ARRAY<INT64>",
        SpannerInformationSchemaScanner.spannerTypeToString(Type.array(Type.int64())));
    assertEquals(
        "ARRAY<STRING>",
        SpannerInformationSchemaScanner.spannerTypeToString(Type.array(Type.string())));
    assertEquals(
        "ARRAY<ARRAY<INT64>>",
        SpannerInformationSchemaScanner.spannerTypeToString(Type.array(Type.array(Type.int64()))));
  }
}
