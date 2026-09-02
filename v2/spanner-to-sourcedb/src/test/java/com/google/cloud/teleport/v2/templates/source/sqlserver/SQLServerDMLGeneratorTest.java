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
package com.google.cloud.teleport.v2.templates.source.sqlserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.utils.SchemaUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class SQLServerDMLGeneratorTest {

  @Test
  public void testUpsertBasicTable() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"John\",\"LastName\":\"Doe\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";

    SQLServerDMLGenerator generator = new SQLServerDMLGenerator();
    DMLGeneratorResponse response =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());

    String sql = response.getDmlStatement();
    assertNotNull(sql);
    assertTrue(sql.contains("MERGE INTO [Singers] AS target"));
    assertTrue(sql.contains("ON (target.[SingerId] = 999)"));
    assertTrue(sql.contains("WHEN MATCHED THEN UPDATE SET"));
    assertTrue(sql.contains("target.[FirstName] = 'John'"));
    assertTrue(sql.contains("target.[LastName] = 'Doe'"));
    assertTrue(sql.contains("WHEN NOT MATCHED THEN INSERT ("));
    assertTrue(sql.contains("[SingerId]"));
    assertTrue(sql.contains("[FirstName]"));
    assertTrue(sql.contains("[LastName]"));
    assertTrue(sql.contains("VALUES ("));
  }

  @Test
  public void testDeleteBasicTable() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    JSONObject newValuesJson = new JSONObject("{}");
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "DELETE";

    SQLServerDMLGenerator generator = new SQLServerDMLGenerator();
    DMLGeneratorResponse response =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());

    String sql = response.getDmlStatement();
    assertEquals("DELETE FROM [Singers] WHERE  [SingerId] = 999", sql);
  }

  @Test
  public void testDeleteMultiplePKColumns() {
    String sessionFile = "src/test/resources/MultiColmPKSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    JSONObject newValuesJson = new JSONObject("{\"LastName\":null}");
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\",\"FirstName\":\"John\"}");
    String modType = "DELETE";

    SQLServerDMLGenerator generator = new SQLServerDMLGenerator();
    DMLGeneratorResponse response =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());

    String sql = response.getDmlStatement();
    assertTrue(sql.startsWith("DELETE FROM [Singers] WHERE "));
    assertTrue(sql.contains("[SingerId] = 999"));
    assertTrue(sql.contains("[FirstName] = 'John'"));
    assertTrue(sql.contains(" AND "));
  }

  @Test
  public void testBitDataType() {
    String valTrue = SQLServerDMLGenerator.getColumnValueByType("bit", "true", "+00:00", "BOOL");
    String valFalse = SQLServerDMLGenerator.getColumnValueByType("bit", "false", "+00:00", "BOOL");
    String valOne = SQLServerDMLGenerator.getColumnValueByType("bit", "1", "+00:00", "BOOL");

    assertEquals("1", valTrue);
    assertEquals("0", valFalse);
    assertEquals("1", valOne);
  }

  @Test
  public void testBinaryDataType() {
    String hex = SQLServerDMLGenerator.convertBase64ToHex("SGVsbG8=");
    assertEquals("0x48656c6c6f", hex);

    String emptyHex = SQLServerDMLGenerator.convertBase64ToHex("");
    assertEquals("0x", emptyHex);

    assertNull(SQLServerDMLGenerator.convertBase64ToHex(null));

    String binaryVal =
        SQLServerDMLGenerator.getColumnValueByType("varbinary", "0x48656c6c6f", "+00:00", "BYTES");
    assertEquals("0x48656c6c6f", binaryVal);
  }

  @Test
  public void testStringEscaping() {
    String escaped =
        SQLServerDMLGenerator.getColumnValueByType("nvarchar", "O'Connor\0", "+00:00", "STRING");
    assertEquals("'O''Connor'", escaped);
  }

  @Test
  public void testNullDmlGeneratorRequestThrowsException() {
    SQLServerDMLGenerator generator = new SQLServerDMLGenerator();
    assertThrows(InvalidDMLGenerationException.class, () -> generator.getDMLStatement(null));
  }

  @Test
  public void testMissingTableThrowsException() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    SQLServerDMLGenerator generator = new SQLServerDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            generator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "NonExistentTable",
                        new JSONObject("{}"),
                        new JSONObject("{}"),
                        "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void testUnsupportedModTypeThrowsException() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    SQLServerDMLGenerator generator = new SQLServerDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            generator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "DROP",
                        "Singers",
                        new JSONObject("{\"FirstName\":\"John\"}"),
                        new JSONObject("{\"SingerId\":\"999\"}"),
                        "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void testCustomTransformationApplied() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    JSONObject newValuesJson = new JSONObject("{\"FirstName\":\"John\",\"LastName\":\"Doe\"}");
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");

    Map<String, Object> customResponse = new HashMap<>();
    customResponse.put("FirstName", "'CustomJohn'");

    SQLServerDMLGenerator generator = new SQLServerDMLGenerator();
    DMLGeneratorResponse response =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    "UPDATE", tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setCustomTransformationResponse(customResponse)
                .build());

    String sql = response.getDmlStatement();
    assertTrue(sql.contains("target.[FirstName] = 'CustomJohn'"));
  }

  @Test
  public void testJsonDataType() {
    String jsonVal = "{\"key\":\"value\",\"num\":123}";

    // Test SQL Server json column with GSQL JSON and PG jsonb / string / varchar
    assertEquals(
        "'{\"key\":\"value\",\"num\":123}'",
        SQLServerDMLGenerator.getColumnValueByType("json", jsonVal, "+00:00", "JSON"));
    assertEquals(
        "'{\"key\":\"value\",\"num\":123}'",
        SQLServerDMLGenerator.getColumnValueByType("json", jsonVal, "+00:00", "PG_JSONB"));
    assertEquals(
        "'{\"key\":\"value\",\"num\":123}'",
        SQLServerDMLGenerator.getColumnValueByType("json", jsonVal, "+00:00", "STRING"));
    assertEquals(
        "'{\"key\":\"value\",\"num\":123}'",
        SQLServerDMLGenerator.getColumnValueByType("json", jsonVal, "+00:00", "PG_VARCHAR"));

    // Test SQL Server varchar column with JSON / PG_JSONB
    assertEquals(
        "'{\"key\":\"value\",\"num\":123}'",
        SQLServerDMLGenerator.getColumnValueByType("varchar", jsonVal, "+00:00", "JSON"));
    assertEquals(
        "'{\"key\":\"value\",\"num\":123}'",
        SQLServerDMLGenerator.getColumnValueByType("varchar", jsonVal, "+00:00", "PG_JSONB"));

    // Test getMappedColumnValue with JSON column
    Ddl ddl =
        Ddl.builder()
            .createTable("T")
            .column("json_col")
            .type(Type.json())
            .endColumn()
            .column("pg_jsonb_col")
            .type(Type.pgJsonb())
            .endColumn()
            .endTable()
            .build();
    Column gsqlJsonCol = ddl.table("T").column("json_col");
    SourceColumn sourceJsonCol =
        SourceColumn.builder(SourceDatabaseType.SQLSERVER).name("json_col").type("json").build();
    JSONObject valuesJson = new JSONObject();
    valuesJson.put("json_col", jsonVal);
    valuesJson.put("pg_jsonb_col", jsonVal);
    assertEquals(
        "'{\"key\":\"value\",\"num\":123}'",
        SQLServerDMLGenerator.getMappedColumnValue(
            gsqlJsonCol, sourceJsonCol, valuesJson, "+00:00", new ArrayList<>()));

    // Test getMappedColumnValue with PG_JSONB column
    Column pgJsonbCol = ddl.table("T").column("pg_jsonb_col");
    assertEquals(
        "'{\"key\":\"value\",\"num\":123}'",
        SQLServerDMLGenerator.getMappedColumnValue(
            pgJsonbCol, sourceJsonCol, valuesJson, "+00:00", new ArrayList<>()));
  }

  @Test
  public void testVectorDataType() {
    String vectorVal = "[1.5,2.5,3.5]";

    // Test SQL Server vector column with GSQL ARRAY and PG ARRAY
    assertEquals(
        "'[1.5,2.5,3.5]'",
        SQLServerDMLGenerator.getColumnValueByType(
            "vector", vectorVal, "+00:00", "ARRAY<FLOAT64>"));
    assertEquals(
        "'[1.5,2.5,3.5]'",
        SQLServerDMLGenerator.getColumnValueByType("vector", vectorVal, "+00:00", "PG_ARRAY"));

    // Test getMappedColumnValue with GSQL ARRAY column
    Ddl ddl =
        Ddl.builder()
            .createTable("T")
            .column("vec_col")
            .type(Type.array(Type.float64()))
            .endColumn()
            .column("pg_vec_col")
            .type(Type.pgArray(Type.pgFloat8()))
            .endColumn()
            .endTable()
            .build();
    Column gsqlArrayCol = ddl.table("T").column("vec_col");
    SourceColumn sourceVectorCol =
        SourceColumn.builder(SourceDatabaseType.SQLSERVER).name("vec_col").type("vector").build();
    JSONObject valuesJson =
        new JSONObject("{\"vec_col\":[1.5,2.5,3.5],\"pg_vec_col\":[1.5,2.5,3.5]}");
    assertEquals(
        "'[1.5,2.5,3.5]'",
        SQLServerDMLGenerator.getMappedColumnValue(
            gsqlArrayCol, sourceVectorCol, valuesJson, "+00:00", new ArrayList<>()));

    // Test getMappedColumnValue with PG ARRAY column
    Column pgArrayCol = ddl.table("T").column("pg_vec_col");
    assertEquals(
        "'[1.5,2.5,3.5]'",
        SQLServerDMLGenerator.getMappedColumnValue(
            pgArrayCol, sourceVectorCol, valuesJson, "+00:00", new ArrayList<>()));
  }

  @Test
  public void testGeneratedPkColumnUpsertAndDelete() {
    Ddl ddl =
        Ddl.builder()
            .createTable("generated_pk_column_table")
            .column("first_name_col")
            .type(Type.string())
            .endColumn()
            .column("last_name_col")
            .type(Type.string())
            .endColumn()
            .column("generated_column_col")
            .type(Type.string())
            .endColumn()
            .primaryKey()
            .asc("generated_column_col")
            .end()
            .endTable()
            .build();

    SourceTable sourceTable =
        SourceTable.builder(SourceDatabaseType.SQLSERVER)
            .name("generated_pk_column_table")
            .columns(
                ImmutableList.of(
                    SourceColumn.builder(SourceDatabaseType.SQLSERVER)
                        .name("first_name_col")
                        .type("varchar")
                        .isGenerated(false)
                        .build(),
                    SourceColumn.builder(SourceDatabaseType.SQLSERVER)
                        .name("last_name_col")
                        .type("varchar")
                        .isGenerated(false)
                        .build(),
                    SourceColumn.builder(SourceDatabaseType.SQLSERVER)
                        .name("generated_column_col")
                        .type("varchar")
                        .isGenerated(true)
                        .build()))
            .primaryKeyColumns(ImmutableList.of("generated_column_col"))
            .build();

    SourceSchema sourceSchema =
        SourceSchema.builder(SourceDatabaseType.SQLSERVER)
            .databaseName("test")
            .tables(ImmutableMap.of("generated_pk_column_table", sourceTable))
            .build();

    ISchemaMapper schemaMapper = new IdentityMapper(ddl);

    SQLServerDMLGenerator generator = new SQLServerDMLGenerator();

    // 1. Test UPDATE with generated PK column
    JSONObject updateNewValues =
        new JSONObject("{\"first_name_col\":\"a\",\"last_name_col\":\"c\"}");
    JSONObject updateKeyValues = new JSONObject("{\"generated_column_col\":\"a \"}");
    DMLGeneratorResponse upsertResponse =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    "UPDATE",
                    "generated_pk_column_table",
                    updateNewValues,
                    updateKeyValues,
                    "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());

    String upsertSql = upsertResponse.getDmlStatement();
    assertTrue(upsertSql.contains("WHEN MATCHED THEN UPDATE SET"));
    assertTrue(upsertSql.contains("target.[first_name_col] = 'a'"));
    assertTrue(upsertSql.contains("target.[last_name_col] = 'c'"));

    // 2. Test DELETE with null non-PK column value in generated PK table
    JSONObject deleteNewValues =
        new JSONObject("{\"first_name_col\":\"b\",\"last_name_col\":null}");
    JSONObject deleteKeyValues = new JSONObject("{\"generated_column_col\":\"b \"}");
    DMLGeneratorResponse deleteResponse =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    "DELETE",
                    "generated_pk_column_table",
                    deleteNewValues,
                    deleteKeyValues,
                    "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());

    String deleteSql = deleteResponse.getDmlStatement();
    assertEquals(
        "DELETE FROM [generated_pk_column_table] WHERE  [first_name_col] = 'b' AND  [last_name_col] IS NULL",
        deleteSql);
  }

  @Test
  public void testValidationAndErrorBranches() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);
    SQLServerDMLGenerator generator = new SQLServerDMLGenerator();

    // 1. Null schemaMapper
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            generator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "Singers", new JSONObject("{}"), new JSONObject("{}"), "+00:00")
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));

    // 2. Null spannerDdl
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            generator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "Singers", new JSONObject("{}"), new JSONObject("{}"), "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setSourceSchema(sourceSchema)
                    .build()));

    // 3. Null sourceSchema
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            generator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "Singers", new JSONObject("{}"), new JSONObject("{}"), "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .build()));

    // 4. SchemaMapper throws NoSuchElementException
    ISchemaMapper throwingMapper = org.mockito.Mockito.mock(ISchemaMapper.class);
    org.mockito.Mockito.when(throwingMapper.getSourceTableName("", "Singers"))
        .thenThrow(new java.util.NoSuchElementException("missing"));
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            generator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "Singers", new JSONObject("{}"), new JSONObject("{}"), "+00:00")
                    .setSchemaMapper(throwingMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));

    // 5. SourceTable not found in sourceSchema
    SourceSchema emptySourceSchema =
        SourceSchema.builder(SourceDatabaseType.SQLSERVER)
            .databaseName("test")
            .tables(ImmutableMap.of())
            .build();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            generator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "Singers", new JSONObject("{}"), new JSONObject("{}"), "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(emptySourceSchema)
                    .build()));

    // 6. SourceTable has no primary key columns
    SourceTable noPkTable =
        SourceTable.builder(SourceDatabaseType.SQLSERVER)
            .name("Singers")
            .columns(ImmutableList.of())
            .primaryKeyColumns(ImmutableList.of())
            .build();
    SourceSchema noPkSchema =
        SourceSchema.builder(SourceDatabaseType.SQLSERVER)
            .databaseName("test")
            .tables(ImmutableMap.of("Singers", noPkTable))
            .build();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            generator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "Singers", new JSONObject("{}"), new JSONObject("{}"), "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(noPkSchema)
                    .build()));

    // 7. Missing PK value in keyValuesJson / newValuesJson
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            generator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "Singers", new JSONObject("{}"), new JSONObject("{}"), "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void testGetColumnValueByTypeAllBranches() {
    // Datetimeoffset with timezone offset & TIMESTAMP
    assertEquals(
        "CAST(SWITCHOFFSET('2026-01-01T00:00:00Z', '+05:30') AS DATETIMEOFFSET)",
        SQLServerDMLGenerator.getColumnValueByType(
            "datetimeoffset", "2026-01-01T00:00:00Z", "+05:30", "TIMESTAMP"));
    assertEquals(
        "NULL",
        SQLServerDMLGenerator.getColumnValueByType("datetime2", null, "+05:30", "PG_TIMESTAMPTZ"));
    assertEquals(
        "'2026-01-01'",
        SQLServerDMLGenerator.getColumnValueByType("datetime", "2026-01-01", "", "STRING"));

    // Uniqueidentifier with BYTES / PG_BYTEA / STRING
    assertEquals(
        "CAST(0x1234 AS UNIQUEIDENTIFIER)",
        SQLServerDMLGenerator.getColumnValueByType(
            "uniqueidentifier", "0x1234", "+00:00", "BYTES"));
    assertEquals(
        "NULL",
        SQLServerDMLGenerator.getColumnValueByType("uniqueidentifier", null, "+00:00", "PG_BYTEA"));
    assertEquals(
        "'12345678-1234-1234-1234-123456789012'",
        SQLServerDMLGenerator.getColumnValueByType(
            "uniqueidentifier", "12345678-1234-1234-1234-123456789012", "+00:00", "STRING"));

    // Binary / varbinary / image
    assertEquals(
        "NULL", SQLServerDMLGenerator.getColumnValueByType("binary", null, "+00:00", "BYTES"));
    assertEquals(
        "0xABCD",
        SQLServerDMLGenerator.getColumnValueByType("varbinary", "ABCD", "+00:00", "BYTES"));
    assertEquals(
        "0XABCD", SQLServerDMLGenerator.getColumnValueByType("image", "0XABCD", "+00:00", "BYTES"));

    // String types with BYTES Spanner column
    assertEquals(
        "CAST(0x48656c6c6f AS VARCHAR(MAX))",
        SQLServerDMLGenerator.getColumnValueByType("varchar", "0x48656c6c6f", "+00:00", "BYTES"));
    assertEquals(
        "NULL", SQLServerDMLGenerator.getColumnValueByType("varchar", null, "+00:00", "PG_BYTEA"));
  }

  @Test
  public void testGetMappedColumnValueAllTypes() {
    Ddl ddl =
        Ddl.builder()
            .createTable("types_table")
            .column("f_col")
            .float64()
            .endColumn()
            .column("b_col")
            .bool()
            .endColumn()
            .column("by_col")
            .bytes()
            .endColumn()
            .column("arr_col")
            .type(Type.array(Type.string()))
            .endColumn()
            .primaryKey()
            .asc("f_col")
            .end()
            .endTable()
            .build();
    com.google.cloud.teleport.v2.spanner.ddl.Table table = ddl.table("types_table");

    Column floatCol = table.column("f_col");
    SourceColumn srcFloatCol =
        SourceColumn.builder(SourceDatabaseType.SQLSERVER).name("f_col").type("float").build();
    JSONObject json = new JSONObject("{\"f_col\":123.45}");
    assertEquals(
        "123.45",
        SQLServerDMLGenerator.getMappedColumnValue(
            floatCol, srcFloatCol, json, "+00:00", new ArrayList<>()));

    Column boolCol = table.column("b_col");
    SourceColumn srcBoolCol =
        SourceColumn.builder(SourceDatabaseType.SQLSERVER).name("b_col").type("bit").build();
    JSONObject boolJson = new JSONObject("{\"b_col\":true}");
    assertEquals(
        "1",
        SQLServerDMLGenerator.getMappedColumnValue(
            boolCol, srcBoolCol, boolJson, "+00:00", new ArrayList<>()));

    Column bytesCol = table.column("by_col");
    SourceColumn srcBytesCol =
        SourceColumn.builder(SourceDatabaseType.SQLSERVER).name("by_col").type("varbinary").build();
    JSONObject bytesJson = new JSONObject("{\"by_col\":\"SGVsbG8=\"}");
    assertEquals(
        "0x48656c6c6f",
        SQLServerDMLGenerator.getMappedColumnValue(
            bytesCol, srcBytesCol, bytesJson, "+00:00", new ArrayList<>()));

    Column arrCol = table.column("arr_col");
    SourceColumn srcArrCol =
        SourceColumn.builder(SourceDatabaseType.SQLSERVER).name("arr_col").type("varchar").build();
    JSONObject arrJson = new JSONObject("{\"arr_col\":[\"a\",\"b\"]}");
    assertEquals(
        "'[\"a\",\"b\"]'",
        SQLServerDMLGenerator.getMappedColumnValue(
            arrCol, srcArrCol, arrJson, "+00:00", new ArrayList<>()));

    JSONObject arrStrJson = new JSONObject("{\"arr_col\":\"raw_str\"}");
    assertEquals(
        "'raw_str'",
        SQLServerDMLGenerator.getMappedColumnValue(
            arrCol, srcArrCol, arrStrJson, "+00:00", new ArrayList<>()));
  }

  @Test
  public void testUpsertWithNullPkInOnCondition() {
    // Tests line 156: when onConditionColumnNameValues has a null value in UPDATE/INSERT
    Ddl ddl =
        Ddl.builder()
            .createTable("null_pk_table")
            .column("id_col")
            .int64()
            .endColumn()
            .column("name_col")
            .string()
            .endColumn()
            .primaryKey()
            .asc("id_col")
            .end()
            .endTable()
            .build();

    SourceTable sourceTable =
        SourceTable.builder(SourceDatabaseType.SQLSERVER)
            .name("null_pk_table")
            .columns(
                ImmutableList.of(
                    SourceColumn.builder(SourceDatabaseType.SQLSERVER)
                        .name("id_col")
                        .type("bigint")
                        .isPrimaryKey(true)
                        .build(),
                    SourceColumn.builder(SourceDatabaseType.SQLSERVER)
                        .name("name_col")
                        .type("varchar")
                        .isPrimaryKey(true)
                        .build()))
            .primaryKeyColumns(ImmutableList.of("id_col", "name_col"))
            .build();

    SourceSchema sourceSchema =
        SourceSchema.builder(SourceDatabaseType.SQLSERVER)
            .databaseName("test")
            .tables(ImmutableMap.of("null_pk_table", sourceTable))
            .build();

    ISchemaMapper schemaMapper = new IdentityMapper(ddl);
    SQLServerDMLGenerator generator = new SQLServerDMLGenerator();

    JSONObject updateNewValues = new JSONObject("{\"name_col\":null}");
    JSONObject updateKeyValues = new JSONObject("{\"id_col\":\"10\"}");

    DMLGeneratorResponse response =
        generator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    "UPDATE", "null_pk_table", updateNewValues, updateKeyValues, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());

    String sql = response.getDmlStatement();
    assertTrue(sql.contains("target.[name_col] IS NULL"));

    // Tests line 215: when insertColumns is empty -> WHEN NOT MATCHED THEN INSERT DEFAULT VALUES;
    DMLGeneratorResponse defaultInsertResp =
        SQLServerDMLGenerator.getUpsertStatement(
            "null_pk_table", Map.of(), Map.of("id_col", "10"), java.util.Set.of("id_col"));
    assertTrue(
        defaultInsertResp
            .getDmlStatement()
            .contains("WHEN NOT MATCHED THEN INSERT DEFAULT VALUES;"));
  }
}
