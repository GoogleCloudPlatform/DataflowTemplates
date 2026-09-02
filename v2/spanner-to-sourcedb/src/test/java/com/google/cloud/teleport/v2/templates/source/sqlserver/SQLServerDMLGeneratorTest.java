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
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.utils.SchemaUtils;
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
}
