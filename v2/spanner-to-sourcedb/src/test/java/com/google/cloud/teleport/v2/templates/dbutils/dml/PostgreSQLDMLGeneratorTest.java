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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.utils.SchemaUtils;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class PostgreSQLDMLGeneratorTest {

  @Test
  public void tableAndAllColumnNameTypesMatch() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";

    PostgreSQLDMLGenerator postgreSQLDMLGenerator = new PostgreSQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        postgreSQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("\"FirstName\" = EXCLUDED.\"FirstName\""));
    assertTrue(sql.contains("\"LastName\" = EXCLUDED.\"LastName\""));
    assertTrue(sql.contains("ON CONFLICT (\"SingerId\") DO UPDATE SET"));
  }

  @Test
  public void deleteMultiplePKColumns() {
    String sessionFile = "src/test/resources/MultiColmPKSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"LastName\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\",\"FirstName\":\"kk\"}");
    String modType = "DELETE";

    PostgreSQLDMLGenerator postgreSQLDMLGenerator = new PostgreSQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        postgreSQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("\"FirstName\" = 'kk'"));
    assertTrue(sql.contains("\"SingerId\" = 999"));
    assertTrue(sql.contains("DELETE FROM \"Singers\" WHERE"));
  }

  @Test
  public void primaryKeyNotFoundInJson() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SomeRandomName\":\"999\"}");
    String modType = "INSERT";

    PostgreSQLDMLGenerator postgreSQLDMLGenerator = new PostgreSQLDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            postgreSQLDMLGenerator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void primaryKeyNotPresentInSourceSchema() {
    String sessionFile = "src/test/resources/sourceNoPkSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";

    PostgreSQLDMLGenerator postgreSQLDMLGenerator = new PostgreSQLDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            postgreSQLDMLGenerator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }
}
