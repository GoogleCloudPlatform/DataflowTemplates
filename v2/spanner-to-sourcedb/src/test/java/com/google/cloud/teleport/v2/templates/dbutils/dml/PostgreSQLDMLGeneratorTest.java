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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.utils.SchemaUtils;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.io.IOUtils;
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

  @Test
  public void allDatatypesDML() throws Exception {
    String sessionFile = "src/test/resources/pgAllDatatypesSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    InputStream stream =
        Channels.newInputStream(
            FileSystems.open(
                FileSystems.matchNewResource(
                    "src/test/resources/bufferInputAllDatatypes.json", false)));
    String record = IOUtils.toString(stream, StandardCharsets.UTF_8);

    ObjectWriter ow = new ObjectMapper().writer();
    TrimmedShardedDataChangeRecord chrec =
        new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.IDENTITY)
            .create()
            .fromJson(record, TrimmedShardedDataChangeRecord.class);

    String tableName = chrec.getTableName();
    String modType = chrec.getModType().name();
    String keysJsonStr = chrec.getMod().getKeysJson();
    String newValueJsonStr = chrec.getMod().getNewValuesJson();
    JSONObject newValuesJson = new JSONObject(newValueJsonStr);
    JSONObject keyValuesJson = new JSONObject(keysJsonStr);

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
    System.out.println("SQL: " + sql);

    assertTrue(sql.contains("333"));
    assertTrue(sql.contains("'\\x616263'"));
    assertTrue(sql.contains("'2023-05-18T12:01:13.088397258Z'::timestamptz"));
    assertTrue(sql.contains("'1'"));
    assertTrue(sql.contains("'<longtext_column>'"));
    assertTrue(sql.contains("'\\x6162636c61726765'"));
    assertTrue(sql.contains("'aaaaaddd'"));
    assertTrue(sql.contains("1"));
    assertTrue(sql.contains("4.2"));
    assertTrue(sql.contains("4444"));
    assertTrue(sql.contains("'10:10:10'"));
    assertTrue(sql.contains("'<tinytext_column>'"));
    assertTrue(sql.contains("'1,2'"));
    assertTrue(sql.contains("'\\x61626c6f6e67626c6f6263'"));
    assertTrue(sql.contains("'<mediumtext_column>'"));
    assertTrue(sql.contains("2023"));
    assertTrue(sql.contains("'\\x616262696763'"));
    assertTrue(sql.contains("444.222"));
    assertTrue(sql.contains("false"));
    assertTrue(sql.contains("'<char_c'"));
    assertTrue(sql.contains("'2023-05-18'"));
    assertTrue(sql.contains("42.42"));
    assertTrue(sql.contains("22"));
    assertTrue(sql.contains("\"mediumint_column\" = EXCLUDED.\"mediumint_column\""));
    assertTrue(sql.contains("\"tinyblob_column\" = EXCLUDED.\"tinyblob_column\""));
    assertTrue(sql.contains("\"datetime_column\" = EXCLUDED.\"datetime_column\""));
  }

  @Test
  public void pgDialectAllDatatypesDML() throws Exception {
    String sessionFile = "src/test/resources/pgDialectPostgresSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    InputStream stream =
        Channels.newInputStream(
            FileSystems.open(
                FileSystems.matchNewResource(
                    "src/test/resources/bufferInputAllDatatypes.json", false)));
    String record = IOUtils.toString(stream, StandardCharsets.UTF_8);

    TrimmedShardedDataChangeRecord chrec =
        new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.IDENTITY)
            .create()
            .fromJson(record, TrimmedShardedDataChangeRecord.class);

    String tableName = chrec.getTableName();
    String modType = chrec.getModType().name();
    String keysJsonStr = chrec.getMod().getKeysJson();
    String newValueJsonStr = chrec.getMod().getNewValuesJson();
    JSONObject newValuesJson = new JSONObject(newValueJsonStr);
    JSONObject keyValuesJson = new JSONObject(keysJsonStr);

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
    System.out.println("PG Dialect SQL: " + sql);

    assertTrue(sql.contains("333"));
    assertTrue(sql.contains("'\\x616263'"));
    assertTrue(sql.contains("'2023-05-18T12:01:13.088397258Z'::timestamptz"));
    assertTrue(sql.contains("'1'"));
    assertTrue(sql.contains("'<longtext_column>'"));
    assertTrue(sql.contains("'\\x6162636c61726765'"));
    assertTrue(sql.contains("'aaaaaddd'"));
    assertTrue(sql.contains("1"));
    assertTrue(sql.contains("4.2"));
    assertTrue(sql.contains("4444"));
    assertTrue(sql.contains("'10:10:10'"));
    assertTrue(sql.contains("'<tinytext_column>'"));
    assertTrue(sql.contains("'1,2'"));
    assertTrue(sql.contains("'\\x61626c6f6e67626c6f6263'"));
    assertTrue(sql.contains("'<mediumtext_column>'"));
    assertTrue(sql.contains("2023"));
    assertTrue(sql.contains("'\\x616262696763'"));
    assertTrue(sql.contains("444.222"));
    assertTrue(sql.contains("false"));
    assertTrue(sql.contains("'<char_c'"));
    assertTrue(sql.contains("'2023-05-18'"));
    assertTrue(sql.contains("42.42"));
    assertTrue(sql.contains("22"));
    assertTrue(sql.contains("\"mediumint_column\" = EXCLUDED.\"mediumint_column\""));
    assertTrue(sql.contains("\"tinyblob_column\" = EXCLUDED.\"tinyblob_column\""));
    assertTrue(sql.contains("\"datetime_column\" = EXCLUDED.\"datetime_column\""));
  }
}
