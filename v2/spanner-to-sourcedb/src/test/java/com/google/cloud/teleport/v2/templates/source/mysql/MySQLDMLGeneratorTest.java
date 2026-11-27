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
package com.google.cloud.teleport.v2.templates.source.mysql;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.ColumnBinding;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementValue;
import com.google.cloud.teleport.v2.templates.utils.SchemaUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public final class MySQLDMLGeneratorTest {

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

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("FirstName")).isEqualTo("kk");
    assertThat(bound.value("LastName")).isEqualTo("ll");
  }

  @Test
  public void tableNameMismatchAllColumnNameTypesMatch() {
    String sessionFile = "src/test/resources/tableNameMismatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "leChanteur";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("FirstName")).isEqualTo("kk");
    assertThat(bound.value("LastName")).isEqualTo("ll");
  }

  @Test
  public void tableNameMatchColumnNameTypeMismatch() {
    String sessionFile = "src/test/resources/coulmnNameTypeMismatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"222\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";
    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("FirstName")).isEqualTo("222");
    assertThat(bound.value("LastName")).isEqualTo("ll");
  }

  @Test
  public void tableNameMatchSourceColumnNotPresentInSpanner() {
    String sessionFile = "src/test/resources/sourceColumnAbsentInSpannerSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("FirstName")).isEqualTo("kk");
    assertThat(bound.value("LastName")).isEqualTo("ll");
  }

  @Test
  public void tableNameMatchSpannerColumnNotPresentInSource() {
    String sessionFile = "src/test/resources/spannerColumnAbsentInSourceSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\",\"hb_shardId\":\"shardA\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("FirstName")).isEqualTo("kk");
    assertThat(bound.value("LastName")).isEqualTo("ll");
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

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            mySQLDMLGenerator.getDMLStatement(
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

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            mySQLDMLGenerator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void tableOnlyContainsPrimaryKeyColumns() {
    String sessionFile = "src/test/resources/onlyPKColumnsSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "resource_access";
    String newValuesString = "{\"user_id\":\"101\",\"group_id\":\"5\",\"resource_id\":\"99\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    // the keys and the newValues are the same because all the columns are part of the key
    JSONObject keyValuesJson = new JSONObject(newValuesString);
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);
    assertThat(sql.contains("ON DUPLICATE KEY UPDATE"));
    assertThat(bound.value("user_id")).isEqualTo("101");
    assertThat(bound.value("group_id")).isEqualTo("5");
    assertThat(bound.value("resource_id")).isEqualTo("99");
    assertEquals(2, countInSQL(sql, "user_id"));
    assertEquals(2, countInSQL(sql, "group_id"));
    assertEquals(2, countInSQL(sql, "resource_id"));
  }

  @Test
  public void timezoneOffsetMismatch() {
    String sessionFile = "src/test/resources/timeZoneSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"Bday\":\"2023-05-18T12:01:13.088397258Z\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+10:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.placeholder("Bday")).isEqualTo("CONVERT_TZ(?, '+00:00', ?)");
    assertThat(bound.values("Bday"))
        .containsExactly("2023-05-18T12:01:13.088397258", "+10:00")
        .inOrder();
  }

  @Test
  public void primaryKeyMismatch() {
    String sessionFile = "src/test/resources/primarykeyMismatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"SingerId\":\"999\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"FirstName\":\"kk\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("FirstName")).isEqualTo("kk");
    assertThat(bound.value("LastName")).isEqualTo("ll");
  }

  @Test
  public void allDataypesDML() throws Exception {
    String sessionFile = "src/test/resources/allDatatypeSession.json";
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

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("mediumint_column")).isEqualTo("333");
    assertThat(bound.bytes("tinyblob_column")).isEqualTo(base64("YWJj"));
    assertThat(bound.placeholder("datetime_column")).isEqualTo("CONVERT_TZ(?, '+00:00', ?)");
    assertThat(bound.values("datetime_column"))
        .containsExactly("2023-05-18T12:01:13.088397258", "+00:00")
        .inOrder();
    assertThat(bound.value("enum_column")).isEqualTo("1");
    assertThat(bound.value("longtext_column")).isEqualTo("<longtext_column>");
    assertThat(bound.bytes("mediumblob_column")).isEqualTo(base64("YWJjbGFyZ2U="));
    assertThat(bound.value("text_column")).isEqualTo("aaaaaddd");
    assertThat(bound.value("tinyint_column")).isEqualTo("1");
    assertThat(bound.placeholder("timestamp_column")).isEqualTo("CONVERT_TZ(?, '+00:00', ?)");
    assertThat(bound.values("timestamp_column"))
        .containsExactly("2023-05-18T12:01:13.088397258", "+00:00")
        .inOrder();
    assertThat(bound.decimal("float_column")).isEqualToIgnoringScale("4.2");
    assertThat(bound.placeholder("varbinary_column")).isEqualTo("?");
    assertThat(bound.bytes("varbinary_column")).isEqualTo(base64("YWJjbGFyZ2U="));
    assertThat(bound.placeholder("binary_column")).isEqualTo("?");
    assertThat(bound.bytes("binary_column")).isEqualTo(base64("YWJjbGFyZ2U="));
    assertThat(bound.value("bigint_column")).isEqualTo("4444");
    assertThat(bound.value("time_column")).isEqualTo("10:10:10");
    assertThat(bound.value("tinytext_column")).isEqualTo("<tinytext_column>");
    assertThat(bound.value("set_column")).isEqualTo("1,2");
    assertThat(bound.bytes("longblob_column")).isEqualTo(base64("YWJsb25nYmxvYmM="));
    assertThat(bound.value("mediumtext_column")).isEqualTo("<mediumtext_column>");
    assertThat(bound.value("year_column")).isEqualTo("2023");
    assertThat(bound.bytes("blob_column")).isEqualTo(base64("YWJiaWdj"));
    // Spanner NUMERIC values are strings in the change stream.
    assertThat(bound.value("decimal_column")).isEqualTo("444.222");
    assertThat(bound.value("bool_column")).isEqualTo(false);
    assertThat(bound.value("char_column")).isEqualTo("<char_c");
    assertThat(bound.value("date_column")).isEqualTo("2023-05-18");
    assertThat(bound.decimal("double_column")).isEqualToIgnoringScale("42.42");
    assertThat(bound.value("smallint_column")).isEqualTo("22");
    assertThat(bound.value("varchar_column")).isEqualTo("abc");
  }

  @Test
  public void pgDialectAllDatatypesDML() throws Exception {
    String sessionFile = "src/test/resources/pgDialectMySQLSession.json";
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

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("mediumint_column")).isEqualTo("333");
    assertThat(bound.bytes("tinyblob_column")).isEqualTo(base64("YWJj"));
    assertThat(bound.placeholder("datetime_column")).isEqualTo("CONVERT_TZ(?, '+00:00', ?)");
    assertThat(bound.values("datetime_column"))
        .containsExactly("2023-05-18T12:01:13.088397258", "+00:00")
        .inOrder();
    assertThat(bound.value("enum_column")).isEqualTo("1");
    assertThat(bound.value("longtext_column")).isEqualTo("<longtext_column>");
    assertThat(bound.bytes("mediumblob_column")).isEqualTo(base64("YWJjbGFyZ2U="));
    assertThat(bound.value("text_column")).isEqualTo("aaaaaddd");
    assertThat(bound.value("tinyint_column")).isEqualTo("1");
    assertThat(bound.placeholder("timestamp_column")).isEqualTo("CONVERT_TZ(?, '+00:00', ?)");
    assertThat(bound.values("timestamp_column"))
        .containsExactly("2023-05-18T12:01:13.088397258", "+00:00")
        .inOrder();
    assertThat(bound.decimal("float_column")).isEqualToIgnoringScale("4.2");
    assertThat(bound.placeholder("varbinary_column")).isEqualTo("?");
    assertThat(bound.bytes("varbinary_column")).isEqualTo(base64("YWJjbGFyZ2U="));
    assertThat(bound.placeholder("binary_column")).isEqualTo("?");
    assertThat(bound.bytes("binary_column")).isEqualTo(base64("YWJjbGFyZ2U="));
    assertThat(bound.value("bigint_column")).isEqualTo("4444");
    assertThat(bound.value("time_column")).isEqualTo("10:10:10");
    assertThat(bound.value("tinytext_column")).isEqualTo("<tinytext_column>");
    assertThat(bound.value("set_column")).isEqualTo("1,2");
    assertThat(bound.bytes("longblob_column")).isEqualTo(base64("YWJsb25nYmxvYmM="));
    assertThat(bound.value("mediumtext_column")).isEqualTo("<mediumtext_column>");
    assertThat(bound.value("year_column")).isEqualTo("2023");
    assertThat(bound.bytes("blob_column")).isEqualTo(base64("YWJiaWdj"));
    assertThat(bound.decimal("decimal_column")).isEqualToIgnoringScale("444.222");
    assertThat(bound.value("bool_column")).isEqualTo(false);
    assertThat(bound.value("char_column")).isEqualTo("<char_c");
    assertThat(bound.value("date_column")).isEqualTo("2023-05-18");
    assertThat(bound.decimal("double_column")).isEqualToIgnoringScale("42.42");
    assertThat(bound.value("smallint_column")).isEqualTo("22");
    assertThat(bound.value("varchar_column")).isEqualTo("abc");
  }

  @Test
  public void updateToNull() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("FirstName")).isEqualTo("kk");
    assertThat(bound.value("LastName")).isNull();
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

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("FirstName")).isEqualTo("kk");
    assertThat(bound.value("SingerId")).isEqualTo("999");
    assertTrue(sql.contains("DELETE FROM `Singers` WHERE"));
  }

  @Test
  public void testSingleQuoteMatch() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"k\u0027k\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("FirstName")).isEqualTo("k'k");
    assertThat(bound.value("LastName")).isEqualTo("ll");
  }

  @Test
  public void singleQuoteBytesDML() throws Exception {
    String sessionFile = "src/test/resources/quotesSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    /*
    Spanner write is : CAST("\'" as BYTES) for blob and "\'" for varchar
    The value is bound as a PreparedStatement parameter without any escaping.*/

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Jw\u003d\u003d\",\"varchar_column\":\"\u0027\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);
    assertThat(bound.value("varchar_column")).isEqualTo("'");
    assertThat(bound.bytes("blob_column")).isEqualTo(base64("Jw=="));
  }

  @Test
  public void twoSingleEscapedQuoteDML() throws Exception {
    /*
    Spanner write is : CAST("\''" as BYTES) for blob and "\'" for varchar
    The value is bound as a PreparedStatement parameter without any escaping.*/

    String sessionFile = "src/test/resources/quotesSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Jyc\u003d\",\"varchar_column\":\"\u0027\u0027\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("varchar_column")).isEqualTo("''");
    assertThat(bound.bytes("blob_column")).isEqualTo(base64("Jyc="));
  }

  @Test
  public void threeEscapesAndSingleQuoteDML() throws Exception {
    /*
    Spanner write is : CAST("\\\'" as BYTES) for blob and "\\\'" for varchar
    Eventual insert is \' The value is bound as a PreparedStatement parameter without any escaping.*/

    String sessionFile = "src/test/resources/quotesSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"XCc\u003d\",\"varchar_column\":\"\\\\\\\u0027\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("varchar_column")).isEqualTo("\\'");
    assertThat(bound.bytes("blob_column")).isEqualTo(base64("XCc="));
  }

  @Test
  public void tabEscapeDML() throws Exception {
    /*
    Spanner write is : CAST("\t" as BYTES) for blob
    and "\t" for varchar
    */

    String sessionFile = "src/test/resources/quotesSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"CQ==\",\"varchar_column\":\"\\t\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("varchar_column")).isEqualTo("\t");
    assertThat(bound.bytes("blob_column")).isEqualTo(base64("CQ=="));
  }

  @Test
  public void backSpaceEscapeDML() throws Exception {
    /*
    Spanner write is : CAST("\b" as BYTES) for blob
    and "\b" for varchar
    */

    String sessionFile = "src/test/resources/quotesSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"CA==\",\"varchar_column\":\"\\b\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("varchar_column")).isEqualTo("\b");
    assertThat(bound.bytes("blob_column")).isEqualTo(base64("CA=="));
  }

  @Test
  public void newLineEscapeDML() throws Exception {
    /*
    Spanner write is : CAST("\n" as BYTES) for blob
    and "\n" for varchar
    */

    String sessionFile = "src/test/resources/quotesSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Cg==\",\"varchar_column\":\"\\n\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("varchar_column")).isEqualTo("\n");
    assertThat(bound.bytes("blob_column")).isEqualTo(base64("Cg=="));
  }

  @Test
  public void carriageReturnEscapeDML() throws Exception {
    /*
    Spanner write is : CAST("\r" as BYTES) for blob
    and "\r" for varchar
    */

    String sessionFile = "src/test/resources/quotesSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"DQ==\",\"varchar_column\":\"\\r\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("varchar_column")).isEqualTo("\r");
    assertThat(bound.bytes("blob_column")).isEqualTo(base64("DQ=="));
  }

  @Test
  public void formFeedEscapeDML() throws Exception {
    /*
    Spanner write is : CAST("\f" as BYTES) for blob
    and "\f" for varchar
    */

    String sessionFile = "src/test/resources/quotesSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"DA==\",\"varchar_column\":\"\\f\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("varchar_column")).isEqualTo("\f");
    assertThat(bound.bytes("blob_column")).isEqualTo(base64("DA=="));
  }

  @Test
  public void doubleQuoteEscapeDML() throws Exception {
    /*
    Spanner write is : CAST("\"" as BYTES) for blob
    and "\"" for varchar
    */

    String sessionFile = "src/test/resources/quotesSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Ig==\",\"varchar_column\":\"\\\"\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("varchar_column")).isEqualTo("\"");
    assertThat(bound.bytes("blob_column")).isEqualTo(base64("Ig=="));
  }

  @Test
  public void backSlashEscapeDML() throws Exception {
    /*
    Spanner write is : CAST("\\" as BYTES) for blob
    and "\\" for varchar
    */

    String sessionFile = "src/test/resources/quotesSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"XA==\",\"varchar_column\":\"\\\\\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("varchar_column")).isEqualTo("\\");
    assertThat(bound.bytes("blob_column")).isEqualTo(base64("XA=="));
  }

  @Test
  public void bitColumnSql() {
    String sessionFile = "src/test/resources/bitSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"YmlsX2NvbA\u003d\u003d\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.placeholder("LastName")).isEqualTo("?");
    assertThat(bound.bytes("LastName")).isEqualTo("bil_col".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testSpannerTableNotInSchema() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "SomeRandomTableNotInSchema";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            mySQLDMLGenerator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void testSpannerKeyIsNull() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":null}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);
    assertThat(sql.contains("ON DUPLICATE KEY UPDATE"));
    assertThat(bound.value("FirstName")).isEqualTo("kk");
    assertThat(bound.value("SingerId")).isNull();
    assertThat(bound.value("LastName")).isEqualTo("ll");
    assertEquals(2, countInSQL(sql, "FirstName"));
    assertEquals(2, countInSQL(sql, "SingerId"));
    assertEquals(2, countInSQL(sql, "LastName"));
  }

  @Test
  public void testKeyInNewValuesJson() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\",\"SingerId\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SmthingElse\":null}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);
    assertThat(sql.contains("ON DUPLICATE KEY UPDATE"));
    assertThat(bound.value("FirstName")).isEqualTo("kk");
    assertThat(bound.value("SingerId")).isNull();
    assertThat(bound.value("LastName")).isEqualTo("ll");
    assertEquals(2, countInSQL(sql, "FirstName"));
    assertEquals(2, countInSQL(sql, "SingerId"));
    assertEquals(2, countInSQL(sql, "LastName"));
  }

  @Test
  public void testSourcePKNotInSpanner() {
    String sessionFile = "src/test/resources/errorSchemaSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "customer";
    String newValuesString = "{\"Does\":\"not\",\"matter\":\"junk\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"Dont\":\"care\"}");
    String modType = "DELETE";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            mySQLDMLGenerator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void primaryKeyMismatchSpannerNull() {
    String sessionFile = "src/test/resources/primarykeyMismatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"SingerId\":\"999\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"FirstName\":null}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("FirstName")).isNull();
  }

  @Test
  public void testUnsupportedModType() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "JUNK";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            mySQLDMLGenerator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void testUpdateModType() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "UPDATE";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("FirstName")).isEqualTo("kk");
    assertThat(bound.value("LastName")).isEqualTo("ll");
  }

  @Test
  public void testSpannerTableIdMismatch() {
    String sessionFile = "src/test/resources/errorSchemaSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    String newValuesString = "{\"Does\":\"not\",\"matter\":\"junk\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"Dont\":\"care\"}");
    String modType = "DELETE";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            mySQLDMLGenerator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void testSourcePkNull() {
    String sessionFile = "src/test/resources/errorSchemaSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Persons";
    String newValuesString = "{\"Does\":\"not\",\"matter\":\"junk\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"Dont\":\"care\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            mySQLDMLGenerator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void testSourceTableNotInSchema() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "contacts";
    String newValuesString = "{\"accountId\": \"Id1\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"Dont\":\"care\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            mySQLDMLGenerator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void testSpannerTableNotInSchemaObject() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "randomname";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\",\"SingerId\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SmthingElse\":null}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            mySQLDMLGenerator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void customTransformationMatch() {
    String sessionFile = "src/test/resources/customTransformation.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";
    Map<String, Object> customTransformation = new HashMap<>();
    // Custom transformation values are bound as data, so they must not be pre-quoted.
    customTransformation.put("FullName", "kk ll");
    customTransformation.put("SingerId", "1");

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setCustomTransformationResponse(customTransformation)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(sql.contains("ON DUPLICATE KEY UPDATE"));
    assertThat(bound.value("FullName")).isEqualTo("kk ll");
    assertThat(bound.value("SingerId")).isEqualTo("1");
    assertEquals(2, countInSQL(sql, "FullName"));
    assertEquals(2, countInSQL(sql, "SingerId"));
  }

  public long countInSQL(String sql, String targetWord) {
    if (sql == null || sql.isEmpty()) {
      return 0;
    }
    return Arrays.stream(sql.split("\\W+"))
        .filter(word -> word.equalsIgnoreCase(targetWord))
        .count();
  }

  @Test
  public void generatedColumnDML() {
    String sessionFile = "src/test/resources/generatedColumnSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "Singers";
    // FullName is generated, so it should be ignored even if present in newValues
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\",\"FullName\":\"kk ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("FirstName")).isEqualTo("kk");
    assertThat(bound.value("LastName")).isEqualTo("ll");
    // Verify FullName is NOT in the SQL
    // It should not be in the column list, values, or update clause.
    assertEquals(0, countInSQL(sql, "FullName"));
  }

  @Test
  public void testGeneratedPrimaryKeyDML() {
    String sessionFile = "src/test/resources/generatedColumnSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    // Modify the DDL to make FirstName the primary key, and it's a generated
    // column.
    Ddl modifiedDdl =
        ddl.toBuilder()
            .createTable("GeneratedPKTable")
            .column("SingerId")
            .int64()
            .endColumn()
            .column("FirstName")
            .string()
            .max()
            .generatedAs("SingerId")
            .endColumn()
            .column("LastName")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("FirstName")
            .end()
            .endTable()
            .build();

    SourceTable sourceTable =
        SourceTable.builder(SourceDatabaseType.MYSQL)
            .name("GeneratedPKTable")
            .schema(null)
            .primaryKeyColumns(ImmutableList.of("SingerId"))
            .columns(
                ImmutableList.of(
                    SourceColumn.builder(SourceDatabaseType.MYSQL)
                        .name("SingerId")
                        .type("bigint")
                        .isGenerated(true)
                        .build(),
                    SourceColumn.builder(SourceDatabaseType.MYSQL)
                        .name("FirstName")
                        .type("varchar")
                        .isPrimaryKey(true)
                        .build(),
                    SourceColumn.builder(SourceDatabaseType.MYSQL)
                        .name("LastName")
                        .type("varchar")
                        .build()))
            .build();
    SourceSchema modifiedSourceSchema =
        SourceSchema.builder(SourceDatabaseType.MYSQL)
            .databaseName("testdb")
            .tables(ImmutableMap.of("GeneratedPKTable", sourceTable))
            .build();

    ISchemaMapper mockSchemaMapper = Mockito.mock(ISchemaMapper.class);
    Mockito.when(mockSchemaMapper.getSpannerColumns(any(), eq("GeneratedPKTable")))
        .thenReturn(ImmutableList.of("SingerId", "FirstName", "LastName"));
    Mockito.when(
            mockSchemaMapper.getSpannerColumnName(any(), eq("GeneratedPKTable"), eq("LastName")))
        .thenReturn("LastName");
    Mockito.when(mockSchemaMapper.getSourceTableName(any(), eq("GeneratedPKTable")))
        .thenReturn("GeneratedPKTable");
    Mockito.when(mockSchemaMapper.isGeneratedColumn(any(), eq("GeneratedPKTable"), eq("FirstName")))
        .thenReturn(true);
    Mockito.when(mockSchemaMapper.isGeneratedColumn(any(), eq("GeneratedPKTable"), eq("SingerId")))
        .thenReturn(false);
    Mockito.when(mockSchemaMapper.isGeneratedColumn(any(), eq("GeneratedPKTable"), eq("LastName")))
        .thenReturn(false);

    for (String col : new String[] {"SingerId", "FirstName", "LastName"}) {
      Mockito.when(mockSchemaMapper.colExistsAtSource(any(), eq("GeneratedPKTable"), eq(col)))
          .thenReturn(true);
      Mockito.when(mockSchemaMapper.getSourceColumnName(any(), eq("GeneratedPKTable"), eq(col)))
          .thenReturn(col);
    }

    String tableName = "GeneratedPKTable";
    // newValues has dependent column `SingerId`, generated PK `FirstName` is
    // omitted
    String newValuesString = "{\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    // Event though it's the PK, the system might pass it in keyValues if available,
    // but if it's omitted in source changes:
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\",}");
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchemaMapper(mockSchemaMapper)
                .setDdl(modifiedDdl)
                .setSourceSchema(modifiedSourceSchema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("LastName")).isEqualTo("ll");
    // Verify FirstName is NOT in the generated column updates,
    // because it's a generated PK and skipped during DML creation.
    assertEquals(0, countInSQL(sql, "FirstName"));
    assertEquals(0, countInSQL(sql, "SingerId"));
  }

  @Test
  public void testBitTypeDML() throws Exception {
    Column spannerCol = Mockito.mock(Column.class);
    Mockito.when(spannerCol.name()).thenReturn("bit_column");
    Mockito.when(spannerCol.type()).thenReturn(Type.bytes());

    SourceColumn sourceCol = Mockito.mock(SourceColumn.class);
    Mockito.when(sourceCol.name()).thenReturn("bit_column");
    Mockito.when(sourceCol.type()).thenReturn("bit");

    JSONObject json = new JSONObject("{\"bit_column\":\"SGVsbG8=\"}"); // "Hello" in base64

    ColumnBinding binding =
        MySQLDMLGenerator.getMappedColumnBinding(spannerCol, sourceCol, json, "+00:00");
    assertThat(binding.getPlaceholderSql()).isEqualTo("?");
    assertThat(binding.getParams())
        .containsExactly(PreparedStatementValue.ofBytes("Hello".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testGetDMLStatement_NullRequest() {
    MySQLDMLGenerator generator = new MySQLDMLGenerator();
    assertThrows(InvalidDMLGenerationException.class, () -> generator.getDMLStatement(null));
  }

  @Test
  public void testGetDMLStatement_NullSchemaMapper() {
    MySQLDMLGenerator generator = new MySQLDMLGenerator();
    DMLGeneratorRequest request = Mockito.mock(DMLGeneratorRequest.class);
    Mockito.when(request.getSpannerTableName()).thenReturn("Singers");
    Mockito.when(request.getSchemaMapper()).thenReturn(null);
    Mockito.when(request.getSpannerDdl()).thenReturn(Mockito.mock(Ddl.class));
    Mockito.when(request.getSourceSchema()).thenReturn(Mockito.mock(SourceSchema.class));

    assertThrows(InvalidDMLGenerationException.class, () -> generator.getDMLStatement(request));
  }

  @Test
  public void testGetDMLStatement_NullSpannerDdl() {
    MySQLDMLGenerator generator = new MySQLDMLGenerator();
    DMLGeneratorRequest request = Mockito.mock(DMLGeneratorRequest.class);
    Mockito.when(request.getSpannerTableName()).thenReturn("Singers");
    Mockito.when(request.getSchemaMapper()).thenReturn(Mockito.mock(ISchemaMapper.class));
    Mockito.when(request.getSpannerDdl()).thenReturn(null);
    Mockito.when(request.getSourceSchema()).thenReturn(Mockito.mock(SourceSchema.class));

    assertThrows(InvalidDMLGenerationException.class, () -> generator.getDMLStatement(request));
  }

  @Test
  public void testGetDMLStatement_NullSourceSchema() {
    MySQLDMLGenerator generator = new MySQLDMLGenerator();
    DMLGeneratorRequest request = Mockito.mock(DMLGeneratorRequest.class);
    Mockito.when(request.getSpannerTableName()).thenReturn("Singers");
    Mockito.when(request.getSchemaMapper()).thenReturn(Mockito.mock(ISchemaMapper.class));
    Mockito.when(request.getSpannerDdl()).thenReturn(Mockito.mock(Ddl.class));
    Mockito.when(request.getSourceSchema()).thenReturn(null);

    assertThrows(InvalidDMLGenerationException.class, () -> generator.getDMLStatement(request));
  }

  @Test
  public void testGetDMLStatement_SourceTableNotFound() {
    MySQLDMLGenerator generator = new MySQLDMLGenerator();
    DMLGeneratorRequest request = Mockito.mock(DMLGeneratorRequest.class);
    ISchemaMapper schemaMapper = Mockito.mock(ISchemaMapper.class);
    SourceSchema sourceSchema = Mockito.mock(SourceSchema.class);
    Ddl ddl = Mockito.mock(Ddl.class);
    Table spannerTable = Mockito.mock(Table.class);

    Mockito.when(request.getSpannerTableName()).thenReturn("Singers");
    Mockito.when(request.getSchemaMapper()).thenReturn(schemaMapper);
    Mockito.when(request.getSourceSchema()).thenReturn(sourceSchema);
    Mockito.when(request.getSpannerDdl()).thenReturn(ddl);
    Mockito.when(ddl.table("Singers")).thenReturn(spannerTable);

    Mockito.when(schemaMapper.getSourceTableName("", "Singers")).thenReturn("Singers");
    Mockito.when(sourceSchema.table("Singers")).thenReturn(null); // Not found!

    assertThrows(InvalidDMLGenerationException.class, () -> generator.getDMLStatement(request));
  }

  @Test
  public void testGetDMLStatement_NoPrimaryKeys() {
    MySQLDMLGenerator generator = new MySQLDMLGenerator();
    DMLGeneratorRequest request = Mockito.mock(DMLGeneratorRequest.class);
    ISchemaMapper schemaMapper = Mockito.mock(ISchemaMapper.class);
    SourceSchema sourceSchema = Mockito.mock(SourceSchema.class);
    SourceTable sourceTable = Mockito.mock(SourceTable.class);
    Ddl ddl = Mockito.mock(Ddl.class);
    Table spannerTable = Mockito.mock(Table.class);

    Mockito.when(request.getSpannerTableName()).thenReturn("Singers");
    Mockito.when(request.getSchemaMapper()).thenReturn(schemaMapper);
    Mockito.when(request.getSourceSchema()).thenReturn(sourceSchema);
    Mockito.when(request.getSpannerDdl()).thenReturn(ddl);
    Mockito.when(ddl.table("Singers")).thenReturn(spannerTable);

    Mockito.when(schemaMapper.getSourceTableName("", "Singers")).thenReturn("Singers");
    Mockito.when(sourceSchema.table("Singers")).thenReturn(sourceTable);
    Mockito.when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of());

    assertThrows(InvalidDMLGenerationException.class, () -> generator.getDMLStatement(request));
  }

  @Test
  public void testGetDMLStatement_NullPrimaryKeys() {
    MySQLDMLGenerator generator = new MySQLDMLGenerator();
    DMLGeneratorRequest request = Mockito.mock(DMLGeneratorRequest.class);
    ISchemaMapper schemaMapper = Mockito.mock(ISchemaMapper.class);
    SourceSchema sourceSchema = Mockito.mock(SourceSchema.class);
    SourceTable sourceTable = Mockito.mock(SourceTable.class);
    Ddl ddl = Mockito.mock(Ddl.class);
    Table spannerTable = Mockito.mock(Table.class);

    Mockito.when(request.getSpannerTableName()).thenReturn("Singers");
    Mockito.when(request.getSchemaMapper()).thenReturn(schemaMapper);
    Mockito.when(request.getSourceSchema()).thenReturn(sourceSchema);
    Mockito.when(request.getSpannerDdl()).thenReturn(ddl);
    Mockito.when(ddl.table("Singers")).thenReturn(spannerTable);

    Mockito.when(schemaMapper.getSourceTableName("", "Singers")).thenReturn("Singers");
    Mockito.when(sourceSchema.table("Singers")).thenReturn(sourceTable);
    Mockito.when(sourceTable.primaryKeyColumns()).thenReturn(null); // NULL!

    assertThrows(InvalidDMLGenerationException.class, () -> generator.getDMLStatement(request));
  }

  @Test
  public void sqlInjectionPayloadIsBoundAsData() {
    String sessionFile = "src/test/resources/allMatchSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String firstName = "x'); DROP TABLE Singers; --";
    String lastName = "\\' OR 1=1 -- ";
    JSONObject newValuesJson = new JSONObject();
    newValuesJson.put("FirstName", firstName);
    newValuesJson.put("LastName", lastName);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");

    DMLGeneratorResponse dmlGeneratorResponse =
        new MySQLDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "Singers", newValuesJson, keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    // Record values never appear in the SQL text; they are bound verbatim, without escaping.
    assertThat(sql).doesNotContain("DROP");
    assertThat(sql).doesNotContain("OR 1=1");
    assertThat(sql).doesNotContain("'");
    assertThat(bound.value("FirstName")).isEqualTo(firstName);
    assertThat(bound.value("LastName")).isEqualTo(lastName);
    assertThat(bound.value("SingerId")).isEqualTo("999");
  }

  @Test
  public void deleteStatementIsParameterized() {
    String sessionFile = "src/test/resources/MultiColmPKSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    JSONObject keyValuesJson = new JSONObject();
    keyValuesJson.put("SingerId", "999");
    keyValuesJson.put("FirstName", "k' OR '1'='1");

    DMLGeneratorResponse dmlGeneratorResponse =
        new MySQLDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "DELETE", "Singers", new JSONObject(), keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(sql).startsWith("DELETE FROM `Singers` WHERE ");
    assertThat(sql).doesNotContain("'");
    assertThat(bound.placeholder("FirstName")).isEqualTo("?");
    assertThat(bound.value("FirstName")).isEqualTo("k' OR '1'='1");
    assertThat(bound.value("SingerId")).isEqualTo("999");
    assertThat(dmlGeneratorResponse.getPreparedStatementParameters()).hasSize(2);
  }

  @Test
  public void quoteIdentifierEscapesBackticks() {
    assertThat(MySQLDMLGenerator.quoteIdentifier("Singers")).isEqualTo("`Singers`");
    assertThat(MySQLDMLGenerator.quoteIdentifier("a`b")).isEqualTo("`a``b`");
    assertThat(MySQLDMLGenerator.quoteIdentifier("x`; DROP TABLE t; --"))
        .isEqualTo("`x``; DROP TABLE t; --`");
  }

  @Test
  public void customTransformationValuesAreBoundAsTypedData() {
    String sessionFile = "src/test/resources/customTransformation.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);
    Map<String, Object> customTransformation = new HashMap<>();
    // SQL expressions returned by a custom transformation are treated as plain strings.
    customTransformation.put("FullName", "CONCAT('kk', ' ', 'll')");
    customTransformation.put("SingerId", 7L);

    DMLGeneratorResponse dmlGeneratorResponse =
        new MySQLDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "Singers",
                        new JSONObject("{\"FirstName\":\"kk\",\"LastName\":\"ll\"}"),
                        new JSONObject("{\"SingerId\":\"999\"}"),
                        "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .setCustomTransformationResponse(customTransformation)
                    .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(dmlGeneratorResponse.getDmlStatement()).doesNotContain("CONCAT");
    assertThat(bound.value("FullName")).isEqualTo("CONCAT('kk', ' ', 'll')");
    assertThat(bound.value("SingerId")).isEqualTo(7L);
  }

  @Test
  public void customTransformationNullValueIsBoundAsNull() {
    String sessionFile = "src/test/resources/customTransformation.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);
    Map<String, Object> customTransformation = new HashMap<>();
    customTransformation.put("FullName", null);

    DMLGeneratorResponse dmlGeneratorResponse =
        new MySQLDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "Singers",
                        new JSONObject("{\"FirstName\":\"kk\",\"LastName\":\"ll\"}"),
                        new JSONObject("{\"SingerId\":\"999\"}"),
                        "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .setCustomTransformationResponse(customTransformation)
                    .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    assertThat(bound.value("FullName")).isNull();
    assertThat(bound.value("SingerId")).isEqualTo("999");
  }

  @Test
  public void customTransformationTimestampIsBoundAsIs() {
    String sessionFile = "src/test/resources/timeZoneSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);
    Map<String, Object> customTransformation = new HashMap<>();
    customTransformation.put("Bday", "2024-01-01 10:00:00");

    DMLGeneratorResponse dmlGeneratorResponse =
        new MySQLDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "Singers",
                        new JSONObject("{\"Bday\":\"2023-05-18T12:01:13.088397258Z\"}"),
                        new JSONObject("{\"SingerId\":\"999\"}"),
                        "+05:30")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .setCustomTransformationResponse(customTransformation)
                    .build());
    BoundDml bound = BoundDml.of(dmlGeneratorResponse);

    // No timezone conversion is applied to custom transformation values.
    assertThat(bound.placeholder("Bday")).isEqualTo("?");
    assertThat(bound.value("Bday")).isEqualTo("2024-01-01 10:00:00");
  }

  @Test
  public void stringToBinaryColumnUsesBinaryPlaceholder() {
    Column spannerCol = Mockito.mock(Column.class);
    Mockito.when(spannerCol.name()).thenReturn("varbinary_column");
    Mockito.when(spannerCol.type()).thenReturn(Type.string());
    SourceColumn sourceCol = Mockito.mock(SourceColumn.class);
    Mockito.when(sourceCol.type()).thenReturn("varbinary");
    JSONObject json = new JSONObject("{\"varbinary_column\":\"a'b\"}");

    ColumnBinding binding =
        MySQLDMLGenerator.getMappedColumnBinding(spannerCol, sourceCol, json, "+00:00");

    assertThat(binding.getPlaceholderSql()).isEqualTo("BINARY(?)");
    assertThat(binding.getParams()).containsExactly(PreparedStatementValue.ofString("a'b"));
  }

  @Test
  public void boolToNumericColumnIsBoundAsBoolean() {
    Column spannerCol = Mockito.mock(Column.class);
    Mockito.when(spannerCol.name()).thenReturn("bool_column");
    Mockito.when(spannerCol.type()).thenReturn(Type.bool());
    SourceColumn sourceCol = Mockito.mock(SourceColumn.class);
    Mockito.when(sourceCol.type()).thenReturn("tinyint");
    JSONObject json = new JSONObject("{\"bool_column\":true}");

    ColumnBinding binding =
        MySQLDMLGenerator.getMappedColumnBinding(spannerCol, sourceCol, json, "+00:00");

    assertThat(binding.getPlaceholderSql()).isEqualTo("?");
    assertThat(binding.getParams()).containsExactly(PreparedStatementValue.ofBoolean(true));
  }

  private static byte[] base64(String encoded) {
    return Base64.getDecoder().decode(encoded);
  }

  /**
   * A parsed view of a parameterized MySQL DML statement, mapping each column in the {@code ON
   * DUPLICATE KEY UPDATE} clause (or the {@code WHERE} clause of a {@code DELETE}) to its SQL
   * placeholder fragment and its bound parameter values.
   */
  private static final class BoundDml {
    private static final String UPSERT_MARKER = " ON DUPLICATE KEY UPDATE ";
    private static final String DELETE_MARKER = " WHERE ";

    private final Map<String, String> placeholders = new LinkedHashMap<>();
    private final Map<String, List<PreparedStatementValue>> params = new LinkedHashMap<>();

    static BoundDml of(DMLGeneratorResponse response) {
      return new BoundDml(response.getDmlStatement(), response.getPreparedStatementParameters());
    }

    private BoundDml(String sql, List<Object> allParams) {
      assertThat(allParams).isNotNull();
      String[] assignments;
      List<Object> columnParams;
      if (sql.startsWith("DELETE FROM ")) {
        assignments =
            sql.substring(sql.indexOf(DELETE_MARKER) + DELETE_MARKER.length()).split(" AND (?=`)");
        columnParams = allParams;
      } else {
        assertThat(sql).startsWith("INSERT INTO ");
        assertThat(sql).contains(UPSERT_MARKER);
        assignments =
            sql.substring(sql.indexOf(UPSERT_MARKER) + UPSERT_MARKER.length()).split(", (?=`)");
        // The same parameters are bound for the VALUES and the ON DUPLICATE KEY UPDATE clauses.
        assertThat(allParams.size() % 2).isEqualTo(0);
        int half = allParams.size() / 2;
        columnParams = allParams.subList(0, half);
        assertThat(allParams.subList(half, allParams.size()))
            .containsExactlyElementsIn(columnParams)
            .inOrder();
      }
      int index = 0;
      for (String assignment : assignments) {
        int separator = assignment.indexOf("` = ");
        String column = assignment.substring(1, separator).replace("``", "`");
        String placeholder = assignment.substring(separator + "` = ".length());
        long count = placeholder.chars().filter(c -> c == '?').count();
        List<PreparedStatementValue> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
          values.add((PreparedStatementValue) columnParams.get(index++));
        }
        placeholders.put(column, placeholder);
        params.put(column, values);
      }
      assertThat(index).isEqualTo(columnParams.size());
    }

    /** The SQL fragment used for the column, e.g. {@code ?} or {@code CONVERT_TZ(?, ...)}. */
    String placeholder(String column) {
      assertThat(placeholders).containsKey(column);
      return placeholders.get(column);
    }

    /** The raw Java values bound for the column, in order. */
    List<Object> values(String column) {
      assertThat(params).containsKey(column);
      List<Object> values = new ArrayList<>();
      for (PreparedStatementValue value : params.get(column)) {
        values.add(value.getValue());
      }
      return values;
    }

    /** The single raw Java value bound for the column ({@code null} for SQL NULL). */
    Object value(String column) {
      List<Object> values = values(column);
      assertThat(values).hasSize(1);
      return values.get(0);
    }

    byte[] bytes(String column) {
      Object value = value(column);
      assertThat(value).isInstanceOf(byte[].class);
      return (byte[]) value;
    }

    BigDecimal decimal(String column) {
      Object value = value(column);
      assertThat(value).isInstanceOf(BigDecimal.class);
      return (BigDecimal) value;
    }
  }
}
