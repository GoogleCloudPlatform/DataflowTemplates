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
package com.google.cloud.teleport.v2.templates.dbutils.dml;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.utils.SchemaUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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

    /*The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
        + " UPDATE  FirstName = 'kk', LastName = 'll'";*/
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

    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`LastName` = 'll'"));
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

    /* The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
        + " UPDATE  FirstName = 'kk', LastName = 'll'";*/
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

    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`LastName` = 'll'"));
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
    /*The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES ('999',222,'ll') ON DUPLICATE"
        + " KEY UPDATE  FirstName = 222, LastName = 'll'";*/
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

    assertTrue(sql.contains("`FirstName` = 222"));
    assertTrue(sql.contains("`LastName` = 'll'"));
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

    /* The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
        + " UPDATE  FirstName = 'kk', LastName = 'll'"; */
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

    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`LastName` = 'll'"));
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

    /* The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
        + " UPDATE  FirstName = 'kk', LastName = 'll'";*/
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

    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`LastName` = 'll'"));
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

    /* The expected sql is: ""*/
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

    /* The expected sql is: ""*/
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

    /*The expected sql is:
    INSERT INTO `resource_access`(`user_id`,`group_id`,`resource_id`) VALUES (101,5,99) ON DUPLICATE KEY UPDATE  `user_id` = 101, `group_id` = 5, `resource_id` = 99
    */
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
    assertThat(sql.contains("ON DUPLICATE KEY UPDATE"));
    assertTrue(sql.contains("`user_id` = 101"));
    assertTrue(sql.contains("`group_id` = 5"));
    assertTrue(sql.contains("`resource_id` = 99"));
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

    /* The expected sql is:
    "INSERT INTO Singers(SingerId,Bday) VALUES (999,"
        + " CONVERT_TZ('2023-05-18T12:01:13.088397258','+00:00','+10:00')) ON DUPLICATE KEY"
        + " UPDATE  Bday =  CONVERT_TZ('2023-05-18T12:01:13.088397258','+00:00','+10:00')";*/
    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+10:00")
                .setSchemaMapper(schemaMapper)
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(
        sql.contains("`Bday` =  CONVERT_TZ('2023-05-18T12:01:13.088397258','+00:00','+10:00'"));
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

    /* The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
        + " UPDATE  FirstName = 'kk', LastName = 'll'";*/
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

    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`LastName` = 'll'"));
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

    /* The expected sql is:
    "INSERT INTO"
        + " sample_table(id,mediumint_column,tinyblob_column,datetime_column,enum_column,longtext_column,mediumblob_column,text_column,tinyint_column,timestamp_column,float_column,varbinary_column,binary_column,bigint_column,time_column,tinytext_column,set_column,longblob_column,mediumtext_column,year_column,blob_column,decimal_column,bool_column,char_column,date_column,double_column,smallint_column,varchar_column)"
        + " VALUES (12,333,FROM_BASE64('YWJj'),"
        + " CONVERT_TZ('2023-05-18T12:01:13.088397258','+00:00','+00:00'),'1','<longtext_column>',FROM_BASE64('YWJjbGFyZ2U='),'aaaaaddd',1,"
        + " CONVERT_TZ('2023-05-18T12:01:13.088397258','+00:00','+00:00'),4.2,BINARY(FROM_BASE64('YWJjbGFyZ2U=')),BINARY(FROM_BASE64('YWJjbGFyZ2U=')),4444,'10:10:10','<tinytext_column>','1,2',FROM_BASE64('YWJsb25nYmxvYmM='),'<mediumtext_column>','2023',FROM_BASE64('YWJiaWdj'),444.222,false,'<char_c','2023-05-18',42.42,22,'abc')"
        + " ON DUPLICATE KEY UPDATE  mediumint_column = 333, tinyblob_column ="
        + " FROM_BASE64('YWJj'), datetime_column = "
        + " CONVERT_TZ('2023-05-18T12:01:13.088397258','+00:00','+00:00'), enum_column = '1',"
        + " longtext_column = '<longtext_column>', mediumblob_column ="
        + " FROM_BASE64('YWJjbGFyZ2U='), text_column = 'aaaaaddd', tinyint_column = 1,"
        + " timestamp_column =  CONVERT_TZ('2023-05-18T12:01:13.088397258','+00:00','+00:00'),"
        + " float_column = 4.2, varbinary_column = BINARY(FROM_BASE64('YWJjbGFyZ2U=')),"
        + " binary_column = BINARY(FROM_BASE64('YWJjbGFyZ2U=')), bigint_column = 4444, time_column"
        + " = '10:10:10', tinytext_column = '<tinytext_column>', set_column = '1,2',"
        + " longblob_column = FROM_BASE64('YWJsb25nYmxvYmM='), mediumtext_column ="
        + " '<mediumtext_column>', year_column = '2023', blob_column = FROM_BASE64('YWJiaWdj'),"
        + " decimal_column = 444.222, bool_column = false, char_column = '<char_c', date_column"
        + " = '2023-05-18', double_column = 42.42, smallint_column = 22, varchar_column ="
        + " 'abc'"; */
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

    assertTrue(sql.contains("`mediumint_column` = 333"));
    assertTrue(sql.contains("`tinyblob_column` = FROM_BASE64('YWJj')"));
    boolean datetimeFlag =
        sql.contains(
            "`datetime_column` =  CONVERT_TZ('2023-05-18T12:01:13.088397258','+00:00','+00:00'");
    assertTrue(datetimeFlag);
    // The same assert below fails to run hence as a workaround we are using the above boolean
    // flag
    /*  assertTrue(
    sql.contains(
        "datetime_column = CONVERT_TZ('2023-05-18T12:01:13.088397258','+00:00','+00:00')"));*/
    assertTrue(sql.contains("`enum_column` = '1'"));
    assertTrue(sql.contains("`longtext_column` = '<longtext_column>'"));
    assertTrue(sql.contains("`mediumblob_column` = FROM_BASE64('YWJjbGFyZ2U=')"));
    assertTrue(sql.contains("`text_column` = 'aaaaaddd'"));
    assertTrue(sql.contains("`tinyint_column` = 1"));
    boolean timestampFlag =
        sql.contains(
            "`timestamp_column` =  CONVERT_TZ('2023-05-18T12:01:13.088397258','+00:00','+00:00')");
    assertTrue(timestampFlag);
    // The same assert below fails to run hence as a workaround we are using the above boolean
    // flag
    /* assertTrue(
    sql.contains(
        "timestamp_column = CONVERT_TZ('2023-05-18T12:01:13.088397258','+00:00','+00:00')"));*/
    assertTrue(sql.contains("`float_column` = 4.2"));
    assertTrue(sql.contains("`varbinary_column` = BINARY(FROM_BASE64('YWJjbGFyZ2U='))"));
    assertTrue(sql.contains("`binary_column` = BINARY(FROM_BASE64('YWJjbGFyZ2U='))"));
    assertTrue(sql.contains("`bigint_column` = 4444"));
    assertTrue(sql.contains("`time_column` = '10:10:10'"));
    assertTrue(sql.contains("`tinytext_column` = '<tinytext_column>'"));
    assertTrue(sql.contains("`set_column` = '1,2'"));
    assertTrue(sql.contains("`longblob_column` = FROM_BASE64('YWJsb25nYmxvYmM=')"));
    assertTrue(sql.contains("`mediumtext_column` = '<mediumtext_column>'"));
    assertTrue(sql.contains("`year_column` = '2023'"));
    assertTrue(sql.contains("`blob_column` = FROM_BASE64('YWJiaWdj')"));
    assertTrue(sql.contains("`decimal_column` = 444.222"));
    assertTrue(sql.contains("`bool_column` = false"));
    assertTrue(sql.contains("`char_column` = '<char_c'"));
    assertTrue(sql.contains("`date_column` = '2023-05-18'"));
    assertTrue(sql.contains("`double_column` = 42.42"));
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

    /* The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk',NULL) ON DUPLICATE KEY"
        + " UPDATE  FirstName = 'kk', LastName = NULL";*/
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

    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`LastName` = NULL"));
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

    /* The expected sql is:
    "DELETE FROM Singers WHERE  FirstName = 'kk' AND  SingerId = 999";*/
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

    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`SingerId` = 999"));
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

    /* The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'k''k','ll') ON DUPLICATE KEY"
        + " UPDATE  FirstName = 'k''k', LastName = 'll'"; */
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

    assertTrue(sql.contains("`FirstName` = 'k''k'"));
    assertTrue(sql.contains("`LastName` = 'll'"));
  }

  @Test
  public void singleQuoteBytesDML() throws Exception {
    String sessionFile = "src/test/resources/quotesSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    /*
    Spanner write is : CAST("\'" as BYTES) for blob and "\'" for varchar
    Eventual insert is '' but mysql synatx escapes each ' with another '*/

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Jw\u003d\u003d\",\"varchar_column\":\"\u0027\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";

    /* The expected sql is:
    "INSERT INTO"
        + " sample_table(id,varchar_column,blob_column)"
        + " VALUES (12,'''',FROM_BASE64('Jw=='))"
        + " ON DUPLICATE KEY UPDATE  varchar_column = '''', blob_column = FROM_BASE64('Jw==')";*/
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
    assertTrue(sql.contains("`varchar_column` = '''"));
    assertTrue(sql.contains("`blob_column` = FROM_BASE64('Jw==')"));
  }

  @Test
  public void twoSingleEscapedQuoteDML() throws Exception {
    /*
    Spanner write is : CAST("\''" as BYTES) for blob and "\'" for varchar
    Eventual insert is '' but mysql synatx escapes each ' with another '*/

    String sessionFile = "src/test/resources/quotesSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Jyc\u003d\",\"varchar_column\":\"\u0027\u0027\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";

    /* The expected sql is:
    "INSERT INTO sample_table(id,varchar_column,blob_column) VALUES"
        + " (12,'''''',FROM_BASE64('Jyc=')) ON DUPLICATE KEY UPDATE  varchar_column = '''''',"
        + " blob_column = FROM_BASE64('Jyc=')";*/
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

    assertTrue(sql.contains("`varchar_column` = '''''"));
    assertTrue(sql.contains("`blob_column` = FROM_BASE64('Jyc=')"));
  }

  @Test
  public void threeEscapesAndSingleQuoteDML() throws Exception {
    /*
    Spanner write is : CAST("\\\'" as BYTES) for blob and "\\\'" for varchar
    Eventual insert is \' but mysql synatx escapes each ' with another ' and \ with another \*/

    String sessionFile = "src/test/resources/quotesSession.json";
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(sessionFile);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(sessionFile);
    ISchemaMapper schemaMapper = new SessionBasedMapper(sessionFile, ddl);

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"XCc\u003d\",\"varchar_column\":\"\\\\\\\u0027\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";

    /* The expected sql is:
    "INSERT INTO sample_table(id,varchar_column,blob_column) VALUES"
        + " (12,'\\\\''',FROM_BASE64('XCc=')) ON DUPLICATE KEY UPDATE  varchar_column ="
        + " '\\\\''', blob_column = FROM_BASE64('XCc=')";*/
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

    assertTrue(sql.contains("`varchar_column` = '\\\\'"));
    assertTrue(sql.contains("`blob_column` = FROM_BASE64('XCc=')"));
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

    /* The expected sql is:
    "INSERT INTO sample_table(id,varchar_column,blob_column) VALUES (12,'"
        + "\t',FROM_BASE64('CQ==')) ON DUPLICATE KEY UPDATE  varchar_column = '\t', blob_column"
        + " = FROM_BASE64('CQ==')"; */
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

    assertTrue(sql.contains("`varchar_column` = '\t'"));
    assertTrue(sql.contains("`blob_column` = FROM_BASE64('CQ==')"));
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

    /* The expected sql is:
    "INSERT INTO sample_table(id,varchar_column,blob_column) VALUES"
        + " (12,'\b',FROM_BASE64('CA==')) ON DUPLICATE KEY UPDATE  varchar_column = '\b',"
        + " blob_column = FROM_BASE64('CA==')";*/
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

    assertTrue(sql.contains("`varchar_column` = '\b'"));
    assertTrue(sql.contains("`blob_column` = FROM_BASE64('CA==')"));
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

    /* The expected sql is:
    "INSERT INTO sample_table(id,varchar_column,blob_column) VALUES (12,'\n"
        + "',FROM_BASE64('Cg==')) ON DUPLICATE KEY UPDATE  varchar_column = '\n"
        + "', blob_column = FROM_BASE64('Cg==')";*/
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

    assertTrue(sql.contains("`varchar_column` = '\n'"));
    assertTrue(sql.contains("`blob_column` = FROM_BASE64('Cg==')"));
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

    /* The expected sql is:
    "INSERT INTO sample_table(id,varchar_column,blob_column) VALUES (12,'\r"
        + "',FROM_BASE64('DQ==')) ON DUPLICATE KEY UPDATE  varchar_column = '\r"
        + "', blob_column = FROM_BASE64('DQ==')";*/
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

    assertTrue(sql.contains("`varchar_column` = '\r'"));
    assertTrue(sql.contains("`blob_column` = FROM_BASE64('DQ==')"));
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

    /* The expected sql is:
    "INSERT INTO sample_table(id,varchar_column,blob_column) VALUES"
        + " (12,'\f',FROM_BASE64('DA==')) ON DUPLICATE KEY UPDATE  varchar_column = '\f',"
        + " blob_column = FROM_BASE64('DA==')";*/
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

    assertTrue(sql.contains("`varchar_column` = '\f'"));
    assertTrue(sql.contains("`blob_column` = FROM_BASE64('DA==')"));
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

    /* The expected sql is:
    "INSERT INTO sample_table(id,varchar_column,blob_column) VALUES"
        + " (12,'\"',FROM_BASE64('Ig==')) ON DUPLICATE KEY UPDATE  varchar_column = '\"',"
        + " blob_column = FROM_BASE64('Ig==')";*/
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

    assertTrue(sql.contains("`varchar_column` = '\"'"));
    assertTrue(sql.contains("`blob_column` = FROM_BASE64('Ig==')"));
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

    /*The expected sql is:
    "INSERT INTO sample_table(id,varchar_column,blob_column) VALUES"
        + " (12,'\\\\',FROM_BASE64('XA==')) ON DUPLICATE KEY UPDATE  varchar_column = '\\\\',"
        + " blob_column = FROM_BASE64('XA==')";*/
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

    assertTrue(sql.contains("`varchar_column` = '\\\\'"));
    assertTrue(sql.contains("`blob_column` = FROM_BASE64('XA==')"));
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

    /*The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES"
        + " (999,'kk',BINARY(FROM_BASE64('YmlsX2NvbA=='))) ON DUPLICATE KEY UPDATE  FirstName ="
        + " 'kk', LastName = x'62696c5f636f6c'))";
     Base64 decode of `YmlsX2NvbA==` is `bil_col`
     Char to Hex for `bil_col` is `62 69 6C 5F 63 6F 6C`
        */
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

    assertTrue(sql.contains("`LastName` = x'62696c5f636f6c'"));
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
    assertThat(sql.contains("ON DUPLICATE KEY UPDATE"));
    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`SingerId` = NULL"));
    assertTrue(sql.contains("`LastName` = 'll'"));
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
    assertThat(sql.contains("ON DUPLICATE KEY UPDATE"));
    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`SingerId` = NULL"));
    assertTrue(sql.contains("`LastName` = 'll'"));
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

    /* The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,NULL,'ll') ON DUPLICATE KEY"
        + " UPDATE  FirstName = NULL , LastName = 'll'";*/
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

    assertTrue(sql.contains("`FirstName` = NULL"));
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

    /*The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
        + " UPDATE  FirstName = 'kk', LastName = 'll'";*/
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

    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`LastName` = 'll'"));
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
    customTransformation.put("FullName", "\'kk ll\'");
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

    assertThat(sql.contains("ON DUPLICATE KEY UPDATE"));
    assertTrue(sql.contains("`FullName` = 'kk ll'"));
    assertTrue(sql.contains("`SingerId` = 1"));
    assertEquals(2, countInSQL(sql, "FullName"));
    assertEquals(2, countInSQL(sql, "SingerId"));
  }

  @Test
  public void testConvertBase64ToXHex() {
    assertThat(MySQLDMLGenerator.convertBase64ToHex(null)).isNull();
    assertThat(MySQLDMLGenerator.convertBase64ToHex("")).isEqualTo("x''");
    assertThat(MySQLDMLGenerator.convertBase64ToHex("AA==")).isEqualTo("x'00'");
    assertThat(MySQLDMLGenerator.convertBase64ToHex("R09PR0xF")).isEqualTo("x'474f4f474c45'");
    // Invalid Base64 string.
    assertThrows(
        IllegalArgumentException.class,
        () -> MySQLDMLGenerator.convertBase64ToHex("####GOOGLE####"));
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

    /*
     * The expected sql is:
     * "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
     * + " UPDATE  FirstName = 'kk', LastName = 'll'";
     */
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

    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`LastName` = 'll'"));
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
                    com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn.builder(
                            SourceDatabaseType.MYSQL)
                        .name("SingerId")
                        .type("bigint")
                        .isGenerated(true)
                        .build(),
                    com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn.builder(
                            SourceDatabaseType.MYSQL)
                        .name("FirstName")
                        .type("varchar")
                        .isPrimaryKey(true)
                        .build(),
                    com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn.builder(
                            SourceDatabaseType.MYSQL)
                        .name("LastName")
                        .type("varchar")
                        .build()))
            .build();
    SourceSchema modifiedSourceSchema =
        SourceSchema.builder(SourceDatabaseType.MYSQL)
            .databaseName("testdb")
            .tables(ImmutableMap.of("GeneratedPKTable", sourceTable))
            .build();

    ISchemaMapper mockSchemaMapper = org.mockito.Mockito.mock(ISchemaMapper.class);
    org.mockito.Mockito.when(
            mockSchemaMapper.getSpannerColumns(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.eq("GeneratedPKTable")))
        .thenReturn(ImmutableList.of("SingerId", "FirstName", "LastName"));
    org.mockito.Mockito.when(
            mockSchemaMapper.getSpannerColumnName(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.eq("GeneratedPKTable"),
                org.mockito.ArgumentMatchers.eq("LastName")))
        .thenReturn("LastName");
    org.mockito.Mockito.when(
            mockSchemaMapper.getSourceTableName(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.eq("GeneratedPKTable")))
        .thenReturn("GeneratedPKTable");
    org.mockito.Mockito.when(
            mockSchemaMapper.isGeneratedColumn(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.eq("GeneratedPKTable"),
                org.mockito.ArgumentMatchers.eq("FirstName")))
        .thenReturn(true);
    org.mockito.Mockito.when(
            mockSchemaMapper.isGeneratedColumn(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.eq("GeneratedPKTable"),
                org.mockito.ArgumentMatchers.eq("SingerId")))
        .thenReturn(false);
    org.mockito.Mockito.when(
            mockSchemaMapper.isGeneratedColumn(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.eq("GeneratedPKTable"),
                org.mockito.ArgumentMatchers.eq("LastName")))
        .thenReturn(false);

    for (String col : new String[] {"SingerId", "FirstName", "LastName"}) {
      org.mockito.Mockito.when(
              mockSchemaMapper.colExistsAtSource(
                  org.mockito.ArgumentMatchers.any(),
                  org.mockito.ArgumentMatchers.eq("GeneratedPKTable"),
                  org.mockito.ArgumentMatchers.eq(col)))
          .thenReturn(true);
      org.mockito.Mockito.when(
              mockSchemaMapper.getSourceColumnName(
                  org.mockito.ArgumentMatchers.any(),
                  org.mockito.ArgumentMatchers.eq("GeneratedPKTable"),
                  org.mockito.ArgumentMatchers.eq(col)))
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

    assertTrue(sql.contains("`LastName` = 'll'"));
    // Verify FirstName is NOT in the generated column updates,
    // because it's a generated PK and skipped during DML creation.
    assertEquals(0, countInSQL(sql, "FirstName"));
    assertEquals(0, countInSQL(sql, "SingerId"));
  }
}
