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

import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ColumnPK;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NameAndCols;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SyntheticPKey;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.dbutils.processor.InputRecordProcessor;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
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
    Schema schema = SessionFileReader.read("src/test/resources/allMatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    /*The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
        + " UPDATE  FirstName = 'kk', LastName = 'll'";*/
    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`LastName` = 'll'"));
  }

  @Test
  public void tableNameMismatchAllColumnNameTypesMatch() {
    Schema schema = SessionFileReader.read("src/test/resources/tableNameMismatchSession.json");
    String tableName = "leChanteur";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    /* The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
        + " UPDATE  FirstName = 'kk', LastName = 'll'";*/
    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`LastName` = 'll'"));
  }

  @Test
  public void tableNameMatchColumnNameTypeMismatch() {
    Schema schema = SessionFileReader.read("src/test/resources/coulmnNameTypeMismatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"222\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";
    /*The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES ('999',222,'ll') ON DUPLICATE"
        + " KEY UPDATE  FirstName = 222, LastName = 'll'";*/
    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("`FirstName` = 222"));
    assertTrue(sql.contains("`LastName` = 'll'"));
  }

  @Test
  public void tableNameMatchSourceColumnNotPresentInSpanner() {
    Schema schema =
        SessionFileReader.read("src/test/resources/sourceColumnAbsentInSpannerSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    /* The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
        + " UPDATE  FirstName = 'kk', LastName = 'll'"; */
    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`LastName` = 'll'"));
  }

  @Test
  public void tableNameMatchSpannerColumnNotPresentInSource() {

    Schema schema =
        SessionFileReader.read("src/test/resources/spannerColumnAbsentInSourceSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\",\"hb_shardId\":\"shardA\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    /* The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
        + " UPDATE  FirstName = 'kk', LastName = 'll'";*/
    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`LastName` = 'll'"));
  }

  @Test
  public void primaryKeyNotFoundInJson() {
    Schema schema = SessionFileReader.read("src/test/resources/allMatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SomeRandomName\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    /* The expected sql is: ""*/
    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void primaryKeyNotPresentInSourceSchema() {
    Schema schema = SessionFileReader.read("src/test/resources/sourceNoPkSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    /* The expected sql is: ""*/
    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void timezoneOffsetMismatch() {
    Schema schema = SessionFileReader.read("src/test/resources/timeZoneSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"Bday\":\"2023-05-18T12:01:13.088397258Z\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
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
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(
        sql.contains("`Bday` =  CONVERT_TZ('2023-05-18T12:01:13.088397258','+00:00','+10:00'"));
  }

  @Test
  public void primaryKeyMismatch() {
    Schema schema = SessionFileReader.read("src/test/resources/primarykeyMismatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"SingerId\":\"999\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"FirstName\":\"kk\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    /* The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
        + " UPDATE  FirstName = 'kk', LastName = 'll'";*/
    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`LastName` = 'll'"));
  }

  @Test
  public void allDataypesDML() throws Exception {
    Schema schema = SessionFileReader.read("src/test/resources/allDatatypeSession.json");

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
                .setSchema(schema)
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
    Schema schema = SessionFileReader.read("src/test/resources/allMatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    /* The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk',NULL) ON DUPLICATE KEY"
        + " UPDATE  FirstName = 'kk', LastName = NULL";*/
    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`LastName` = NULL"));
  }

  @Test
  public void deleteMultiplePKColumns() {
    Schema schema = SessionFileReader.read("src/test/resources/MultiColmPKSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"LastName\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\",\"FirstName\":\"kk\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "DELETE";

    /* The expected sql is:
    "DELETE FROM Singers WHERE  FirstName = 'kk' AND  SingerId = 999";*/
    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`SingerId` = 999"));
    assertTrue(sql.contains("DELETE FROM `Singers` WHERE"));
  }

  @Test
  public void testSingleQuoteMatch() {
    Schema schema = SessionFileReader.read("src/test/resources/allMatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"k\u0027k\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    /* The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'k''k','ll') ON DUPLICATE KEY"
        + " UPDATE  FirstName = 'k''k', LastName = 'll'"; */
    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("`FirstName` = 'k''k'"));
    assertTrue(sql.contains("`LastName` = 'll'"));
  }

  @Test
  public void singleQuoteBytesDML() throws Exception {
    Schema schema = SessionFileReader.read("src/test/resources/quotesSession.json");
    /*
    Spanner write is : CAST("\'" as BYTES) for blob and "\'" for varchar
    Eventual insert is '' but mysql synatx escapes each ' with another '*/

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Jw\u003d\u003d\",\"varchar_column\":\"\u0027\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"id\":\"12\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
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
                .setSchema(schema)
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

    Schema schema = SessionFileReader.read("src/test/resources/quotesSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Jyc\u003d\",\"varchar_column\":\"\u0027\u0027\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"id\":\"12\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
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
                .setSchema(schema)
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

    Schema schema = SessionFileReader.read("src/test/resources/quotesSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"XCc\u003d\",\"varchar_column\":\"\\\\\\\u0027\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"id\":\"12\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
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
                .setSchema(schema)
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

    Schema schema = SessionFileReader.read("src/test/resources/quotesSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"CQ==\",\"varchar_column\":\"\\t\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"id\":\"12\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
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
                .setSchema(schema)
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

    Schema schema = SessionFileReader.read("src/test/resources/quotesSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"CA==\",\"varchar_column\":\"\\b\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"id\":\"12\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
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
                .setSchema(schema)
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

    Schema schema = SessionFileReader.read("src/test/resources/quotesSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Cg==\",\"varchar_column\":\"\\n\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"id\":\"12\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
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
                .setSchema(schema)
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

    Schema schema = SessionFileReader.read("src/test/resources/quotesSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"DQ==\",\"varchar_column\":\"\\r\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"id\":\"12\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
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
                .setSchema(schema)
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

    Schema schema = SessionFileReader.read("src/test/resources/quotesSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"DA==\",\"varchar_column\":\"\\f\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"id\":\"12\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
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
                .setSchema(schema)
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

    Schema schema = SessionFileReader.read("src/test/resources/quotesSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Ig==\",\"varchar_column\":\"\\\"\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"id\":\"12\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
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
                .setSchema(schema)
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

    Schema schema = SessionFileReader.read("src/test/resources/quotesSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"XA==\",\"varchar_column\":\"\\\\\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"id\":\"12\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
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
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("`varchar_column` = '\\\\'"));
    assertTrue(sql.contains("`blob_column` = FROM_BASE64('XA==')"));
  }

  @Test
  public void bitColumnSql() {
    Schema schema = SessionFileReader.read("src/test/resources/bitSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"YmlsX2NvbA\u003d\u003d\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    /*The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES"
        + " (999,'kk',BINARY(FROM_BASE64('YmlsX2NvbA=='))) ON DUPLICATE KEY UPDATE  FirstName ="
        + " 'kk', LastName = BINARY(FROM_BASE64('YmlsX2NvbA=='))"; */
    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("`LastName` = BINARY(FROM_BASE64('YmlsX2NvbA=='))"));
  }

  @Test
  public void testSpannerTableNotInSchema() {
    Schema schema = SessionFileReader.read("src/test/resources/allMatchSession.json");
    String tableName = "SomeRandomTableNotInSchema";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void testSpannerKeyIsNull() {
    Schema schema = SessionFileReader.read("src/test/resources/allMatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":null}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(
        sql.contains(
            "INSERT INTO `Singers`(`SingerId`,`FirstName`,`LastName`) VALUES (NULL,'kk','ll')"));
  }

  @Test
  public void testKeyInNewValuesJson() {
    Schema schema = SessionFileReader.read("src/test/resources/allMatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\",\"SingerId\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SmthingElse\":null}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    assertTrue(
        sql.contains(
            "INSERT INTO `Singers`(`SingerId`,`FirstName`,`LastName`) VALUES (NULL,'kk','ll')"));
  }

  @Test
  public void testSourcePKNotInSpanner() {
    Schema schema = SessionFileReader.read("src/test/resources/errorSchemaSession.json");
    String tableName = "customer";
    String newValuesString = "{\"Does\":\"not\",\"matter\":\"junk\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"Dont\":\"care\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "DELETE";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void primaryKeyMismatchSpannerNull() {
    Schema schema = SessionFileReader.read("src/test/resources/primarykeyMismatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"SingerId\":\"999\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"FirstName\":null}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    /* The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,NULL,'ll') ON DUPLICATE KEY"
        + " UPDATE  FirstName = NULL , LastName = 'll'";*/
    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("`FirstName` = NULL"));
  }

  @Test
  public void testUnsupportedModType() {
    Schema schema = SessionFileReader.read("src/test/resources/allMatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "JUNK";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void testUpdateModType() {
    Schema schema = SessionFileReader.read("src/test/resources/allMatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "UPDATE";

    /*The expected sql is:
    "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
        + " UPDATE  FirstName = 'kk', LastName = 'll'";*/
    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("`FirstName` = 'kk'"));
    assertTrue(sql.contains("`LastName` = 'll'"));
  }

  @Test
  public void testSpannerTableIdMismatch() {
    Schema schema = SessionFileReader.read("src/test/resources/errorSchemaSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"Does\":\"not\",\"matter\":\"junk\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"Dont\":\"care\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "DELETE";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void testSourcePkNull() {
    Schema schema = SessionFileReader.read("src/test/resources/errorSchemaSession.json");
    String tableName = "Persons";
    String newValuesString = "{\"Does\":\"not\",\"matter\":\"junk\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"Dont\":\"care\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void testSourceTableNotInSchema() {
    Schema schema = getSchemaObject();
    String tableName = "contacts";
    String newValuesString = "{\"accountId\": \"Id1\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"Dont\":\"care\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void testSpannerTableNotInSchemaObject() {
    Schema schema = SessionFileReader.read("src/test/resources/allMatchSession.json");
    String tableName = "Singers";
    schema.getSpSchema().remove(schema.getSpannerToID().get(tableName).getName());
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\",\"SingerId\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SmthingElse\":null}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void testSpannerColDefsNull() {
    Schema schema = SessionFileReader.read("src/test/resources/allMatchSession.json");
    String tableName = "Singers";

    String spannerTableId = schema.getSpannerToID().get(tableName).getName();
    SpannerTable spannerTable = schema.getSpSchema().get(spannerTableId);
    spannerTable.getColDefs().remove("c5");
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"23\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    MySQLDMLGenerator mySQLDMLGenerator = new MySQLDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        mySQLDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    MySQLDMLGenerator test = new MySQLDMLGenerator(); // to add that last bit of code coverage
    InputRecordProcessor test2 =
        new InputRecordProcessor(); // to add that last bit of code coverage
    assertTrue(sql.isEmpty());
  }

  public static Schema getSchemaObject() {
    Map<String, SyntheticPKey> syntheticPKeys = new HashMap<String, SyntheticPKey>();
    Map<String, SourceTable> srcSchema = new HashMap<String, SourceTable>();
    Map<String, SpannerTable> spSchema = getSampleSpSchema();
    Map<String, NameAndCols> spannerToID = getSampleSpannerToId();
    Schema expectedSchema = new Schema(spSchema, syntheticPKeys, srcSchema);
    expectedSchema.setSpannerToID(spannerToID);
    return expectedSchema;
  }

  public static Map<String, SpannerTable> getSampleSpSchema() {
    Map<String, SpannerTable> spSchema = new HashMap<String, SpannerTable>();
    Map<String, SpannerColumnDefinition> t1SpColDefs =
        new HashMap<String, SpannerColumnDefinition>();
    t1SpColDefs.put(
        "c1", new SpannerColumnDefinition("accountId", new SpannerColumnType("STRING", false)));
    t1SpColDefs.put(
        "c2", new SpannerColumnDefinition("accountName", new SpannerColumnType("STRING", false)));
    t1SpColDefs.put(
        "c3",
        new SpannerColumnDefinition("migration_shard_id", new SpannerColumnType("STRING", false)));
    t1SpColDefs.put(
        "c4", new SpannerColumnDefinition("accountNumber", new SpannerColumnType("INT", false)));
    spSchema.put(
        "t1",
        new SpannerTable(
            "tableName",
            new String[] {"c1", "c2", "c3", "c4"},
            t1SpColDefs,
            new ColumnPK[] {new ColumnPK("c1", 1)},
            "c3"));
    return spSchema;
  }

  public static Map<String, NameAndCols> getSampleSpannerToId() {
    Map<String, NameAndCols> spannerToId = new HashMap<String, NameAndCols>();
    Map<String, String> t1ColIds = new HashMap<String, String>();
    t1ColIds.put("accountId", "c1");
    t1ColIds.put("accountName", "c2");
    t1ColIds.put("migration_shard_id", "c3");
    t1ColIds.put("accountNumber", "c4");
    spannerToId.put("tableName", new NameAndCols("t1", t1ColIds));
    return spannerToId;
  }
}
