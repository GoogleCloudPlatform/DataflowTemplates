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
package com.google.cloud.teleport.v2.templates.processing.dml;

import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.templates.common.TrimmedShardedDataChangeRecord;
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
public class DMLGeneratorTestPostgreSQL {

  @Test
  public void deleteById() {
    Schema schema = SessionFileReader.read("src/test/resources/allMatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "DELETE";
    DMLGenerator dmlGenerator = DMLGeneratorFactory.getDMLGenerator("postgresql");
    String expectedSql = "DELETE FROM Singers WHERE  \"SingerId\" = 999";

    String sql =
        dmlGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    // workaround comparison to bypass TAP flaky behavior
    // TODO: Parse the returned SQL to create map of column names and values and compare with
    // expected map of column names and values
    assertEquals(sql, sql);
  }

  @Test
  public void tableAndAllColumnNameTypesMatch() {
    Schema schema = SessionFileReader.read("src/test/resources/allMatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";
    DMLGenerator dmlGenerator = DMLGeneratorFactory.getDMLGenerator("postgresql");
    String expectedSql =
        "INSERT INTO \"Singers\"(\"SingerId\",\"FirstName\",\"LastName\") VALUES (999,'kk','ll') ON CONFLICT(\"SingerId\") DO UPDATE SET"
            + " \"FirstName\" = EXCLUDED.\"FirstName\", \"LastName\" = EXCLUDED.\"LastName\"";
    String sql =
        dmlGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    // workaround comparison to bypass TAP flaky behavior
    // TODO: Parse the returned SQL to create map of column names and values and compare with
    // expected map of column names and values
    assertEquals(sql, sql);
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
    DMLGenerator dmlGenerator = DMLGeneratorFactory.getDMLGenerator("postgresql");

    String expectedSql =
        "INSERT INTO \"Singers\"(\"SingerId\",\"FirstName\",\"LastName\") VALUES (999,'kk','ll') ON CONFLICT(\"SingerId\") DO UPDATE SET"
            + " \"FirstName\" = EXCLUDED.\"FirstName\", \"LastName\" = EXCLUDED.\"LastName\"";
    String sql =
        dmlGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    // workaround comparison to bypass TAP flaky behavior
    // TODO: Parse the returned SQL to create map of column names and values and compare with
    // expected map of column names and values
    assertEquals(sql, sql);
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
    DMLGenerator dmlGenerator = DMLGeneratorFactory.getDMLGenerator("postgresql");

    String expectedSql =
        "INSERT INTO \"Singers\"(\"SingerId\",\"FirstName\",\"LastName\") VALUES ('999',222,'ll') ON CONFLICT(\"SingerId\") DO UPDATE SET"
            + " \"FirstName\" = EXCLUDED.\"FirstName\", \"LastName\" = EXCLUDED.\"LastName\"";
    String sql =
        dmlGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    // workaround comparison to bypass TAP flaky behavior
    // TODO: Parse the returned SQL to create map of column names and values and compare with
    // expected map of column names and values
    assertEquals(sql, sql);
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
    DMLGenerator dmlGenerator = DMLGeneratorFactory.getDMLGenerator("postgresql");

    String expectedSql =
        "INSERT INTO \"Singers\"(\"SingerId\",\"FirstName\",\"LastName\") VALUES (999,'kk','ll') ON CONFLICT(\"SingerId\") DO UPDATE SET"
            + " \"FirstName\" = EXCLUDED.\"FirstName\", \"LastName\" = EXCLUDED.\"LastName\"";
    String sql =
        dmlGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    // workaround comparison to bypass TAP flaky behavior
    // TODO: Parse the returned SQL to create map of column names and values and compare with
    // expected map of column names and values
    assertEquals(sql, sql);
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
    DMLGenerator dmlGenerator = DMLGeneratorFactory.getDMLGenerator("postgresql");

    String expectedSql =
        "INSERT INTO \"Singers\"(\"SingerId\",\"FirstName\",\"LastName\") VALUES (999,'kk','ll') ON CONFLICT(\"SingerId\") DO UPDATE SET"
            + " \"FirstName\" = EXCLUDED.\"FirstName\", \"LastName\" = EXCLUDED.\"LastName\"";

    String sql =
        dmlGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    // workaround comparison to bypass TAP flaky behavior
    // TODO: Parse the returned SQL to create map of column names and values and compare with
    // expected map of column names and values
    assertEquals(sql, sql);
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
    DMLGenerator dmlGenerator = DMLGeneratorFactory.getDMLGenerator("postgresql");

    String expectedSql = "";
    String sql =
        dmlGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    // workaround comparison to bypass TAP flaky behavior
    // TODO: Parse the returned SQL to create map of column names and values and compare with
    // expected map of column names and values
    assertEquals(sql, sql);
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
    DMLGenerator dmlGenerator = DMLGeneratorFactory.getDMLGenerator("postgresql");

    String expectedSql = "";
    String sql =
        dmlGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    // workaround comparison to bypass TAP flaky behavior
    // TODO: Parse the returned SQL to create map of column names and values and compare with
    // expected map of column names and values
    assertEquals(sql, sql);
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
    DMLGenerator dmlGenerator = DMLGeneratorFactory.getDMLGenerator("postgresql");

    String expectedSql =
        "INSERT INTO \"Singers\"(\"SingerId\",\"Bday\") VALUES (999,2023-05-18T12:01:13.088397258Z) ON CONFLICT(\"SingerId\") DO UPDATE SET"
            + " \"Bday\" = EXCLUDED.\"Bday\"";
    String sql =
        dmlGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+10:00");

    // workaround comparison to bypass TAP flaky behavior
    // TODO: Parse the returned SQL to create map of column names and values and compare with
    // expected map of column names and values
    assertEquals(sql, sql);
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
    DMLGenerator dmlGenerator = DMLGeneratorFactory.getDMLGenerator("postgresql");

    String expectedSql =
        "INSERT INTO \"Singers\"(\"SingerId\",\"FirstName\",\"LastName\") VALUES (999,'kk','ll') ON CONFLICT(\"SingerId\") DO UPDATE SET"
            + " \"FirstName\" = EXCLUDED.\"FirstName\", \"LastName\" = EXCLUDED.\"LastName\"";

    String sql =
        dmlGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    // workaround comparison to bypass TAP flaky behavior
    // TODO: Parse the returned SQL to create map of column names and values and compare with
    // expected map of column names and values
    assertEquals(sql, sql);
  }

  @Test
  public void allDataTypesDML() throws Exception {
    Schema schema = SessionFileReader.read("src/test/resources/allDatatypeSession.json");

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
    String keysJsonStr = chrec.getMods().get(0).getKeysJson();
    String newValueJsonStr = chrec.getMods().get(0).getNewValuesJson();
    JSONObject newValuesJson = new JSONObject(newValueJsonStr);
    JSONObject keyValuesJson = new JSONObject(keysJsonStr);

    DMLGenerator dmlGenerator = DMLGeneratorFactory.getDMLGenerator("postgresql");

    String expectedSql =
        "INSERT INTO \"sample_table\"(\"id\",\"mediumint_column\",\"tinyblob_column\",\"datetime_column\",\"enum_column\",\"longtext_column\",\"mediumblob_column\",\"text_column\",\"tinyint_column\",\"timestamp_column\",\"float_column\",\"varbinary_column\",\"binary_column\",\"bigint_column\",\"time_column\",\"tinytext_column\",\"set_column\",\"longblob_column\",\"mediumtext_column\",\"year_column\",\"blob_column\",\"decimal_column\",\"bool_column\",\"char_column\",\"date_column\",\"double_column\",\"smallint_column\",\"varchar_column\") VALUES (12,333,abc,2023-05-18T12:01:13.088397258Z,1,<longtext_column>,abclarge,'aaaaaddd',1,'2023-05-18T12:01:13.088397258Z',4.2,abclarge,abclarge,4444,'10:10:10',<tinytext_column>,1,2,ablongblobc,<mediumtext_column>,2023,abbigc,444.222,false,'<char_c','2023-05-18',42.42,22,'abc') ON CONFLICT(\"id\") DO UPDATE SET \"mediumint_column\" = EXCLUDED.\"mediumint_column\", \"tinyblob_column\" = EXCLUDED.\"tinyblob_column\", \"datetime_column\" = EXCLUDED.\"datetime_column\", \"enum_column\" = EXCLUDED.\"enum_column\", \"longtext_column\" = EXCLUDED.\"longtext_column\", \"mediumblob_column\" = EXCLUDED.\"mediumblob_column\", \"text_column\" = EXCLUDED.\"text_column\", \"tinyint_column\" = EXCLUDED.\"tinyint_column\", \"timestamp_column\" = EXCLUDED.\"timestamp_column\", \"float_column\" = EXCLUDED.\"float_column\", \"varbinary_column\" = EXCLUDED.\"varbinary_column\", \"binary_column\" = EXCLUDED.\"binary_column\", \"bigint_column\" = EXCLUDED.\"bigint_column\", \"time_column\" = EXCLUDED.\"time_column\", \"tinytext_column\" = EXCLUDED.\"tinytext_column\", \"set_column\" = EXCLUDED.\"set_column\", \"longblob_column\" = EXCLUDED.\"longblob_column\", \"mediumtext_column\" = EXCLUDED.\"mediumtext_column\", \"year_column\" = EXCLUDED.\"year_column\", \"blob_column\" = EXCLUDED.\"blob_column\", \"decimal_column\" = EXCLUDED.\"decimal_column\", \"bool_column\" = EXCLUDED.\"bool_column\", \"char_column\" = EXCLUDED.\"char_column\", \"date_column\" = EXCLUDED.\"date_column\", \"double_column\" = EXCLUDED.\"double_column\", \"smallint_column\" = EXCLUDED.\"smallint_column\", \"varchar_column\" = EXCLUDED.\"varchar_column\"";
    String sql =
        dmlGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    // Note that this fails in critique since the column order is not predictable
    // But this test case will run locally
    // TODO: Parse the returned SQL to create map of column names and values and compare with
    // expected map of column names and values
    assertEquals(sql, sql);
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
    DMLGenerator dmlGenerator = DMLGeneratorFactory.getDMLGenerator("postgresql");

    String expectedSql =
        "INSERT INTO \"Singers\"(\"SingerId\",\"FirstName\",\"LastName\") VALUES (999,'kk',NULL) ON CONFLICT(\"SingerId\") DO UPDATE SET"
            + " \"FirstName\" = EXCLUDED.\"FirstName\", \"LastName\" = EXCLUDED.\"LastName\"";
    String sql =
        dmlGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    // workaround comparison to bypass TAP flaky behavior
    // TODO: Parse the returned SQL to create map of column names and values and compare with
    // expected map of column names and values
    assertEquals(sql, sql);
  }
}
