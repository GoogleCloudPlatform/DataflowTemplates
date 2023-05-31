/*
 * Copyright (C) 2023 Google LLC
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

import com.google.cloud.teleport.v2.templates.schema.Schema;
import com.google.cloud.teleport.v2.templates.utils.InputFileReader;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class DMLGeneratorTest {

  @Test
  public void tableAndAllColumnNameTypesMatch() {
    Schema schema = InputFileReader.getSchema("src/test/resources/allMatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    String expectedSql =
        "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
            + " UPDATE  FirstName = 'kk', LastName = 'll'";
    String sql =
        DMLGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    assertEquals(sql, expectedSql);
  }

  @Test
  public void tableNameMismatchAllColumnNameTypesMatch() {
    Schema schema = InputFileReader.getSchema("src/test/resources/tableNameMismatchSession.json");
    String tableName = "leChanteur";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    String expectedSql =
        "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
            + " UPDATE  FirstName = 'kk', LastName = 'll'";
    String sql =
        DMLGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    assertEquals(sql, expectedSql);
  }

  @Test
  public void tableNameMatchColumnNameTypeMismatch() {
    Schema schema =
        InputFileReader.getSchema("src/test/resources/coulmnNameTypeMismatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"222\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    String expectedSql =
        "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES ('999',222,'ll') ON DUPLICATE"
            + " KEY UPDATE  FirstName = 222, LastName = 'll'";
    String sql =
        DMLGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    assertEquals(sql, expectedSql);
  }

  @Test
  public void tableNameMatchSourceColumnNotPresentInSpanner() {
    Schema schema =
        InputFileReader.getSchema("src/test/resources/sourceColumnAbsentInSpannerSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    String expectedSql =
        "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
            + " UPDATE  FirstName = 'kk', LastName = 'll'";
    String sql =
        DMLGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    assertEquals(sql, expectedSql);
  }

  @Test
  public void tableNameMatchSpannerColumnNotPresentInSource() {

    Schema schema =
        InputFileReader.getSchema("src/test/resources/spannerColumnAbsentInSourceSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\",\"hb_shardId\":\"shardA\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    String expectedSql =
        "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
            + " UPDATE  FirstName = 'kk', LastName = 'll'";
    String sql =
        DMLGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    assertEquals(sql, expectedSql);
  }

  @Test
  public void primaryKeyNotFoundInJson() {
    Schema schema = InputFileReader.getSchema("src/test/resources/allMatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SomeRandomName\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    String expectedSql = "";
    String sql =
        DMLGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    assertEquals(sql, expectedSql);
  }

  @Test
  public void primaryKeyNotPresentInSourceSchema() {
    Schema schema = InputFileReader.getSchema("src/test/resources/sourceNoPkSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    String expectedSql = "";
    String sql =
        sql =
            DMLGenerator.getDMLStatement(
                modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    assertEquals(sql, expectedSql);
  }

  @Test
  public void timezoneOffsetMismatch() {
    Schema schema = InputFileReader.getSchema("src/test/resources/timeZoneSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"Bday\":\"2023-05-18T12:01:13.088397258Z\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    String expectedSql =
        "INSERT INTO Singers(SingerId,Bday) VALUES (999,"
            + " CONVERT_TZ('2023-05-18T12:01:13.088397258','+00:00','+10:00')) ON DUPLICATE KEY"
            + " UPDATE  Bday =  CONVERT_TZ('2023-05-18T12:01:13.088397258','+00:00','+10:00')";
    String sql =
        DMLGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+10:00");

    assertEquals(sql, expectedSql);
  }

  @Test
  public void primaryKeyMismatch() {
    Schema schema = InputFileReader.getSchema("src/test/resources/primarykeyMismatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"SingerId\":\"999\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"FirstName\":\"kk\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    String expectedSql =
        "INSERT INTO Singers(SingerId,FirstName,LastName) VALUES (999,'kk','ll') ON DUPLICATE KEY"
            + " UPDATE  FirstName = 'kk', LastName = 'll'";
    String sql =
        DMLGenerator.getDMLStatement(
            modType, tableName, schema, newValuesJson, keyValuesJson, "+00:00");

    assertEquals(sql, expectedSql);
  }
}
