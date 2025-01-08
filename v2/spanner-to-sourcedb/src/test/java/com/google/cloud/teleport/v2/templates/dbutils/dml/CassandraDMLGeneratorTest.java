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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.spanner.migrations.schema.*;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.templates.dbutils.processor.InputRecordProcessor;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementGeneratedResponse;
import java.util.*;
import org.json.JSONObject;
import org.junit.Test;

public class CassandraDMLGeneratorTest {

  @Test
  public void tableAndAllColumnNameTypesMatch() {
    Schema schema =
        SessionFileReader.read(
            "src/test/resources/CassandraJson/cassandraPrimarykeyMismatchSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("SingerId"));
    assertEquals(4, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
  }

  @Test
  public void tableNameMismatchAllColumnNameTypesMatch() {
    Schema schema =
        SessionFileReader.read(
            "src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");
    String tableName = "leChanteur";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("SingerId"));
    assertTrue(sql.contains("LastName"));
    assertEquals(4, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
  }

  @Test
  public void tableNameMatchColumnNameTypeMismatch() {
    Schema schema =
        SessionFileReader.read(
            "src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"John\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("FirstName"));
    assertTrue(sql.contains("LastName"));
    assertEquals(4, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
  }

  @Test
  public void tableNameMatchSourceColumnNotPresentInSpanner() {
    Schema schema =
        SessionFileReader.read(
            "src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("FirstName"));
    assertTrue(sql.contains("LastName"));
    assertEquals(4, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
  }

  @Test
  public void tableNameMatchSpannerColumnNotPresentInSource() {

    Schema schema =
        SessionFileReader.read(
            "src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\",\"hb_shardId\":\"shardA\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("FirstName"));
    assertTrue(sql.contains("LastName"));
    assertEquals(4, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
  }

  @Test
  public void primaryKeyNotFoundInJson() {
    Schema schema =
        SessionFileReader.read("src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SomeRandomName\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void primaryKeyNotPresentInSourceSchema() {
    Schema schema =
        SessionFileReader.read("src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void primaryKeyMismatch() {
    Schema schema =
        SessionFileReader.read(
            "src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"SingerId\":\"999\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"FirstName\":\"kk\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("SingerId"));
    assertTrue(sql.contains("LastName"));
    assertEquals(4, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
  }

  @Test
  public void updateToNull() {
    Schema schema =
        SessionFileReader.read("src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("SingerId"));
    assertTrue(sql.contains("FirstName"));
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
  }

  @Test
  public void deleteMultiplePKColumns() {
    Schema schema =
        SessionFileReader.read("src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"LastName\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\",\"FirstName\":\"kk\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "DELETE";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    assertEquals(1, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
  }

  @Test
  public void testSingleQuoteMatch() {
    Schema schema =
        SessionFileReader.read("src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"k\u0027k\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("FirstName"));
    assertTrue(sql.contains("LastName"));
    assertEquals(4, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
  }

  @Test
  public void singleQuoteBytesDML() throws Exception {
    Schema schema =
        SessionFileReader.read("src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Jw\u003d\u003d\",\"varchar_column\":\"\u0027\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"id\":\"12\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    assertEquals(2, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
  }

  @Test
  public void twoSingleEscapedQuoteDML() throws Exception {
    Schema schema =
        SessionFileReader.read("src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Jyc\u003d\",\"varchar_column\":\"\u0027\u0027\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"id\":\"12\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    assertTrue(sql.contains("sample_table"));
    assertEquals(2, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
  }

  @Test
  public void threeEscapesAndSingleQuoteDML() throws Exception {
    Schema schema =
        SessionFileReader.read("src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"XCc\u003d\",\"varchar_column\":\"\\\\\\\u0027\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"id\":\"12\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("sample_table"));
    assertEquals(2, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
  }

  @Test
  public void tabEscapeDML() throws Exception {
    Schema schema =
        SessionFileReader.read("src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"CQ==\",\"varchar_column\":\"\\t\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"id\":\"12\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("sample_table"));
    assertEquals(2, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
  }

  @Test
  public void backSpaceEscapeDML() throws Exception {
    Schema schema =
        SessionFileReader.read("src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"CA==\",\"varchar_column\":\"\\b\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"id\":\"12\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("sample_table"));
    assertEquals(2, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
  }

  @Test
  public void newLineEscapeDML() throws Exception {
    Schema schema =
        SessionFileReader.read("src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Cg==\",\"varchar_column\":\"\\n\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"id\":\"12\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("sample_table"));
    assertEquals(2, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
  }

  @Test
  public void testSpannerTableNotInSchema() {
    Schema schema =
        SessionFileReader.read("src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");
    String tableName = "SomeRandomTableNotInSchema";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void testSpannerKeyIsNull() {
    Schema schema =
        SessionFileReader.read("src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":null}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("FirstName"));
    assertTrue(sql.contains("LastName"));
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
  }

  @Test
  public void testKeyInNewValuesJson() {
    Schema schema =
        SessionFileReader.read("src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\",\"SingerId\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SmthingElse\":null}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("FirstName"));
    assertTrue(sql.contains("LastName"));
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
  }

  @Test
  public void testSpannerColDefsNull() {
    Schema schema =
        SessionFileReader.read("src/test/resources/CassandraJson/cassandraAllDatatypeSession.json");
    String tableName = "Singers";

    String spannerTableId = schema.getSpannerToID().get(tableName).getName();
    SpannerTable spannerTable = schema.getSpSchema().get(spannerTableId);
    spannerTable.getColDefs().remove("c5");
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"23\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    CassandraDMLGenerator test = new CassandraDMLGenerator();
    InputRecordProcessor test2 = new InputRecordProcessor();
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
