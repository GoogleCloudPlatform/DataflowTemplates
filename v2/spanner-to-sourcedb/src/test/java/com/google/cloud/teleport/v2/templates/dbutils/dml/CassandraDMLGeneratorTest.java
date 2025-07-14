/*
 * Copyright (C) 2025 Google LLC
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ColumnPK;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NameAndCols;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SyntheticPKey;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.templates.dbutils.processor.InputRecordProcessor;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementGeneratedResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementValueObject;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CassandraDMLGeneratorTest {

  private CassandraDMLGenerator cassandraDMLGenerator;

  @Before
  public void setUp() {
    cassandraDMLGenerator = new CassandraDMLGenerator();
  }

  @Test
  public void testGetDMLStatement_NullRequest() {
    DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(null);
    assertNotNull(response);
    assertEquals("", response.getDmlStatement());
  }

  @Test
  public void testGetDMLStatement_InvalidSchema() {
    DMLGeneratorRequest dmlGeneratorRequest =
        new DMLGeneratorRequest.Builder("insert", "text", null, null, null).setSchema(null).build();

    DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(dmlGeneratorRequest);
    assertNotNull(response);
    assertEquals("", response.getDmlStatement());
  }

  @Test
  public void testGetDMLStatement_MissingTableMapping() {
    Schema schema = new Schema();
    schema.setSpannerToID(null);
    DMLGeneratorRequest dmlGeneratorRequest =
        new DMLGeneratorRequest.Builder("insert", "text", null, null, null)
            .setSchema(schema)
            .build();
    DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(dmlGeneratorRequest);
    assertNotNull(response);
    assertEquals("", response.getDmlStatement());
  }

  @Test
  public void tableAndAllColumnNameTypesMatch() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"LastName\":\"ll\"}";
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("SingerId"));
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        999,
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertEquals(
        "ll",
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value()));
  }

  @Test
  public void tableAndAllColumnNameTypesForNullValueMatch() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
    String tableName = "sample_table";
    String newValueStr = "{\"date_column\":null}";
    JSONObject newValuesJson = new JSONObject(newValueStr);
    String keyValueString = "{\"id\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("id"));
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        "999",
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertTrue(values.get(1).value() instanceof CassandraTypeHandler.NullClass);
  }

  @Test
  public void tableAndAllColumnNameTypesForCustomTransformation() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"Bday\":\"1995-12-12\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";
    Map<String, Object> customTransformation = new HashMap<>();
    customTransformation.put("SingerId", "1000");
    customTransformation.put("LastName", "kk ll");
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .setCommitTimestamp(Timestamp.now())
                .setCustomTransformationResponse(customTransformation)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("LastName"));
    assertEquals(4, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        1000,
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertEquals(
        "kk ll",
        CassandraTypeHandler.castToExpectedType(values.get(2).dataType(), values.get(2).value()));
    assertEquals(
        Instant.parse("1995-12-12T00:00:00Z"),
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value()));
  }

  @Test
  public void tableNameMatchSourceColumnNotPresentInSpanner() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("LastName"));
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        999,
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertEquals(
        "ll",
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value()));
  }

  @Test
  public void tableNameMatchSpannerColumnNotPresentInSource() {

    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("LastName"));
    assertEquals(4, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        999,
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertEquals(
        "ll",
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value()));
    assertEquals(
        "shardA",
        CassandraTypeHandler.castToExpectedType(values.get(2).dataType(), values.get(2).value()));
  }

  @Test
  public void primaryKeyNotFoundInJson() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void primaryKeyNotPresentInSourceSchema() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"musicId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void primaryKeyMismatch() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("SingerId"));
    assertTrue(sql.contains("LastName"));
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        999,
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertEquals(
        "ll",
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value()));
  }

  @Test
  public void deleteMultiplePKColumns() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    assertEquals(2, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        999,
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value()));
  }

  @Test
  public void testSingleQuoteMatch() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("LastName"));
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        999,
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertEquals(
        "ll",
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value()));
  }

  @Test
  public void singleQuoteBytesDML() throws Exception {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Jw\u003d\u003d\",\"string_column\":\"\u0027\",}";
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        "12",
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertTrue(
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value())
            instanceof ByteBuffer);
  }

  @Test
  public void testParseBlobType_hexString() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"0102030405\",\"string_column\":\"\u0027\",}";
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        "12",
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertTrue(
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value())
            instanceof ByteBuffer);
  }

  @Test
  public void testParseBlobType_base64String() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"AQIDBAU=\",\"string_column\":\"\u0027\",}";
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        "12",
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertTrue(
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value())
            instanceof ByteBuffer);
  }

  @Test
  public void twoSingleEscapedQuoteDML() throws Exception {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Jyc\u003d\",\"string_column\":\"\u0027\u0027\",}";
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    assertTrue(sql.contains("sample_table"));
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        "12",
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertTrue(
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value())
            instanceof ByteBuffer);
  }

  @Test
  public void threeEscapesAndSingleQuoteDML() throws Exception {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"XCc\u003d\",\"string_column\":\"\\\\\\\u0027\",}";
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("sample_table"));
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        "12",
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertTrue(
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value())
            instanceof ByteBuffer);
  }

  @Test
  public void tabEscapeDML() throws Exception {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"CQ==\",\"string_column\":\"\\t\",}";
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("sample_table"));
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        "12",
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertTrue(
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value())
            instanceof ByteBuffer);
  }

  @Test
  public void backSpaceEscapeDML() throws Exception {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"CA==\",\"string_column\":\"\\b\",}";
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("sample_table"));
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        "12",
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertTrue(
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value())
            instanceof ByteBuffer);
  }

  @Test
  public void newLineEscapeDML() throws Exception {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");

    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Cg==\",\"string_column\":\"\\n\",}";
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("sample_table"));
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        "12",
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertTrue(
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value())
            instanceof ByteBuffer);
  }

  @Test
  public void bitColumnSql() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"YmlsX2NvbA\u003d\u003d\"}";
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("LastName"));
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        999,
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertEquals(
        "YmlsX2NvbA==",
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value()));
  }

  @Test
  public void testSpannerKeyIsNull() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("LastName"));
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(CassandraTypeHandler.NullClass.INSTANCE, values.get(0).value());
    assertEquals(
        "ll",
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value()));
  }

  @Test
  public void testSourcePKNotInSpanner() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
    String tableName = "customer";
    String newValuesString = "{\"Does\":\"not\",\"matter\":\"junk\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"Dont\":\"care\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "DELETE";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void primaryKeyMismatchSpannerNull() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"SingerId\":\"999\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"FirstName\":null}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("SingerId"));
    assertTrue(sql.contains("LastName"));
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        999,
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertEquals(
        "ll",
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value()));
  }

  @Test
  public void testUnsupportedModType() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "JUNK";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void testUpdateModType() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"SingerId\":\"999\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "UPDATE";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.contains("SingerId"));
    assertTrue(sql.contains("LastName"));
    assertEquals(3, ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues().size());
    List<PreparedStatementValueObject<?>> values =
        ((PreparedStatementGeneratedResponse) dmlGeneratorResponse).getValues();
    assertEquals(
        999,
        CassandraTypeHandler.castToExpectedType(values.get(0).dataType(), values.get(0).value()));
    assertEquals(
        "ll",
        CassandraTypeHandler.castToExpectedType(values.get(1).dataType(), values.get(1).value()));
  }

  @Test
  public void testSpannerTableIdMismatch() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
    String tableName = "Singers";
    String newValuesString = "{\"Does\":\"not\",\"matter\":\"junk\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"Dont\":\"care\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "DELETE";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    assertTrue(sql.isEmpty());
  }

  @Test
  public void testSourcePkNull() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
    String tableName = "Persons";
    String newValuesString = "{\"Does\":\"not\",\"matter\":\"junk\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    String keyValueString = "{\"Dont\":\"care\"}";
    JSONObject keyValuesJson = new JSONObject(keyValueString);
    String modType = "INSERT";

    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setSchema(schema)
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void testSpannerTableNotInSchemaObject() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
    String tableName = "Singers";
    schema.getSpSchema().remove(schema.getSpannerToID().get(tableName).getName());
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
                .setCommitTimestamp(Timestamp.now())
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();

    assertTrue(sql.isEmpty());
  }

  @Test
  public void testSpannerColDefsNull() {
    Schema schema = SessionFileReader.read("src/test/resources/cassandraSession.json");
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
                .setCommitTimestamp(Timestamp.now())
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
