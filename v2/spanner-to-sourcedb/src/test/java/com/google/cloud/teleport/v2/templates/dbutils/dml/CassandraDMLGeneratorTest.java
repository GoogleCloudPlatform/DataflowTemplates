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
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SchemaUtils;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
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
  private static final String SESSION_FILE = "src/test/resources/cassandraSession.json";

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
        new DMLGeneratorRequest.Builder("insert", "text", null, null, null).build();
    DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(dmlGeneratorRequest);
    assertNotNull(response);
    assertEquals("", response.getDmlStatement());
  }

  @Test
  public void tableAndAllColumnNameTypesMatch() {
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "sample_table";
    String newValueStr = "{\"date_column\":null}";
    JSONObject newValuesJson = new JSONObject(newValueStr);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"999\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"Bday\":\"1995-12-12\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";
    Map<String, Object> customTransformation = new HashMap<>();
    customTransformation.put("SingerId", "1000");
    customTransformation.put("LastName", "kk ll");
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\",\"hb_shardId\":\"shardA\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SomeRandomName\":\"999\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    assertTrue(sql.isEmpty());
  }

  @Test
  public void primaryKeyNotPresentInSourceSchema() {
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"musicId\":\"999\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    assertTrue(sql.isEmpty());
  }

  @Test
  public void primaryKeyMismatch() {
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"SingerId\":\"999\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"FirstName\":\"kk\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"LastName\":null}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\",\"FirstName\":\"kk\"}");
    String modType = "DELETE";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"k\u0027k\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Jw\u003d\u003d\",\"string_column\":\"\u0027\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"0102030405\",\"string_column\":\"\u0027\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"AQIDBAU=\",\"string_column\":\"\u0027\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Jyc\u003d\",\"string_column\":\"\u0027\u0027\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"XCc\u003d\",\"string_column\":\"\\\\\\\u0027\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"CQ==\",\"string_column\":\"\\t\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"CA==\",\"string_column\":\"\\b\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "sample_table";
    String newValuesString = "{\"blob_column\":\"Cg==\",\"string_column\":\"\\n\",}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"id\":\"12\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"YmlsX2NvbA\u003d\u003d\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":null}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "customer";
    String newValuesString = "{\"Does\":\"not\",\"matter\":\"junk\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"Dont\":\"care\"}");
    String modType = "DELETE";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    assertTrue(sql.isEmpty());
  }

  @Test
  public void primaryKeyMismatchSpannerNull() {
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"SingerId\":\"999\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"FirstName\":null}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "JUNK";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    assertTrue(sql.isEmpty());
  }

  @Test
  public void testUpdateModType() {
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "UPDATE";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
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
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Random";
    String newValuesString = "{\"Does\":\"not\",\"matter\":\"junk\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"Dont\":\"care\"}");
    String modType = "DELETE";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    assertTrue(sql.isEmpty());
  }

  @Test
  public void testSourcePkNull() {
    Ddl ddl = SchemaUtils.buildDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Persons";
    String newValuesString = "{\"Does\":\"not\",\"matter\":\"junk\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"Dont\":\"care\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    DMLGeneratorResponse dmlGeneratorResponse =
        cassandraDMLGenerator.getDMLStatement(
            new DMLGeneratorRequest.Builder(
                    modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
                .setSchemaMapper(schemaMapper)
                .build());
    String sql = dmlGeneratorResponse.getDmlStatement();
    assertTrue(sql.isEmpty());
  }
}
