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
package com.google.cloud.teleport.v2.templates.source.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementGeneratedResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementValueObject;
import com.google.cloud.teleport.v2.templates.utils.SchemaUtils;
import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
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
    assertThrows(
        InvalidDMLGenerationException.class, () -> cassandraDMLGenerator.getDMLStatement(null));
  }

  @Test
  public void testGetDMLStatement_InvalidSchema() {
    DMLGeneratorRequest dmlGeneratorRequest =
        new DMLGeneratorRequest.Builder("insert", "text", null, null, null).build();

    assertThrows(
        InvalidDMLGenerationException.class,
        () -> cassandraDMLGenerator.getDMLStatement(dmlGeneratorRequest));
  }

  @Test
  public void tableAndAllColumnNameTypesMatch() {
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SomeRandomName\":\"999\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            cassandraDMLGenerator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setCommitTimestamp(Timestamp.now())
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void primaryKeyNotPresentInSourceSchema() {
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"musicId\":\"999\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            cassandraDMLGenerator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setCommitTimestamp(Timestamp.now())
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void primaryKeyMismatch() {
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "customer";
    String newValuesString = "{\"Does\":\"not\",\"matter\":\"junk\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"Dont\":\"care\"}");
    String modType = "DELETE";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            cassandraDMLGenerator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setCommitTimestamp(Timestamp.now())
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void primaryKeyMismatchSpannerNull() {
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Singers";
    String newValuesString = "{\"FirstName\":\"kk\",\"LastName\":\"ll\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"SingerId\":\"999\"}");
    String modType = "JUNK";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            cassandraDMLGenerator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setCommitTimestamp(Timestamp.now())
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void testUpdateModType() {
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
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
                .setSchemaMapper(schemaMapper)
                .setCommitTimestamp(Timestamp.now())
                .setDdl(ddl)
                .setSourceSchema(sourceSchema)
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
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Random";
    String newValuesString = "{\"Does\":\"not\",\"matter\":\"junk\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"Dont\":\"care\"}");
    String modType = "DELETE";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            cassandraDMLGenerator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setCommitTimestamp(Timestamp.now())
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void testSourcePkNull() {
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE, ddl);
    String tableName = "Persons";
    String newValuesString = "{\"Does\":\"not\",\"matter\":\"junk\"}";
    JSONObject newValuesJson = new JSONObject(newValuesString);
    JSONObject keyValuesJson = new JSONObject("{\"Dont\":\"care\"}");
    String modType = "INSERT";
    CassandraDMLGenerator cassandraDMLGenerator = new CassandraDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            cassandraDMLGenerator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        modType, tableName, newValuesJson, keyValuesJson, "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setCommitTimestamp(Timestamp.now())
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void testGetDeleteStatementCQL_NullTimestamp() {
    Map<String, PreparedStatementValueObject<?>> pkValues =
        Map.of("id", PreparedStatementValueObject.create("int", 1));
    DMLGeneratorResponse response =
        CassandraDMLGenerator.getDeleteStatementCQL("my_table", null, pkValues);

    assertEquals("DELETE FROM \"my_table\" WHERE \"id\" = ?", response.getDmlStatement());
  }

  @Test
  public void testGetDeleteStatementCQL_WithTimestamp() {
    Map<String, PreparedStatementValueObject<?>> pkValues =
        Map.of("id", PreparedStatementValueObject.create("int", 1));
    java.sql.Timestamp timestamp = new java.sql.Timestamp(1000L);
    DMLGeneratorResponse response =
        CassandraDMLGenerator.getDeleteStatementCQL("my_table", timestamp, pkValues);

    assertEquals(
        "DELETE FROM \"my_table\" USING TIMESTAMP ? WHERE \"id\" = ?", response.getDmlStatement());

    assertTrue(response instanceof PreparedStatementGeneratedResponse);
    PreparedStatementGeneratedResponse responseObj = (PreparedStatementGeneratedResponse) response;
    assertEquals(2, responseObj.getValues().size());
    assertEquals("USING_TIMESTAMP", responseObj.getValues().get(0).dataType());
    assertEquals(1000L, responseObj.getValues().get(0).value());
    assertEquals("int", responseObj.getValues().get(1).dataType());
    assertEquals(1, responseObj.getValues().get(1).value());
  }

  @Test
  public void testGetColumnValues_MissingSpannerColumnMapping() throws Exception {
    ISchemaMapper mockSchemaMapper = Mockito.mock(ISchemaMapper.class);
    Table spannerTable = Mockito.mock(Table.class);
    SourceTable sourceTable = Mockito.mock(SourceTable.class);
    SourceColumn sourceCol = Mockito.mock(SourceColumn.class);

    Mockito.when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of());
    Mockito.when(sourceTable.columns()).thenReturn(ImmutableList.of(sourceCol));
    Mockito.when(sourceCol.name()).thenReturn("col1");
    Mockito.when(mockSchemaMapper.getSpannerColumnName("", "my_table", "col1"))
        .thenThrow(new NoSuchElementException());
    Mockito.when(sourceTable.name()).thenReturn("my_table");

    Map<String, PreparedStatementValueObject<?>> result =
        CassandraDMLGenerator.getColumnValues(
            mockSchemaMapper,
            spannerTable,
            sourceTable,
            new JSONObject(),
            new JSONObject(),
            "+00:00",
            null);

    assertTrue(result.isEmpty());
  }

  @Test
  public void testGetPkColumnValues_MissingSourceColumnDefinition() {
    ISchemaMapper schemaMapper = Mockito.mock(ISchemaMapper.class);
    Table spannerTable = Mockito.mock(Table.class);
    SourceTable sourceTable = Mockito.mock(SourceTable.class);

    Mockito.when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of("id"));
    Mockito.when(sourceTable.column("id")).thenReturn(null); // Missing!

    Map<String, PreparedStatementValueObject<?>> result =
        CassandraDMLGenerator.getPkColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            new JSONObject(),
            new JSONObject(),
            "+00:00",
            null);

    assertNull(result);
  }

  @Test
  public void testGetDMLStatement_NullSpannerDdl() {
    CassandraDMLGenerator generator = new CassandraDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            generator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "tableName", new JSONObject(), new JSONObject(), "+00:00")
                    .setSchemaMapper(Mockito.mock(ISchemaMapper.class))
                    .setSourceSchema(Mockito.mock(SourceSchema.class))
                    .setDdl(null)
                    .build()));
  }

  @Test
  public void testGetDMLStatement_NullSourceSchema() {
    CassandraDMLGenerator generator = new CassandraDMLGenerator();
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            generator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "tableName", new JSONObject(), new JSONObject(), "+00:00")
                    .setSchemaMapper(Mockito.mock(ISchemaMapper.class))
                    .setDdl(Mockito.mock(Ddl.class))
                    .setSourceSchema(null)
                    .build()));
  }

  @Test
  public void testGetDMLStatement_NullSpannerTable() {
    CassandraDMLGenerator generator = new CassandraDMLGenerator();
    Ddl ddl = Mockito.mock(Ddl.class);
    Mockito.when(ddl.table("tableName")).thenReturn(null);

    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            generator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "tableName", new JSONObject(), new JSONObject(), "+00:00")
                    .setSchemaMapper(Mockito.mock(ISchemaMapper.class))
                    .setDdl(ddl)
                    .setSourceSchema(Mockito.mock(SourceSchema.class))
                    .build()));
  }

  @Test
  public void testGetDMLStatement_NullSourceTable() {
    CassandraDMLGenerator generator = new CassandraDMLGenerator();
    Ddl ddl = Mockito.mock(Ddl.class);
    Table table = Mockito.mock(Table.class);
    Mockito.when(ddl.table("tableName")).thenReturn(table);

    SourceSchema sourceSchema = Mockito.mock(SourceSchema.class);
    Mockito.when(sourceSchema.table(Mockito.anyString())).thenReturn(null);

    ISchemaMapper schemaMapper = Mockito.mock(ISchemaMapper.class);
    Mockito.when(schemaMapper.getSourceTableName(Mockito.anyString(), Mockito.anyString()))
        .thenReturn("src_table");

    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            generator.getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "tableName", new JSONObject(), new JSONObject(), "+00:00")
                    .setSchemaMapper(schemaMapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build()));
  }

  @Test
  public void testGetColumnValues_CustomTransformation() {
    ISchemaMapper schemaMapper = Mockito.mock(ISchemaMapper.class);
    Table spannerTable = Mockito.mock(Table.class);
    SourceTable sourceTable = Mockito.mock(SourceTable.class);
    SourceColumn sourceCol = Mockito.mock(SourceColumn.class);

    Mockito.when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of());
    Mockito.when(sourceTable.columns()).thenReturn(ImmutableList.of(sourceCol));
    Mockito.when(sourceCol.name()).thenReturn("col1");
    Mockito.when(sourceCol.type()).thenReturn("text");

    Map<String, Object> customResponse = Map.of("col1", "custom_val");

    Map<String, PreparedStatementValueObject<?>> response =
        CassandraDMLGenerator.getColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            new JSONObject(),
            new JSONObject(),
            "+00:00",
            customResponse);

    assertNotNull(response);
    assertEquals(1, response.size());
    assertEquals("custom_val", response.get("col1").value());
    assertEquals("text", response.get("col1").dataType());
  }

  @Test
  public void testGetColumnValues() {
    ISchemaMapper schemaMapper = Mockito.mock(ISchemaMapper.class);
    Table spannerTable = Mockito.mock(Table.class);
    SourceTable sourceTable = Mockito.mock(SourceTable.class);
    SourceColumn sourceCol = Mockito.mock(SourceColumn.class);
    Column spannerCol = Mockito.mock(Column.class);

    Mockito.when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of());
    Mockito.when(sourceTable.columns()).thenReturn(ImmutableList.of(sourceCol));
    Mockito.when(sourceCol.name()).thenReturn("col1");
    Mockito.when(sourceCol.type()).thenReturn("text");

    Mockito.when(schemaMapper.getSpannerColumnName("", "src_table", "col1"))
        .thenReturn("spanner_col1");
    Mockito.when(sourceTable.name()).thenReturn("src_table");
    Mockito.when(spannerTable.column("spanner_col1")).thenReturn(spannerCol);

    Type spannerType = Type.string();
    Mockito.when(spannerCol.type()).thenReturn(spannerType);
    Mockito.when(spannerCol.name()).thenReturn("spanner_col1");

    JSONObject newValuesJson = new JSONObject();
    newValuesJson.put("spanner_col1", "val1");

    Map<String, PreparedStatementValueObject<?>> response =
        CassandraDMLGenerator.getColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            newValuesJson,
            new JSONObject(),
            "+00:00",
            null);

    assertNotNull(response);
    assertEquals(1, response.size());
    assertEquals("val1", response.get("col1").value());
    assertEquals("text", response.get("col1").dataType());
  }

  @Test
  public void testGetPkColumnValues() {
    ISchemaMapper schemaMapper = Mockito.mock(ISchemaMapper.class);
    Table spannerTable = Mockito.mock(Table.class);
    SourceTable sourceTable = Mockito.mock(SourceTable.class);
    SourceColumn sourceCol = Mockito.mock(SourceColumn.class);
    Column spannerCol = Mockito.mock(Column.class);

    Mockito.when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of("col1"));
    Mockito.when(sourceTable.column("col1")).thenReturn(sourceCol);
    Mockito.when(sourceCol.name()).thenReturn("col1");
    Mockito.when(sourceCol.type()).thenReturn("int");

    Mockito.when(schemaMapper.getSpannerColumnName("", "src_table", "col1"))
        .thenReturn("spanner_col1");
    Mockito.when(sourceTable.name()).thenReturn("src_table");
    Mockito.when(spannerTable.column("spanner_col1")).thenReturn(spannerCol);

    Type spannerType = Type.int64();
    Mockito.when(spannerCol.type()).thenReturn(spannerType);
    Mockito.when(spannerCol.name()).thenReturn("spanner_col1");

    JSONObject keyValuesJson = new JSONObject();
    keyValuesJson.put("spanner_col1", 123L);

    Map<String, PreparedStatementValueObject<?>> response =
        CassandraDMLGenerator.getPkColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            new JSONObject(),
            keyValuesJson,
            "+00:00",
            null);

    assertNotNull(response);
    assertEquals(1, response.size());
    assertEquals(123L, response.get("col1").value());
    assertEquals("int", response.get("col1").dataType());
  }

  @Test
  public void testGetColumnValues_SkipPK() {
    ISchemaMapper schemaMapper = Mockito.mock(ISchemaMapper.class);
    Table spannerTable = Mockito.mock(Table.class);
    SourceTable sourceTable = Mockito.mock(SourceTable.class);
    SourceColumn sourceCol = Mockito.mock(SourceColumn.class);

    Mockito.when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of("col1"));
    Mockito.when(sourceTable.columns()).thenReturn(ImmutableList.of(sourceCol));
    Mockito.when(sourceCol.name()).thenReturn("col1");

    Map<String, PreparedStatementValueObject<?>> response =
        CassandraDMLGenerator.getColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            new JSONObject(),
            new JSONObject(),
            "+00:00",
            null);

    assertNotNull(response);
    assertTrue(response.isEmpty());
  }

  @Test
  public void testGetColumnValues_MissingSpannerMapping() {
    ISchemaMapper schemaMapper = Mockito.mock(ISchemaMapper.class);
    Table spannerTable = Mockito.mock(Table.class);
    SourceTable sourceTable = Mockito.mock(SourceTable.class);
    SourceColumn sourceCol = Mockito.mock(SourceColumn.class);

    Mockito.when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of());
    Mockito.when(sourceTable.columns()).thenReturn(ImmutableList.of(sourceCol));
    Mockito.when(sourceCol.name()).thenReturn("col1");
    Mockito.when(sourceTable.name()).thenReturn("src_table");

    Mockito.when(schemaMapper.getSpannerColumnName("", "src_table", "col1"))
        .thenThrow(new NoSuchElementException());

    Map<String, PreparedStatementValueObject<?>> response =
        CassandraDMLGenerator.getColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            new JSONObject(),
            new JSONObject(),
            "+00:00",
            null);

    assertNotNull(response);
    assertTrue(response.isEmpty());
  }

  @Test
  public void testGetPkColumnValues_MissingValueInJson() {
    ISchemaMapper schemaMapper = Mockito.mock(ISchemaMapper.class);
    Table spannerTable = Mockito.mock(Table.class);
    SourceTable sourceTable = Mockito.mock(SourceTable.class);
    SourceColumn sourceCol = Mockito.mock(SourceColumn.class);
    Column spannerCol = Mockito.mock(Column.class);

    Mockito.when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of("col1"));
    Mockito.when(sourceTable.column("col1")).thenReturn(sourceCol);
    Mockito.when(sourceCol.name()).thenReturn("col1");

    Mockito.when(schemaMapper.getSpannerColumnName("", "src_table", "col1"))
        .thenReturn("spanner_col1");
    Mockito.when(sourceTable.name()).thenReturn("src_table");
    Mockito.when(spannerTable.column("spanner_col1")).thenReturn(spannerCol);

    Map<String, PreparedStatementValueObject<?>> response =
        CassandraDMLGenerator.getPkColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            new JSONObject(),
            new JSONObject(),
            "+00:00",
            null);

    assertNull(response);
  }

  @Test
  public void testGetDMLStatement_PkColSpannerColNameThrowsNoSuchElement() {
    ISchemaMapper schemaMapper = Mockito.mock(ISchemaMapper.class);
    Ddl spannerDdl = Ddl.builder().build();
    spannerDdl = Mockito.spy(spannerDdl);
    Table spannerTable = Mockito.mock(Table.class);
    SourceSchema sourceSchema = Mockito.mock(SourceSchema.class);
    SourceTable sourceTable = Mockito.mock(SourceTable.class);
    SourceColumn sourceCol = Mockito.mock(SourceColumn.class);

    Mockito.doReturn(spannerTable).when(spannerDdl).table("Singers");
    Mockito.when(schemaMapper.getSourceTableName("", "Singers")).thenReturn("Singers");
    Mockito.when(sourceSchema.table("Singers")).thenReturn(sourceTable);
    Mockito.when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of("SingerId"));
    Mockito.when(sourceTable.column("SingerId")).thenReturn(sourceCol);
    Mockito.when(sourceCol.type()).thenReturn("int");
    Mockito.when(sourceTable.name()).thenReturn("Singers");

    Mockito.when(schemaMapper.getSpannerColumnName("", "Singers", "SingerId"))
        .thenThrow(new NoSuchElementException("Not found"));

    DMLGeneratorRequest request =
        new DMLGeneratorRequest.Builder(
                "INSERT", "Singers", new JSONObject(), new JSONObject(), "+00:00")
            .setSchemaMapper(schemaMapper)
            .setDdl(spannerDdl)
            .setSourceSchema(sourceSchema)
            .setCommitTimestamp(Timestamp.now())
            .build();

    assertThrows(
        InvalidDMLGenerationException.class, () -> cassandraDMLGenerator.getDMLStatement(request));
  }

  @Test
  public void testGetDMLStatement_PkColSpannerColNameNull() {
    ISchemaMapper schemaMapper = Mockito.mock(ISchemaMapper.class);
    Ddl spannerDdl = Ddl.builder().build();
    spannerDdl = Mockito.spy(spannerDdl);
    Table spannerTable = Mockito.mock(Table.class);
    SourceSchema sourceSchema = Mockito.mock(SourceSchema.class);
    SourceTable sourceTable = Mockito.mock(SourceTable.class);
    SourceColumn sourceCol = Mockito.mock(SourceColumn.class);

    Mockito.doReturn(spannerTable).when(spannerDdl).table("Singers");
    Mockito.when(schemaMapper.getSourceTableName("", "Singers")).thenReturn("Singers");
    Mockito.when(sourceSchema.table("Singers")).thenReturn(sourceTable);
    Mockito.when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of("SingerId"));
    Mockito.when(sourceTable.column("SingerId")).thenReturn(sourceCol);
    Mockito.when(sourceCol.type()).thenReturn("int");
    Mockito.when(sourceTable.name()).thenReturn("Singers");

    Mockito.when(schemaMapper.getSpannerColumnName("", "Singers", "SingerId")).thenReturn(null);

    DMLGeneratorRequest request =
        new DMLGeneratorRequest.Builder(
                "INSERT", "Singers", new JSONObject(), new JSONObject(), "+00:00")
            .setSchemaMapper(schemaMapper)
            .setDdl(spannerDdl)
            .setSourceSchema(sourceSchema)
            .setCommitTimestamp(Timestamp.now())
            .build();

    assertThrows(
        InvalidDMLGenerationException.class, () -> cassandraDMLGenerator.getDMLStatement(request));
  }

  @Test
  public void testGetDMLStatement_PkColSpannerColDefNull() {
    ISchemaMapper schemaMapper = Mockito.mock(ISchemaMapper.class);
    Ddl spannerDdl = Ddl.builder().build();
    spannerDdl = Mockito.spy(spannerDdl);
    Table spannerTable = Mockito.mock(Table.class);
    SourceSchema sourceSchema = Mockito.mock(SourceSchema.class);
    SourceTable sourceTable = Mockito.mock(SourceTable.class);
    SourceColumn sourceCol = Mockito.mock(SourceColumn.class);

    Mockito.doReturn(spannerTable).when(spannerDdl).table("Singers");
    Mockito.when(schemaMapper.getSourceTableName("", "Singers")).thenReturn("Singers");
    Mockito.when(sourceSchema.table("Singers")).thenReturn(sourceTable);
    Mockito.when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of("SingerId"));
    Mockito.when(sourceTable.column("SingerId")).thenReturn(sourceCol);
    Mockito.when(sourceCol.type()).thenReturn("int");
    Mockito.when(sourceTable.name()).thenReturn("Singers");

    Mockito.when(schemaMapper.getSpannerColumnName("", "Singers", "SingerId"))
        .thenReturn("SingerId");
    Mockito.when(spannerTable.column("SingerId")).thenReturn(null);

    DMLGeneratorRequest request =
        new DMLGeneratorRequest.Builder(
                "INSERT", "Singers", new JSONObject(), new JSONObject(), "+00:00")
            .setSchemaMapper(schemaMapper)
            .setDdl(spannerDdl)
            .setSourceSchema(sourceSchema)
            .setCommitTimestamp(Timestamp.now())
            .build();

    assertThrows(
        InvalidDMLGenerationException.class, () -> cassandraDMLGenerator.getDMLStatement(request));
  }

  @Test
  public void testGetDMLStatement_ColValuesSpannerColDefNullAndKeyValuesHasNonPk() {
    ISchemaMapper schemaMapper = Mockito.mock(ISchemaMapper.class);
    Table spannerTable = Mockito.mock(Table.class);
    SourceSchema sourceSchema = Mockito.mock(SourceSchema.class);
    SourceTable sourceTable = Mockito.mock(SourceTable.class);
    SourceColumn pkCol = Mockito.mock(SourceColumn.class);
    SourceColumn nonPkCol = Mockito.mock(SourceColumn.class);
    SourceColumn missingCol = Mockito.mock(SourceColumn.class);
    Column spannerPkCol = Mockito.mock(Column.class);
    Column spannerNonPkCol = Mockito.mock(Column.class);

    Ddl spannerDdl = Ddl.builder().build();
    spannerDdl = Mockito.spy(spannerDdl);
    Mockito.doReturn(spannerTable).when(spannerDdl).table("Singers");

    Mockito.when(schemaMapper.getSourceTableName("", "Singers")).thenReturn("Singers");
    Mockito.when(sourceSchema.table("Singers")).thenReturn(sourceTable);
    Mockito.when(sourceTable.primaryKeyColumns()).thenReturn(ImmutableList.of("SingerId"));
    Mockito.when(sourceTable.column("SingerId")).thenReturn(pkCol);
    Mockito.when(pkCol.type()).thenReturn("int");
    Mockito.when(sourceTable.name()).thenReturn("Singers");

    Mockito.when(schemaMapper.getSpannerColumnName("", "Singers", "SingerId"))
        .thenReturn("SingerId");
    Mockito.when(spannerTable.column("SingerId")).thenReturn(spannerPkCol);
    Mockito.when(spannerPkCol.name()).thenReturn("SingerId");
    Mockito.when(spannerPkCol.type()).thenReturn(Type.int64());

    Mockito.when(sourceTable.columns()).thenReturn(ImmutableList.of(pkCol, nonPkCol, missingCol));
    Mockito.when(nonPkCol.name()).thenReturn("LastName");
    Mockito.when(nonPkCol.type()).thenReturn("varchar");
    Mockito.when(missingCol.name()).thenReturn("MissingCol");
    Mockito.when(missingCol.type()).thenReturn("varchar");

    Mockito.when(schemaMapper.getSpannerColumnName("", "Singers", "LastName"))
        .thenReturn("LastName");
    Mockito.when(spannerTable.column("LastName")).thenReturn(spannerNonPkCol);
    Mockito.when(spannerNonPkCol.name()).thenReturn("LastName");
    Mockito.when(spannerNonPkCol.type()).thenReturn(Type.string());

    Mockito.when(schemaMapper.getSpannerColumnName("", "Singers", "MissingCol"))
        .thenReturn("MissingCol");
    Mockito.when(spannerTable.column("MissingCol")).thenReturn(null);

    JSONObject keyValuesJson = new JSONObject();
    keyValuesJson.put("SingerId", "999");
    keyValuesJson.put("LastName", "Smith");

    DMLGeneratorRequest request =
        new DMLGeneratorRequest.Builder(
                "INSERT", "Singers", new JSONObject(), keyValuesJson, "+00:00")
            .setSchemaMapper(schemaMapper)
            .setDdl(spannerDdl)
            .setSourceSchema(sourceSchema)
            .setCommitTimestamp(Timestamp.now())
            .build();

    DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(request);

    assertTrue(response instanceof PreparedStatementGeneratedResponse);
    PreparedStatementGeneratedResponse prepResponse = (PreparedStatementGeneratedResponse) response;

    assertTrue(prepResponse.getDmlStatement().contains("\"LastName\""));
    assertFalse(prepResponse.getDmlStatement().contains("\"MissingCol\""));

    assertEquals(3, prepResponse.getValues().size());
  }

  @Test
  public void testGetDMLStatement_CompositePk_OnePkSpannerColDefNull() {
    ISchemaMapper schemaMapper = Mockito.mock(ISchemaMapper.class);
    Table spannerTable = Mockito.mock(Table.class);
    SourceSchema sourceSchema = Mockito.mock(SourceSchema.class);
    SourceTable sourceTable = Mockito.mock(SourceTable.class);
    SourceColumn pkCol1 = Mockito.mock(SourceColumn.class);
    SourceColumn pkCol2 = Mockito.mock(SourceColumn.class);
    SourceColumn nonPkCol = Mockito.mock(SourceColumn.class);
    Column spannerPkCol1 = Mockito.mock(Column.class);
    Column spannerNonPkCol = Mockito.mock(Column.class);

    Ddl spannerDdl = Ddl.builder().build();
    spannerDdl = Mockito.spy(spannerDdl);
    Mockito.doReturn(spannerTable).when(spannerDdl).table("Singers");

    Mockito.when(schemaMapper.getSourceTableName("", "Singers")).thenReturn("Singers");
    Mockito.when(sourceSchema.table("Singers")).thenReturn(sourceTable);

    // Composite PK
    Mockito.when(sourceTable.primaryKeyColumns())
        .thenReturn(ImmutableList.of("SingerId", "LastName"));
    Mockito.when(sourceTable.column("SingerId")).thenReturn(pkCol1);
    Mockito.when(pkCol1.type()).thenReturn("int");
    Mockito.when(sourceTable.column("LastName")).thenReturn(pkCol2);
    Mockito.when(pkCol2.type()).thenReturn("varchar");
    Mockito.when(sourceTable.name()).thenReturn("Singers");

    // SingerId (pkCol1) maps successfully
    Mockito.when(schemaMapper.getSpannerColumnName("", "Singers", "SingerId"))
        .thenReturn("SingerId");
    Mockito.when(spannerTable.column("SingerId")).thenReturn(spannerPkCol1);
    Mockito.when(spannerPkCol1.name()).thenReturn("SingerId");
    Mockito.when(spannerPkCol1.type()).thenReturn(Type.int64());

    // LastName (pkCol2) fails to map because Spanner column def is null
    Mockito.when(schemaMapper.getSpannerColumnName("", "Singers", "LastName"))
        .thenReturn("LastName");
    Mockito.when(spannerTable.column("LastName")).thenReturn(null);

    // Non-PK col (Age) maps successfully
    Mockito.when(sourceTable.columns()).thenReturn(ImmutableList.of(pkCol1, pkCol2, nonPkCol));
    Mockito.when(nonPkCol.name()).thenReturn("Age");
    Mockito.when(nonPkCol.type()).thenReturn("int");

    Mockito.when(schemaMapper.getSpannerColumnName("", "Singers", "Age")).thenReturn("Age");
    Mockito.when(spannerTable.column("Age")).thenReturn(spannerNonPkCol);
    Mockito.when(spannerNonPkCol.name()).thenReturn("Age");
    Mockito.when(spannerNonPkCol.type()).thenReturn(Type.int64());

    JSONObject keyValuesJson = new JSONObject();
    keyValuesJson.put("SingerId", 999L);
    keyValuesJson.put("LastName", "Smith");
    keyValuesJson.put("Age", 30L);

    DMLGeneratorRequest request =
        new DMLGeneratorRequest.Builder(
                "INSERT", "Singers", new JSONObject(), keyValuesJson, "+00:00")
            .setSchemaMapper(schemaMapper)
            .setDdl(spannerDdl)
            .setSourceSchema(sourceSchema)
            .setCommitTimestamp(Timestamp.now())
            .build();

    DMLGeneratorResponse response = cassandraDMLGenerator.getDMLStatement(request);

    assertTrue(response instanceof PreparedStatementGeneratedResponse);
    PreparedStatementGeneratedResponse prepResponse = (PreparedStatementGeneratedResponse) response;

    // The statement should contain SingerId and Age, but NOT LastName
    assertTrue(prepResponse.getDmlStatement().contains("\"SingerId\""));
    assertTrue(prepResponse.getDmlStatement().contains("\"Age\""));
    assertFalse(prepResponse.getDmlStatement().contains("\"LastName\""));

    // Expected values: SingerId (999), Age (30), and using_timestamp
    assertEquals(3, prepResponse.getValues().size());
  }
}
