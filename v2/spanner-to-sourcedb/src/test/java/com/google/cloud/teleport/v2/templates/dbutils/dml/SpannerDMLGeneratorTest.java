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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.SpannerMutationResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class SpannerDMLGeneratorTest {

  private static final SourceDatabaseType SRC_TYPE = SourceDatabaseType.SPANNER;

  /** Builds a simple Spanner DDL with one table: Singers(SingerId INT64 PK, FirstName STRING). */
  private static Ddl buildDdl() {
    Ddl.Builder builder = Ddl.builder();
    Table.Builder tableBuilder = builder.createTable("Singers");
    tableBuilder.column("SingerId").int64().notNull().endColumn();
    tableBuilder.column("FirstName").string().max().endColumn();
    tableBuilder.column("LastName").string().max().endColumn();
    tableBuilder.primaryKey().asc("SingerId").end();
    tableBuilder.endTable();
    return builder.build();
  }

  /** Builds a SourceSchema (target Spanner) mirroring the DDL above. */
  private static SourceSchema buildSourceSchema() {
    SourceColumn singerIdCol =
        SourceColumn.builder(SRC_TYPE)
            .name("SingerId")
            .type("INT64")
            .isPrimaryKey(true)
            .isNullable(false)
            .build();
    SourceColumn firstNameCol =
        SourceColumn.builder(SRC_TYPE).name("FirstName").type("STRING").isNullable(true).build();
    SourceColumn lastNameCol =
        SourceColumn.builder(SRC_TYPE).name("LastName").type("STRING").isNullable(true).build();

    SourceTable table =
        SourceTable.builder(SRC_TYPE)
            .name("Singers")
            .columns(ImmutableList.of(singerIdCol, firstNameCol, lastNameCol))
            .primaryKeyColumns(ImmutableList.of("SingerId"))
            .foreignKeys(ImmutableList.of())
            .indexes(ImmutableList.of())
            .build();

    return SourceSchema.builder(SRC_TYPE)
        .databaseName("test-db")
        .tables(ImmutableMap.of("Singers", table))
        .build();
  }

  /** Creates a schema mapper that maps Singers → Singers with identity column mapping. */
  private static ISchemaMapper buildIdentityMapper() throws Exception {
    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSourceTableName("", "Singers")).thenReturn("Singers");
    when(mapper.getSpannerColumnName("", "Singers", "SingerId")).thenReturn("SingerId");
    when(mapper.getSpannerColumnName("", "Singers", "FirstName")).thenReturn("FirstName");
    when(mapper.getSpannerColumnName("", "Singers", "LastName")).thenReturn("LastName");
    when(mapper.getSourceColumnName("", "Singers", "SingerId")).thenReturn("SingerId");
    when(mapper.getSourceColumnName("", "Singers", "FirstName")).thenReturn("FirstName");
    when(mapper.getSourceColumnName("", "Singers", "LastName")).thenReturn("LastName");
    return mapper;
  }

  @Test
  public void insertProducesInsertOrUpdateMutation() throws Exception {
    Ddl ddl = buildDdl();
    SourceSchema sourceSchema = buildSourceSchema();
    ISchemaMapper mapper = buildIdentityMapper();

    JSONObject newValues = new JSONObject("{\"FirstName\":\"John\",\"LastName\":\"Doe\"}");
    JSONObject keyValues = new JSONObject("{\"SingerId\":\"42\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "Singers", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build());

    assertNotNull(response);
    SpannerMutationResponse mutationResponse = (SpannerMutationResponse) response;
    Mutation mutation = mutationResponse.getMutation();
    assertEquals(Mutation.Op.INSERT_OR_UPDATE, mutation.getOperation());
    assertEquals("Singers", mutation.getTable());
  }

  @Test
  public void updateProducesInsertOrUpdateMutation() throws Exception {
    Ddl ddl = buildDdl();
    SourceSchema sourceSchema = buildSourceSchema();
    ISchemaMapper mapper = buildIdentityMapper();

    JSONObject newValues = new JSONObject("{\"FirstName\":\"Jane\",\"LastName\":\"Smith\"}");
    JSONObject keyValues = new JSONObject("{\"SingerId\":\"7\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("UPDATE", "Singers", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build());

    SpannerMutationResponse mutationResponse = (SpannerMutationResponse) response;
    assertEquals(Mutation.Op.INSERT_OR_UPDATE, mutationResponse.getMutation().getOperation());
  }

  @Test
  public void deleteProducesDeleteMutation() throws Exception {
    Ddl ddl = buildDdl();
    SourceSchema sourceSchema = buildSourceSchema();
    ISchemaMapper mapper = buildIdentityMapper();

    JSONObject newValues = new JSONObject("{}");
    JSONObject keyValues = new JSONObject("{\"SingerId\":\"99\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("DELETE", "Singers", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build());

    SpannerMutationResponse mutationResponse = (SpannerMutationResponse) response;
    assertEquals(Mutation.Op.DELETE, mutationResponse.getMutation().getOperation());
    assertEquals("Singers", mutationResponse.getMutation().getTable());
  }

  @Test
  public void nullNonPkColumnIsIncludedInMutation() throws Exception {
    Ddl ddl = buildDdl();
    SourceSchema sourceSchema = buildSourceSchema();
    ISchemaMapper mapper = buildIdentityMapper();

    JSONObject newValues = new JSONObject();
    newValues.put("FirstName", JSONObject.NULL);
    newValues.put("LastName", "Doe");
    JSONObject keyValues = new JSONObject("{\"SingerId\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "Singers", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(sourceSchema)
                    .build());

    assertNotNull(((SpannerMutationResponse) response).getMutation());
  }

  @Test
  public void nullRequestThrows() {
    assertThrows(
        InvalidDMLGenerationException.class, () -> new SpannerDMLGenerator().getDMLStatement(null));
  }

  @Test
  public void missingTableInDdlThrows() throws Exception {
    Ddl ddl = Ddl.builder().build(); // empty DDL
    SourceSchema sourceSchema = buildSourceSchema();
    ISchemaMapper mapper = buildIdentityMapper();

    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            new SpannerDMLGenerator()
                .getDMLStatement(
                    new DMLGeneratorRequest.Builder(
                            "INSERT",
                            "Singers",
                            new JSONObject("{}"),
                            new JSONObject("{\"SingerId\":\"1\"}"),
                            "+00:00")
                        .setSchemaMapper(mapper)
                        .setDdl(ddl)
                        .setSourceSchema(sourceSchema)
                        .build()));
  }

  @Test
  public void unsupportedModTypeThrows() throws Exception {
    Ddl ddl = buildDdl();
    SourceSchema sourceSchema = buildSourceSchema();
    ISchemaMapper mapper = buildIdentityMapper();

    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            new SpannerDMLGenerator()
                .getDMLStatement(
                    new DMLGeneratorRequest.Builder(
                            "UPSERT",
                            "Singers",
                            new JSONObject("{}"),
                            new JSONObject("{\"SingerId\":\"1\"}"),
                            "+00:00")
                        .setSchemaMapper(mapper)
                        .setDdl(ddl)
                        .setSourceSchema(sourceSchema)
                        .build()));
  }

  @Test
  public void boolColumnIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("BoolVal", Type.bool());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("BoolVal", "BOOL");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"BoolVal\":true}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    assertNotNull(((SpannerMutationResponse) response).getMutation());
  }

  @Test
  public void bytesColumnIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("BytesVal", Type.bytes());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("BytesVal", "BYTES");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    String base64Hello = java.util.Base64.getEncoder().encodeToString("hello".getBytes());
    JSONObject newValues = new JSONObject("{\"BytesVal\":\"" + base64Hello + "\"}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    assertNotNull(((SpannerMutationResponse) response).getMutation());
  }

  @Test
  public void timestampColumnIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("TsVal", Type.timestamp());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("TsVal", "TIMESTAMP");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"TsVal\":\"2024-01-15T10:30:00Z\"}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    assertNotNull(((SpannerMutationResponse) response).getMutation());
  }

  @Test
  public void dateColumnIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("DateVal", Type.date());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("DateVal", "DATE");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"DateVal\":\"2024-06-15\"}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    assertNotNull(((SpannerMutationResponse) response).getMutation());
  }

  @Test
  public void numericColumnIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("NumVal", Type.numeric());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("NumVal", "NUMERIC");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"NumVal\":\"123.456\"}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    assertNotNull(((SpannerMutationResponse) response).getMutation());
  }

  @Test
  public void int64ColumnIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("IntVal", Type.int64());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("IntVal", "INT64");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"IntVal\":\"42\"}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    assertNotNull(((SpannerMutationResponse) response).getMutation());
  }

  @Test
  public void float64ColumnIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("FloatVal", Type.float64());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("FloatVal", "FLOAT64");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"FloatVal\":3.14}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    assertNotNull(((SpannerMutationResponse) response).getMutation());
  }

  @Test
  public void float32ColumnIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Float32Val", Type.float32());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Float32Val", "FLOAT32");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"Float32Val\":1.5}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    assertNotNull(((SpannerMutationResponse) response).getMutation());
  }

  @Test
  public void stringColumnIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("StrVal", Type.string());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("StrVal", "STRING");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"StrVal\":\"hello\"}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    assertNotNull(((SpannerMutationResponse) response).getMutation());
  }

  @Test
  public void jsonColumnIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("JsonVal", Type.json());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("JsonVal", "JSON");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"JsonVal\":\"{\\\"k\\\":1}\"}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    assertNotNull(((SpannerMutationResponse) response).getMutation());
  }

  @Test
  public void arrayOfInt64ColumnIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.int64()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<INT64>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"ArrVal\":[\"1\",\"2\",\"3\"]}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    assertNotNull(((SpannerMutationResponse) response).getMutation());
  }

  @Test
  public void arrayOfStringColumnIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.string()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<STRING>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"ArrVal\":[\"a\",\"b\"]}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    assertNotNull(((SpannerMutationResponse) response).getMutation());
  }

  private static Ddl buildDdlWithSingleNonPkCol(String colName, Type colType) {
    Ddl.Builder ddlBuilder = Ddl.builder();
    Table.Builder tableBuilder = ddlBuilder.createTable("T");
    tableBuilder.column("Id").int64().notNull().endColumn();
    tableBuilder.column(colName).type(colType).endColumn();
    tableBuilder.primaryKey().asc("Id").end();
    tableBuilder.endTable();
    return ddlBuilder.build();
  }

  private static SourceSchema buildSchemaWithSingleNonPkCol(String colName, String colType) {
    SourceColumn idCol =
        SourceColumn.builder(SRC_TYPE)
            .name("Id")
            .type("INT64")
            .isPrimaryKey(true)
            .isNullable(false)
            .build();
    SourceColumn dataCol =
        SourceColumn.builder(SRC_TYPE).name(colName).type(colType).isNullable(true).build();

    SourceTable table =
        SourceTable.builder(SRC_TYPE)
            .name("T")
            .columns(ImmutableList.of(idCol, dataCol))
            .primaryKeyColumns(ImmutableList.of("Id"))
            .foreignKeys(ImmutableList.of())
            .indexes(ImmutableList.of())
            .build();

    return SourceSchema.builder(SRC_TYPE)
        .databaseName("test-db")
        .tables(ImmutableMap.of("T", table))
        .build();
  }

  private static ISchemaMapper buildMapperForSingleColTable(SourceSchema schema) throws Exception {
    SourceTable table = schema.tables().values().iterator().next();
    String tableName = table.name();
    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSourceTableName("", tableName)).thenReturn(tableName);
    for (SourceColumn col : table.columns()) {
      when(mapper.getSpannerColumnName("", tableName, col.name())).thenReturn(col.name());
      when(mapper.getSourceColumnName("", tableName, col.name())).thenReturn(col.name());
    }
    return mapper;
  }

  @Test
  public void customTransformationInt64IsBoundAsInt64() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Counter", Type.int64());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Counter", "INT64");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"Counter\":\"1\"}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    java.util.Map<String, Object> custom = new java.util.HashMap<>();
    custom.put("Counter", 42L); // custom returns a Long, not a String

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(custom)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    // Type-aware binding: the value should be an INT64, not a STRING.
    assertEquals(42L, mutation.asMap().get("Counter").getInt64());
  }

  @Test
  public void customTransformationBoolIsBoundAsBool() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("IsActive", Type.bool());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("IsActive", "BOOL");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"IsActive\":false}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    java.util.Map<String, Object> custom = new java.util.HashMap<>();
    custom.put("IsActive", Boolean.TRUE);

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(custom)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertEquals(true, mutation.asMap().get("IsActive").getBool());
  }

  @Test
  public void customTransformationTimestampIsBoundAsTimestamp() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Ts", Type.timestamp());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Ts", "TIMESTAMP");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"Ts\":\"2024-01-15T10:30:00Z\"}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    java.util.Map<String, Object> custom = new java.util.HashMap<>();
    custom.put("Ts", "2025-06-01T00:00:00Z"); // custom returns a String for TIMESTAMP

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(custom)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertEquals(
        com.google.cloud.Timestamp.parseTimestamp("2025-06-01T00:00:00Z"),
        mutation.asMap().get("Ts").getTimestamp());
  }

  @Test
  public void nullArrayOfInt64IsBoundAsTypedNullArray() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.int64()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<INT64>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject();
    newValues.put("ArrVal", JSONObject.NULL);
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    com.google.cloud.spanner.Value v =
        ((SpannerMutationResponse) response).getMutation().asMap().get("ArrVal");
    assertNotNull(v);
    org.junit.Assert.assertTrue(v.isNull());
    assertEquals(
        com.google.cloud.spanner.Type.array(com.google.cloud.spanner.Type.int64()), v.getType());
  }

  @Test
  public void nullArrayOfTimestampIsBoundAsTypedNullArray() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.timestamp()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<TIMESTAMP>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject();
    newValues.put("ArrVal", JSONObject.NULL);
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    com.google.cloud.spanner.Value v =
        ((SpannerMutationResponse) response).getMutation().asMap().get("ArrVal");
    assertNotNull(v);
    org.junit.Assert.assertTrue(v.isNull());
    assertEquals(
        com.google.cloud.spanner.Type.array(com.google.cloud.spanner.Type.timestamp()),
        v.getType());
  }

  @Test
  public void nullArrayOfBoolIsBoundAsTypedNullArray() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.bool()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<BOOL>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject();
    newValues.put("ArrVal", JSONObject.NULL);
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    com.google.cloud.spanner.Value v =
        ((SpannerMutationResponse) response).getMutation().asMap().get("ArrVal");
    assertNotNull(v);
    org.junit.Assert.assertTrue(v.isNull());
    assertEquals(
        com.google.cloud.spanner.Type.array(com.google.cloud.spanner.Type.bool()), v.getType());
  }

  @Test
  public void customTransformationNullEmitsTypedNull() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Counter", Type.int64());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Counter", "INT64");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"Counter\":\"1\"}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    java.util.Map<String, Object> custom = new java.util.HashMap<>();
    custom.put("Counter", null);

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(custom)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation.asMap().get("Counter"));
    org.junit.Assert.assertTrue(mutation.asMap().get("Counter").isNull());
  }

  @Test
  public void deleteWithCustomTransformationInt64PkUsesTypedKey() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Data", Type.string());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Data", "STRING");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject();
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    java.util.Map<String, Object> custom = new java.util.HashMap<>();
    custom.put("Id", 7L); // custom returns a Long, not String

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("DELETE", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(custom)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertEquals(Mutation.Op.DELETE, mutation.getOperation());
    // Key contains a typed INT64 part, not a STRING coercion.
    assertEquals(
        com.google.cloud.spanner.Key.of(7L).toString(),
        mutation.getKeySet().getKeys().iterator().next().toString());
  }

  @Test
  public void deleteWithCustomTransformationNullPk() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Data", Type.string());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Data", "STRING");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject();
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    java.util.Map<String, Object> custom = new java.util.HashMap<>();
    custom.put("Id", null);

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("DELETE", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(custom)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertEquals(Mutation.Op.DELETE, mutation.getOperation());
  }

  @Test
  public void customTransformationStringValueIsBoundAsString() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Name", Type.string());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Name", "STRING");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"Name\":\"original\"}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    java.util.Map<String, Object> custom = new java.util.HashMap<>();
    custom.put("Name", "overridden");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(custom)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertEquals("overridden", mutation.asMap().get("Name").getString());
  }

  @Test
  public void customTransformationFloat64ValueIsBoundAsFloat() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Ratio", Type.float64());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Ratio", "FLOAT64");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"Ratio\":1.5}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    java.util.Map<String, Object> custom = new java.util.HashMap<>();
    custom.put("Ratio", 3.14);

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(custom)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertEquals(3.14, mutation.asMap().get("Ratio").getFloat64(), 0.0001);
  }

  @Test
  public void customTransformationNumericValueIsBoundAsNumeric() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Amount", Type.numeric());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Amount", "NUMERIC");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"Amount\":\"1.0\"}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    java.util.Map<String, Object> custom = new java.util.HashMap<>();
    custom.put("Amount", new java.math.BigDecimal("12345.6789"));

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(custom)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertEquals(
        new java.math.BigDecimal("12345.6789"), mutation.asMap().get("Amount").getNumeric());
  }

  @Test
  public void nullValueForBoolColumnIsTypedNull() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Flag", Type.bool());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Flag", "BOOL");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject();
    newValues.put("Flag", JSONObject.NULL);
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    com.google.cloud.spanner.Value v =
        ((SpannerMutationResponse) response).getMutation().asMap().get("Flag");
    assertNotNull(v);
    org.junit.Assert.assertTrue(v.isNull());
    assertEquals(com.google.cloud.spanner.Type.bool(), v.getType());
  }

  @Test
  public void nullValueForDateColumnIsTypedNull() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Day", Type.date());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Day", "DATE");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject();
    newValues.put("Day", JSONObject.NULL);
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    com.google.cloud.spanner.Value v =
        ((SpannerMutationResponse) response).getMutation().asMap().get("Day");
    assertNotNull(v);
    org.junit.Assert.assertTrue(v.isNull());
    assertEquals(com.google.cloud.spanner.Type.date(), v.getType());
  }

  @Test
  public void nullValueForJsonColumnIsTypedNull() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Payload", Type.json());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Payload", "JSON");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject();
    newValues.put("Payload", JSONObject.NULL);
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    com.google.cloud.spanner.Value v =
        ((SpannerMutationResponse) response).getMutation().asMap().get("Payload");
    assertNotNull(v);
    org.junit.Assert.assertTrue(v.isNull());
  }

  @Test
  public void arrayOfBoolColumnIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Flags", Type.array(Type.bool()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Flags", "ARRAY<BOOL>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"Flags\":[true,false,true]}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation.asMap().get("Flags"));
  }

  @Test
  public void arrayOfFloat64ColumnIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Vals", Type.array(Type.float64()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Vals", "ARRAY<FLOAT64>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"Vals\":[1.1, 2.2, 3.3]}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation.asMap().get("Vals"));
  }

  @Test
  public void arrayOfTimestampColumnIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Tss", Type.array(Type.timestamp()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Tss", "ARRAY<TIMESTAMP>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues =
        new JSONObject("{\"Tss\":[\"2024-01-01T00:00:00Z\",\"2024-06-15T12:00:00Z\"]}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation.asMap().get("Tss"));
  }

  @Test
  public void arrayOfDateColumnIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Days", Type.array(Type.date()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Days", "ARRAY<DATE>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"Days\":[\"2024-01-01\",\"2024-06-15\"]}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation.asMap().get("Days"));
  }

  @Test
  public void arrayOfNumericColumnIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Nums", Type.array(Type.numeric()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Nums", "ARRAY<NUMERIC>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"Nums\":[\"1.1\",\"2.2\"]}");
    JSONObject keyValues = new JSONObject("{\"Id\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "T", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation.asMap().get("Nums"));
  }

  @Test
  public void missingTargetTableInSourceSchemaThrows() throws Exception {
    Ddl ddl = buildDdl();
    SourceSchema emptySchema =
        SourceSchema.builder(SRC_TYPE).databaseName("test-db").tables(ImmutableMap.of()).build();
    ISchemaMapper mapper = buildIdentityMapper();

    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            new SpannerDMLGenerator()
                .getDMLStatement(
                    new DMLGeneratorRequest.Builder(
                            "INSERT",
                            "Singers",
                            new JSONObject("{}"),
                            new JSONObject("{\"SingerId\":\"1\"}"),
                            "+00:00")
                        .setSchemaMapper(mapper)
                        .setDdl(ddl)
                        .setSourceSchema(emptySchema)
                        .build()));
  }

  @Test
  public void nullSchemaMapperThrows() throws Exception {
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            new SpannerDMLGenerator()
                .getDMLStatement(
                    new DMLGeneratorRequest.Builder(
                            "INSERT",
                            "Singers",
                            new JSONObject("{}"),
                            new JSONObject("{\"SingerId\":\"1\"}"),
                            "+00:00")
                        .setDdl(buildDdl())
                        .setSourceSchema(buildSourceSchema())
                        .build()));
  }

  @Test
  public void nullDdlThrows() throws Exception {
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            new SpannerDMLGenerator()
                .getDMLStatement(
                    new DMLGeneratorRequest.Builder(
                            "INSERT",
                            "Singers",
                            new JSONObject("{}"),
                            new JSONObject("{\"SingerId\":\"1\"}"),
                            "+00:00")
                        .setSchemaMapper(buildIdentityMapper())
                        .setSourceSchema(buildSourceSchema())
                        .build()));
  }

  @Test
  public void nullSourceSchemaThrows() throws Exception {
    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            new SpannerDMLGenerator()
                .getDMLStatement(
                    new DMLGeneratorRequest.Builder(
                            "INSERT",
                            "Singers",
                            new JSONObject("{}"),
                            new JSONObject("{\"SingerId\":\"1\"}"),
                            "+00:00")
                        .setSchemaMapper(buildIdentityMapper())
                        .setDdl(buildDdl())
                        .build()));
  }
}
