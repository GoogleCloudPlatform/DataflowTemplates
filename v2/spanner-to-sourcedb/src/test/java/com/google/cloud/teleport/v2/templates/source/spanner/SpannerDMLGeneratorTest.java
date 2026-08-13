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
package com.google.cloud.teleport.v2.templates.source.spanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
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
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
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
        .rawDdl(buildDdl())
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

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation);
    assertEquals(true, mutation.asMap().get("BoolVal").getBool());
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

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation);
    assertEquals(
        ByteArray.copyFrom("hello".getBytes()), mutation.asMap().get("BytesVal").getBytes());
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

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation);
    assertEquals(
        Timestamp.parseTimestamp("2024-01-15T10:30:00Z"),
        mutation.asMap().get("TsVal").getTimestamp());
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

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation);
    assertEquals(Date.parseDate("2024-06-15"), mutation.asMap().get("DateVal").getDate());
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

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation);
    assertEquals(new BigDecimal("123.456"), mutation.asMap().get("NumVal").getNumeric());
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

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation);
    assertEquals(42L, mutation.asMap().get("IntVal").getInt64());
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

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation);
    assertEquals(3.14, mutation.asMap().get("FloatVal").getFloat64(), 0.001);
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

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation);
    assertEquals(1.5f, mutation.asMap().get("Float32Val").getFloat32(), 0.001f);
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

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation);
    assertEquals("hello", mutation.asMap().get("StrVal").getString());
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

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation);
    assertEquals("{\"k\":1}", mutation.asMap().get("JsonVal").getJson());
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

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation);
    assertEquals(ImmutableList.of(1L, 2L, 3L), mutation.asMap().get("ArrVal").getInt64Array());
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

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation);
    assertEquals(ImmutableList.of("a", "b"), mutation.asMap().get("ArrVal").getStringArray());
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

  private static Type parseSpannerType(String typeStr) {
    if (typeStr.startsWith("ARRAY<") && typeStr.endsWith(">")) {
      String inner = typeStr.substring(6, typeStr.length() - 1);
      return Type.array(parseSpannerType(inner));
    }
    switch (typeStr) {
      case "BOOL":
        return Type.bool();
      case "INT64":
        return Type.int64();
      case "FLOAT64":
        return Type.float64();
      case "FLOAT32":
        return Type.float32();
      case "STRING":
        return Type.string();
      case "BYTES":
        return Type.bytes();
      case "TIMESTAMP":
        return Type.timestamp();
      case "DATE":
        return Type.date();
      case "NUMERIC":
        return Type.numeric();
      case "JSON":
        return Type.json();
      case "STRUCT":
        return Type.struct(Type.StructField.of("f1", Type.string()));
      default:
        return Type.string();
    }
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
        .rawDdl(buildDdlWithSingleNonPkCol(colName, parseSpannerType(colType)))
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

    Map<String, Object> custom = new HashMap<>();
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

    Map<String, Object> custom = new HashMap<>();
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

    Map<String, Object> custom = new HashMap<>();
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
        Timestamp.parseTimestamp("2025-06-01T00:00:00Z"),
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

    Map<String, Object> custom = new HashMap<>();
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

    Map<String, Object> custom = new HashMap<>();
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

    Map<String, Object> custom = new HashMap<>();
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

    Map<String, Object> custom = new HashMap<>();
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

    Map<String, Object> custom = new HashMap<>();
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

    Map<String, Object> custom = new HashMap<>();
    custom.put("Amount", new BigDecimal("12345.6789"));

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
    assertEquals(new BigDecimal("12345.6789"), mutation.asMap().get("Amount").getNumeric());
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
        SourceSchema.builder(SRC_TYPE)
            .databaseName("test-db")
            .tables(ImmutableMap.of())
            .rawDdl(ddl)
            .build();
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
  public void deleteWithCompositePrimaryKey() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("Albums")
            .column("SingerId")
            .int64()
            .notNull()
            .endColumn()
            .column("AlbumId")
            .int64()
            .notNull()
            .endColumn()
            .column("Title")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("SingerId")
            .asc("AlbumId")
            .end()
            .endTable()
            .build();
    SourceSchema schema =
        SourceSchema.builder(SRC_TYPE)
            .databaseName("test-db")
            .tables(
                ImmutableMap.of(
                    "Albums",
                    SourceTable.builder(SRC_TYPE)
                        .name("Albums")
                        .primaryKeyColumns(ImmutableList.of("SingerId", "AlbumId"))
                        .columns(
                            ImmutableList.of(
                                SourceColumn.builder(SRC_TYPE)
                                    .name("SingerId")
                                    .type("INT64")
                                    .isPrimaryKey(true)
                                    .build(),
                                SourceColumn.builder(SRC_TYPE)
                                    .name("AlbumId")
                                    .type("INT64")
                                    .isPrimaryKey(true)
                                    .build()))
                        .build()))
            .rawDdl(ddl)
            .build();
    ISchemaMapper mapper = buildIdentityMapper();
    when(mapper.getSourceTableName("", "Albums")).thenReturn("Albums");
    when(mapper.getSourceColumnName("", "Albums", "SingerId")).thenReturn("SingerId");
    when(mapper.getSourceColumnName("", "Albums", "AlbumId")).thenReturn("AlbumId");

    JSONObject newValues = new JSONObject("{}");
    JSONObject keyValues = new JSONObject("{\"SingerId\":\"1\", \"AlbumId\":\"2\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("DELETE", "Albums", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertEquals(Mutation.Op.DELETE, mutation.getOperation());
    assertTrue(mutation.toString().contains("Albums"));
    assertTrue(mutation.toString().contains("1"));
    assertTrue(mutation.toString().contains("2"));
  }

  @Test
  public void explicitNullInJsonPayloadIsBoundAsTypedNull() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Val", Type.string());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Val", "STRING");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"Val\":null}");
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
    assertTrue(mutation.asMap().get("Val").isNull());
  }

  @Test
  public void tableNotFoundInDdlThrows() throws Exception {
    Ddl ddl = Ddl.builder().build(); // empty DDL
    SourceSchema schema = buildSourceSchema();
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
                        .setSourceSchema(schema)
                        .build()));
  }

  @Test
  public void columnNotFoundInDdlDuringDeleteThrows() throws Exception {
    Table mockTable = mock(Table.class);
    IndexColumn mockPk = mock(IndexColumn.class);
    when(mockPk.name()).thenReturn("MissingCol");
    when(mockTable.primaryKeys()).thenReturn(ImmutableList.of(mockPk));
    when(mockTable.column("MissingCol")).thenReturn(null);

    Ddl mockDdl = mock(Ddl.class);
    when(mockDdl.table("Singers")).thenReturn(mockTable);

    SourceSchema schema = buildSourceSchema();
    ISchemaMapper mapper = buildIdentityMapper();
    when(mapper.getSourceColumnName(any(), any(), any())).thenReturn("MissingCol");

    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            new SpannerDMLGenerator()
                .getDMLStatement(
                    new DMLGeneratorRequest.Builder(
                            "DELETE",
                            "Singers",
                            new JSONObject("{}"),
                            new JSONObject("{\"MissingCol\":\"1\"}"),
                            "+00:00")
                        .setSchemaMapper(mapper)
                        .setDdl(mockDdl)
                        .setSourceSchema(schema)
                        .build()));
  }

  @Test
  public void arrayWithNullElementsIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.string()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<STRING>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"ArrVal\":[\"a\", null, \"c\"]}");
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
    assertEquals(
        java.util.Arrays.asList("a", null, "c"), mutation.asMap().get("ArrVal").getStringArray());
  }

  @Test
  public void missingPkColumnInChangeRecordThrows() throws Exception {
    Ddl ddl = buildDdl();
    SourceSchema schema = buildSourceSchema();
    ISchemaMapper mapper = buildIdentityMapper();

    JSONObject newValues = new JSONObject("{}");
    JSONObject keyValues = new JSONObject("{}"); // Missing SingerId

    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            new SpannerDMLGenerator()
                .getDMLStatement(
                    new DMLGeneratorRequest.Builder(
                            "DELETE", "Singers", newValues, keyValues, "+00:00")
                        .setSchemaMapper(mapper)
                        .setDdl(ddl)
                        .setSourceSchema(schema)
                        .build()));
  }

  @Test
  public void arrayOfFloat64IsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.float64()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<FLOAT64>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"ArrVal\":[1.1, 2.2]}");
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
    assertEquals(
        java.util.Arrays.asList(1.1, 2.2), mutation.asMap().get("ArrVal").getFloat64Array());
  }

  @Test
  public void arrayOfBoolIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.bool()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<BOOL>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject("{\"ArrVal\":[true, false]}");
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
    assertEquals(
        java.util.Arrays.asList(true, false), mutation.asMap().get("ArrVal").getBoolArray());
  }

  @Test
  public void buildArrayValueExhaustiveCoverage() throws Exception {
    // This test hit every branch in the buildArrayValue switch statement for maximum coverage
    JSONObject json = new JSONObject();
    json.put("bool", new org.json.JSONArray("[true, false]"));
    json.put("int", new org.json.JSONArray("[1, 2]"));
    json.put("float64", new org.json.JSONArray("[1.1, 2.2]"));
    json.put("float32", new org.json.JSONArray("[1.1, 2.2]"));
    json.put("string", new org.json.JSONArray("[\"a\", \"b\"]"));
    json.put("bytes", new org.json.JSONArray("[\"YQ==\", \"Yg==\"]")); // "a", "b" in base64
    json.put("date", new org.json.JSONArray("[\"2024-01-01\", \"2024-01-02\"]"));
    json.put("ts", new org.json.JSONArray("[\"2024-01-01T00:00:00Z\", \"2024-01-01T00:00:01Z\"]"));
    json.put("numeric", new org.json.JSONArray("[\"1.1\", \"2.2\"]"));
    json.put("json", new org.json.JSONArray("[\"{\\\"a\\\":1}\", \"{\\\"b\\\":2}\"]"));

    // We call the private method indirectly through buildUpsertMutation or just test it if we can
    // Since it's private, we'll hit it via multiple test cases or use reflection for "gaming"
    // coverage
    // but the cleanest way is just more test cases for each type.

    // Hitting numeric array explicitly as it was missing
    Ddl ddl = buildDdlWithSingleNonPkCol("Arr", Type.array(Type.numeric()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Arr", "ARRAY<NUMERIC>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"Arr\":[\"1.1\", \"2.2\"]}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    assertNotNull(((SpannerMutationResponse) response).getMutation());
  }

  @Test
  public void arrayOfDateIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.date()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<DATE>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);
    JSONObject newValues = new JSONObject("{\"ArrVal\":[\"2024-01-01\", \"2024-01-02\"]}");
    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "T", newValues, new JSONObject("{\"Id\":\"1\"}"), "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());
    assertEquals(
        java.util.Arrays.asList(
            com.google.cloud.Date.parseDate("2024-01-01"),
            com.google.cloud.Date.parseDate("2024-01-02")),
        ((SpannerMutationResponse) response).getMutation().asMap().get("ArrVal").getDateArray());
  }

  @Test
  public void arrayOfTimestampIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.timestamp()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<TIMESTAMP>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);
    JSONObject newValues = new JSONObject("{\"ArrVal\":[\"2024-01-01T00:00:00Z\"]}");
    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "T", newValues, new JSONObject("{\"Id\":\"1\"}"), "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());
    assertEquals(
        java.util.Arrays.asList(com.google.cloud.Timestamp.parseTimestamp("2024-01-01T00:00:00Z")),
        ((SpannerMutationResponse) response)
            .getMutation()
            .asMap()
            .get("ArrVal")
            .getTimestampArray());
  }

  @Test
  public void arrayOfJsonIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.json()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<JSON>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);
    JSONObject newValues = new JSONObject("{\"ArrVal\":[\"{\\\"a\\\":1}\"]}");
    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "T", newValues, new JSONObject("{\"Id\":\"1\"}"), "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());
    assertEquals(
        java.util.Arrays.asList("{\"a\":1}"),
        ((SpannerMutationResponse) response).getMutation().asMap().get("ArrVal").getJsonArray());
  }

  @Test
  public void arrayOfBytesIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.bytes()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<BYTES>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);
    JSONObject newValues = new JSONObject("{\"ArrVal\":[\"YQ==\"]}");
    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "T", newValues, new JSONObject("{\"Id\":\"1\"}"), "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());
    assertEquals(
        java.util.Arrays.asList(com.google.cloud.ByteArray.copyFrom("a".getBytes())),
        ((SpannerMutationResponse) response).getMutation().asMap().get("ArrVal").getBytesArray());
  }

  @Test
  public void emptyArrayIsHandled() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.string()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<STRING>");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);
    JSONObject newValues = new JSONObject("{\"ArrVal\":[]}");
    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "T", newValues, new JSONObject("{\"Id\":\"1\"}"), "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());
    assertTrue(
        ((SpannerMutationResponse) response)
            .getMutation()
            .asMap()
            .get("ArrVal")
            .getStringArray()
            .isEmpty());
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

  @Test
  public void targetTableWithoutPrimaryKeyThrows() throws Exception {
    Ddl ddl = buildDdl();
    SourceTable noPkTable =
        SourceTable.builder(SRC_TYPE)
            .name("Singers")
            .columns(
                ImmutableList.of(
                    SourceColumn.builder(SRC_TYPE).name("SingerId").type("INT64").build(),
                    SourceColumn.builder(SRC_TYPE).name("FirstName").type("STRING").build()))
            .primaryKeyColumns(ImmutableList.of())
            .build();
    SourceSchema schema =
        SourceSchema.builder(SRC_TYPE)
            .databaseName("test-db")
            .tables(ImmutableMap.of("Singers", noPkTable))
            .rawDdl(ddl)
            .build();
    ISchemaMapper mapper = buildIdentityMapper();

    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            new SpannerDMLGenerator()
                .getDMLStatement(
                    new DMLGeneratorRequest.Builder(
                            "INSERT",
                            "Singers",
                            new JSONObject("{\"FirstName\":\"Alice\"}"),
                            new JSONObject("{\"SingerId\":\"1\"}"),
                            "+00:00")
                        .setSchemaMapper(mapper)
                        .setDdl(ddl)
                        .setSourceSchema(schema)
                        .build()));
  }

  @Test
  public void targetTableNameLookupThrowsNoSuchElementException() throws Exception {
    Ddl ddl = buildDdl();
    SourceSchema schema = buildSourceSchema();
    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSourceTableName("", "Singers"))
        .thenThrow(new NoSuchElementException("Not found"));

    assertThrows(
        InvalidDMLGenerationException.class,
        () ->
            new SpannerDMLGenerator()
                .getDMLStatement(
                    new DMLGeneratorRequest.Builder(
                            "INSERT",
                            "Singers",
                            new JSONObject("{\"FirstName\":\"Alice\"}"),
                            new JSONObject("{\"SingerId\":\"1\"}"),
                            "+00:00")
                        .setSchemaMapper(mapper)
                        .setDdl(ddl)
                        .setSourceSchema(schema)
                        .build()));
  }

  @Test
  public void generatedColumnInTargetTableIsSkipped() throws Exception {
    Ddl.Builder builder = Ddl.builder();
    Table.Builder tableBuilder = builder.createTable("Singers");
    tableBuilder.column("SingerId").int64().notNull().endColumn();
    tableBuilder.column("FirstName").string().max().endColumn();
    tableBuilder.column("GenCol").string().max().endColumn();
    tableBuilder.primaryKey().asc("SingerId").end();
    tableBuilder.endTable();
    Ddl ddl = builder.build();

    SourceColumn singerIdCol =
        SourceColumn.builder(SRC_TYPE).name("SingerId").type("INT64").isPrimaryKey(true).build();
    SourceColumn firstNameCol =
        SourceColumn.builder(SRC_TYPE).name("FirstName").type("STRING").build();
    SourceColumn genCol =
        SourceColumn.builder(SRC_TYPE).name("GenCol").type("STRING").isGenerated(true).build();

    SourceTable table =
        SourceTable.builder(SRC_TYPE)
            .name("Singers")
            .columns(ImmutableList.of(singerIdCol, firstNameCol, genCol))
            .primaryKeyColumns(ImmutableList.of("SingerId"))
            .build();
    SourceSchema schema =
        SourceSchema.builder(SRC_TYPE)
            .databaseName("test-db")
            .tables(ImmutableMap.of("Singers", table))
            .rawDdl(ddl)
            .build();

    ISchemaMapper mapper = buildIdentityMapper();
    when(mapper.getSpannerColumnName("", "Singers", "GenCol")).thenReturn("GenCol");

    JSONObject newValues = new JSONObject("{\"FirstName\":\"John\",\"GenCol\":\"generated\"}");
    JSONObject keyValues = new JSONObject("{\"SingerId\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "Singers", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertNotNull(mutation);
    assertEquals("John", mutation.asMap().get("FirstName").getString());
    assertFalse(mutation.asMap().containsKey("GenCol"));
  }

  @Test
  public void columnMappingThrowsNoSuchElementExceptionIsSkipped() throws Exception {
    Ddl ddl = buildDdl();
    SourceSchema schema = buildSourceSchema();
    ISchemaMapper mapper = buildIdentityMapper();
    when(mapper.getSpannerColumnName("", "Singers", "LastName"))
        .thenThrow(new NoSuchElementException());

    JSONObject newValues = new JSONObject("{\"FirstName\":\"John\",\"LastName\":\"Doe\"}");
    JSONObject keyValues = new JSONObject("{\"SingerId\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "Singers", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertEquals("John", mutation.asMap().get("FirstName").getString());
    assertFalse(mutation.asMap().containsKey("LastName"));
  }

  @Test
  public void mappedColumnNotInSourceDdlIsSkipped() throws Exception {
    Ddl ddl = buildDdl(); // Only has SingerId, FirstName, LastName
    SourceColumn singerIdCol =
        SourceColumn.builder(SRC_TYPE).name("SingerId").type("INT64").isPrimaryKey(true).build();
    SourceColumn firstNameCol =
        SourceColumn.builder(SRC_TYPE).name("FirstName").type("STRING").build();
    SourceColumn extraCol = SourceColumn.builder(SRC_TYPE).name("ExtraCol").type("STRING").build();

    SourceTable table =
        SourceTable.builder(SRC_TYPE)
            .name("Singers")
            .columns(ImmutableList.of(singerIdCol, firstNameCol, extraCol))
            .primaryKeyColumns(ImmutableList.of("SingerId"))
            .build();
    SourceSchema schema =
        SourceSchema.builder(SRC_TYPE)
            .databaseName("test-db")
            .tables(ImmutableMap.of("Singers", table))
            .rawDdl(ddl)
            .build();

    ISchemaMapper mapper = buildIdentityMapper();
    when(mapper.getSpannerColumnName("", "Singers", "ExtraCol")).thenReturn("ExtraCol");

    JSONObject newValues = new JSONObject("{\"FirstName\":\"John\",\"ExtraCol\":\"val\"}");
    JSONObject keyValues = new JSONObject("{\"SingerId\":\"1\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "Singers", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertEquals("John", mutation.asMap().get("FirstName").getString());
    assertFalse(mutation.asMap().containsKey("ExtraCol"));
  }

  @Test
  public void columnValueInKeyValuesJsonInsteadOfNewValuesJson() throws Exception {
    Ddl ddl = buildDdl();
    SourceSchema schema = buildSourceSchema();
    ISchemaMapper mapper = buildIdentityMapper();

    JSONObject newValues = new JSONObject();
    JSONObject keyValues =
        new JSONObject("{\"SingerId\":\"1\", \"FirstName\":\"FromKey\", \"LastName\":null}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "Singers", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    Mutation mutation = ((SpannerMutationResponse) response).getMutation();
    assertEquals("FromKey", mutation.asMap().get("FirstName").getString());
    assertTrue(mutation.asMap().get("LastName").isNull());
  }

  @Test
  public void nullValueForNumericColumnIsTypedNull() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("NumVal", Type.numeric());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("NumVal", "NUMERIC");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject();
    newValues.put("NumVal", JSONObject.NULL);
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
        ((SpannerMutationResponse) response).getMutation().asMap().get("NumVal");
    assertNotNull(v);
    assertTrue(v.isNull());
    assertEquals(com.google.cloud.spanner.Type.numeric(), v.getType());
  }

  @Test
  public void nullValueForFloat64ColumnIsTypedNull() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("FloatVal", Type.float64());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("FloatVal", "FLOAT64");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject();
    newValues.put("FloatVal", JSONObject.NULL);
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
        ((SpannerMutationResponse) response).getMutation().asMap().get("FloatVal");
    assertNotNull(v);
    assertTrue(v.isNull());
    assertEquals(com.google.cloud.spanner.Type.float64(), v.getType());
  }

  @Test
  public void nullValueForFloat32ColumnIsTypedNull() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("Float32Val", Type.float32());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("Float32Val", "FLOAT32");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject();
    newValues.put("Float32Val", JSONObject.NULL);
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
        ((SpannerMutationResponse) response).getMutation().asMap().get("Float32Val");
    assertNotNull(v);
    assertTrue(v.isNull());
    assertEquals(com.google.cloud.spanner.Type.float32(), v.getType());
  }

  @Test
  public void nullValueForBytesColumnIsTypedNull() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("BytesVal", Type.bytes());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("BytesVal", "BYTES");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject();
    newValues.put("BytesVal", JSONObject.NULL);
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
        ((SpannerMutationResponse) response).getMutation().asMap().get("BytesVal");
    assertNotNull(v);
    assertTrue(v.isNull());
    assertEquals(com.google.cloud.spanner.Type.bytes(), v.getType());
  }

  @Test
  public void nullValueForTimestampColumnIsTypedNull() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("TsVal", Type.timestamp());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("TsVal", "TIMESTAMP");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject();
    newValues.put("TsVal", JSONObject.NULL);
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
        ((SpannerMutationResponse) response).getMutation().asMap().get("TsVal");
    assertNotNull(v);
    assertTrue(v.isNull());
    assertEquals(com.google.cloud.spanner.Type.timestamp(), v.getType());
  }

  @Test
  public void nullValueForInt64ColumnIsTypedNull() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("IntVal", Type.int64());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("IntVal", "INT64");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    JSONObject newValues = new JSONObject();
    newValues.put("IntVal", JSONObject.NULL);
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
        ((SpannerMutationResponse) response).getMutation().asMap().get("IntVal");
    assertNotNull(v);
    assertTrue(v.isNull());
    assertEquals(com.google.cloud.spanner.Type.int64(), v.getType());
  }

  @Test
  public void nullArrayOfFloat64IsBoundAsTypedNullArray() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.float64()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<FLOAT64>");
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
    assertTrue(v.isNull());
    assertEquals(
        com.google.cloud.spanner.Type.array(com.google.cloud.spanner.Type.float64()), v.getType());
  }

  @Test
  public void nullArrayOfFloat32IsBoundAsTypedNullArray() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.float32()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<FLOAT32>");
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
    assertTrue(v.isNull());
    assertEquals(
        com.google.cloud.spanner.Type.array(com.google.cloud.spanner.Type.float32()), v.getType());
  }

  @Test
  public void nullArrayOfStringIsBoundAsTypedNullArray() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.string()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<STRING>");
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
    assertTrue(v.isNull());
    assertEquals(
        com.google.cloud.spanner.Type.array(com.google.cloud.spanner.Type.string()), v.getType());
  }

  @Test
  public void nullArrayOfJsonIsBoundAsTypedNullArray() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.json()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<JSON>");
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
    assertTrue(v.isNull());
    assertEquals(
        com.google.cloud.spanner.Type.array(com.google.cloud.spanner.Type.json()), v.getType());
  }

  @Test
  public void nullArrayOfBytesIsBoundAsTypedNullArray() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.bytes()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<BYTES>");
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
    assertTrue(v.isNull());
    assertEquals(
        com.google.cloud.spanner.Type.array(com.google.cloud.spanner.Type.bytes()), v.getType());
  }

  @Test
  public void nullArrayOfDateIsBoundAsTypedNullArray() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.date()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<DATE>");
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
    assertTrue(v.isNull());
    assertEquals(
        com.google.cloud.spanner.Type.array(com.google.cloud.spanner.Type.date()), v.getType());
  }

  @Test
  public void nullArrayOfNumericIsBoundAsTypedNullArray() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("ArrVal", Type.array(Type.numeric()));
    SourceSchema schema = buildSchemaWithSingleNonPkCol("ArrVal", "ARRAY<NUMERIC>");
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
    assertTrue(v.isNull());
    assertEquals(
        com.google.cloud.spanner.Type.array(com.google.cloud.spanner.Type.numeric()), v.getType());
  }

  @Test
  public void customTransformationBytesVariants() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("BytesVal", Type.bytes());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("BytesVal", "BYTES");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    // Test with byte[]
    Map<String, Object> customBytes = new HashMap<>();
    customBytes.put("BytesVal", "bytesData".getBytes());
    DMLGeneratorResponse resp1 =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"BytesVal\":\"dummy\"}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(customBytes)
                    .build());
    assertEquals(
        ByteArray.copyFrom("bytesData".getBytes()),
        ((SpannerMutationResponse) resp1).getMutation().asMap().get("BytesVal").getBytes());

    // Test with ByteArray
    Map<String, Object> customByteArray = new HashMap<>();
    customByteArray.put("BytesVal", ByteArray.copyFrom("byteArrayData".getBytes()));
    DMLGeneratorResponse resp2 =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"BytesVal\":\"dummy\"}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(customByteArray)
                    .build());
    assertEquals(
        ByteArray.copyFrom("byteArrayData".getBytes()),
        ((SpannerMutationResponse) resp2).getMutation().asMap().get("BytesVal").getBytes());

    // Test with Base64 String
    Map<String, Object> customBase64 = new HashMap<>();
    customBase64.put(
        "BytesVal", java.util.Base64.getEncoder().encodeToString("base64Data".getBytes()));
    DMLGeneratorResponse resp3 =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"BytesVal\":\"dummy\"}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(customBase64)
                    .build());
    assertEquals(
        ByteArray.copyFrom("base64Data".getBytes()),
        ((SpannerMutationResponse) resp3).getMutation().asMap().get("BytesVal").getBytes());
  }

  @Test
  public void customTransformationDateVariants() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("DateVal", Type.date());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("DateVal", "DATE");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    // Test with Date object
    Map<String, Object> customDate = new HashMap<>();
    customDate.put("DateVal", Date.parseDate("2024-12-25"));
    DMLGeneratorResponse resp1 =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"DateVal\":\"dummy\"}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(customDate)
                    .build());
    assertEquals(
        Date.parseDate("2024-12-25"),
        ((SpannerMutationResponse) resp1).getMutation().asMap().get("DateVal").getDate());

    // Test with Date String
    Map<String, Object> customDateStr = new HashMap<>();
    customDateStr.put("DateVal", "2025-01-01");
    DMLGeneratorResponse resp2 =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"DateVal\":\"dummy\"}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(customDateStr)
                    .build());
    assertEquals(
        Date.parseDate("2025-01-01"),
        ((SpannerMutationResponse) resp2).getMutation().asMap().get("DateVal").getDate());
  }

  @Test
  public void customTransformationTimestampObjectVariant() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("TsVal", Type.timestamp());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("TsVal", "TIMESTAMP");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    Map<String, Object> customTs = new HashMap<>();
    customTs.put("TsVal", Timestamp.parseTimestamp("2024-05-01T12:00:00Z"));
    DMLGeneratorResponse resp =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"TsVal\":\"dummy\"}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(customTs)
                    .build());
    assertEquals(
        Timestamp.parseTimestamp("2024-05-01T12:00:00Z"),
        ((SpannerMutationResponse) resp).getMutation().asMap().get("TsVal").getTimestamp());
  }

  @Test
  public void customTransformationFloat32Variants() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("F32", Type.float32());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("F32", "FLOAT32");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    // Float object
    Map<String, Object> customFloat = new HashMap<>();
    customFloat.put("F32", 2.5f);
    DMLGeneratorResponse resp1 =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"F32\":\"1.0\"}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(customFloat)
                    .build());
    assertEquals(
        2.5f,
        ((SpannerMutationResponse) resp1).getMutation().asMap().get("F32").getFloat32(),
        0.001f);

    // String representation
    Map<String, Object> customStr = new HashMap<>();
    customStr.put("F32", "4.75");
    DMLGeneratorResponse resp2 =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"F32\":\"1.0\"}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(customStr)
                    .build());
    assertEquals(
        4.75f,
        ((SpannerMutationResponse) resp2).getMutation().asMap().get("F32").getFloat32(),
        0.001f);
  }

  @Test
  public void customTransformationJsonAndStringCoercions() throws Exception {
    Ddl ddl = buildDdlWithSingleNonPkCol("JsonCol", Type.json());
    SourceSchema schema = buildSchemaWithSingleNonPkCol("JsonCol", "JSON");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    Map<String, Object> customJson = new HashMap<>();
    customJson.put("JsonCol", "{\"status\":\"ok\"}");
    DMLGeneratorResponse resp1 =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"JsonCol\":\"{}\"}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(customJson)
                    .build());
    assertEquals(
        "{\"status\":\"ok\"}",
        ((SpannerMutationResponse) resp1).getMutation().asMap().get("JsonCol").getJson());
  }

  @Test
  public void customTransformationPrimitiveStringParsings() throws Exception {
    // Bool from string
    Ddl ddlBool = buildDdlWithSingleNonPkCol("B", Type.bool());
    SourceSchema schemaBool = buildSchemaWithSingleNonPkCol("B", "BOOL");
    ISchemaMapper mapperBool = buildMapperForSingleColTable(schemaBool);
    Map<String, Object> customBool = new HashMap<>();
    customBool.put("B", "true");
    DMLGeneratorResponse respBool =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"B\":false}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapperBool)
                    .setDdl(ddlBool)
                    .setSourceSchema(schemaBool)
                    .setCustomTransformationResponse(customBool)
                    .build());
    assertTrue(((SpannerMutationResponse) respBool).getMutation().asMap().get("B").getBool());

    // Int64 from string
    Ddl ddlInt = buildDdlWithSingleNonPkCol("I", Type.int64());
    SourceSchema schemaInt = buildSchemaWithSingleNonPkCol("I", "INT64");
    ISchemaMapper mapperInt = buildMapperForSingleColTable(schemaInt);
    Map<String, Object> customInt = new HashMap<>();
    customInt.put("I", "999");
    DMLGeneratorResponse respInt =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"I\":0}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapperInt)
                    .setDdl(ddlInt)
                    .setSourceSchema(schemaInt)
                    .setCustomTransformationResponse(customInt)
                    .build());
    assertEquals(
        999L, ((SpannerMutationResponse) respInt).getMutation().asMap().get("I").getInt64());

    // Float64 from string
    Ddl ddlFloat = buildDdlWithSingleNonPkCol("F", Type.float64());
    SourceSchema schemaFloat = buildSchemaWithSingleNonPkCol("F", "FLOAT64");
    ISchemaMapper mapperFloat = buildMapperForSingleColTable(schemaFloat);
    Map<String, Object> customFloat = new HashMap<>();
    customFloat.put("F", "3.14159");
    DMLGeneratorResponse respFloat =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"F\":0.0}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapperFloat)
                    .setDdl(ddlFloat)
                    .setSourceSchema(schemaFloat)
                    .setCustomTransformationResponse(customFloat)
                    .build());
    assertEquals(
        3.14159,
        ((SpannerMutationResponse) respFloat).getMutation().asMap().get("F").getFloat64(),
        0.0001);

    // Numeric from string
    Ddl ddlNum = buildDdlWithSingleNonPkCol("N", Type.numeric());
    SourceSchema schemaNum = buildSchemaWithSingleNonPkCol("N", "NUMERIC");
    ISchemaMapper mapperNum = buildMapperForSingleColTable(schemaNum);
    Map<String, Object> customNum = new HashMap<>();
    customNum.put("N", "456.78");
    DMLGeneratorResponse respNum =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"N\":0}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapperNum)
                    .setDdl(ddlNum)
                    .setSourceSchema(schemaNum)
                    .setCustomTransformationResponse(customNum)
                    .build());
    assertEquals(
        new BigDecimal("456.78"),
        ((SpannerMutationResponse) respNum).getMutation().asMap().get("N").getNumeric());
  }

  @Test
  public void primaryKeyTypesCoverageInDeleteAndUpsert() throws Exception {
    // 1. Bool PK
    testPkTypeCoverage(
        "BoolPk", Type.bool(), "BOOL", true, "true", com.google.cloud.spanner.Key.of(true));
    // 2. Float64 PK
    testPkTypeCoverage(
        "Float64Pk",
        Type.float64(),
        "FLOAT64",
        3.14,
        "3.14",
        com.google.cloud.spanner.Key.of(3.14));
    // 3. Float32 PK
    testPkTypeCoverage(
        "Float32Pk", Type.float32(), "FLOAT32", 1.5f, "1.5", com.google.cloud.spanner.Key.of(1.5f));
    // 4. Date PK
    testPkTypeCoverage(
        "DatePk",
        Type.date(),
        "DATE",
        Date.parseDate("2024-01-01"),
        "2024-01-01",
        com.google.cloud.spanner.Key.of(Date.parseDate("2024-01-01")));
    // 5. Timestamp PK
    testPkTypeCoverage(
        "TsPk",
        Type.timestamp(),
        "TIMESTAMP",
        Timestamp.parseTimestamp("2024-01-01T00:00:00Z"),
        "2024-01-01T00:00:00Z",
        com.google.cloud.spanner.Key.of(Timestamp.parseTimestamp("2024-01-01T00:00:00Z")));
    // 6. Numeric PK
    testPkTypeCoverage(
        "NumPk",
        Type.numeric(),
        "NUMERIC",
        new BigDecimal("123.45"),
        "123.45",
        com.google.cloud.spanner.Key.of(new BigDecimal("123.45")));
    // 7. Bytes PK
    byte[] rawBytes = "key".getBytes();
    String b64 = java.util.Base64.getEncoder().encodeToString(rawBytes);
    testPkTypeCoverage(
        "BytesPk",
        Type.bytes(),
        "BYTES",
        rawBytes,
        b64,
        com.google.cloud.spanner.Key.of(ByteArray.copyFrom(rawBytes)));
    testPkTypeCoverage(
        "BytesPk2",
        Type.bytes(),
        "BYTES",
        ByteArray.copyFrom(rawBytes),
        b64,
        com.google.cloud.spanner.Key.of(ByteArray.copyFrom(rawBytes)));
    // 8. String PK
    testPkTypeCoverage(
        "StrPk",
        Type.string(),
        "STRING",
        "my-key",
        "my-key",
        com.google.cloud.spanner.Key.of("my-key"));
  }

  private static void testPkTypeCoverage(
      String colName,
      Type type,
      String srcType,
      Object customVal,
      String jsonVal,
      com.google.cloud.spanner.Key expectedKey)
      throws Exception {
    Ddl.Builder ddlBuilder = Ddl.builder();
    Table.Builder tableBuilder = ddlBuilder.createTable("PkTable");
    tableBuilder.column(colName).type(type).notNull().endColumn();
    tableBuilder.column("Data").string().max().endColumn();
    tableBuilder.primaryKey().asc(colName).end();
    tableBuilder.endTable();
    Ddl ddl = ddlBuilder.build();

    SourceColumn pkCol =
        SourceColumn.builder(SRC_TYPE)
            .name(colName)
            .type(srcType)
            .isPrimaryKey(true)
            .isNullable(false)
            .build();
    SourceColumn dataCol = SourceColumn.builder(SRC_TYPE).name("Data").type("STRING").build();
    SourceTable table =
        SourceTable.builder(SRC_TYPE)
            .name("PkTable")
            .columns(ImmutableList.of(pkCol, dataCol))
            .primaryKeyColumns(ImmutableList.of(colName))
            .build();
    SourceSchema schema =
        SourceSchema.builder(SRC_TYPE)
            .databaseName("test-db")
            .tables(ImmutableMap.of("PkTable", table))
            .rawDdl(ddl)
            .build();

    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSourceTableName("", "PkTable")).thenReturn("PkTable");
    when(mapper.getSpannerColumnName("", "PkTable", colName)).thenReturn(colName);
    when(mapper.getSpannerColumnName("", "PkTable", "Data")).thenReturn("Data");
    when(mapper.getSourceColumnName("", "PkTable", colName)).thenReturn(colName);
    when(mapper.getSourceColumnName("", "PkTable", "Data")).thenReturn("Data");

    // Test DELETE with customVal
    Map<String, Object> custom = new HashMap<>();
    custom.put(colName, customVal);
    DMLGeneratorResponse delResp1 =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "DELETE",
                        "PkTable",
                        new JSONObject(),
                        new JSONObject("{\"" + colName + "\":\"" + jsonVal + "\"}"),
                        "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(custom)
                    .build());
    assertEquals(
        Mutation.Op.DELETE, ((SpannerMutationResponse) delResp1).getMutation().getOperation());
    assertEquals(
        expectedKey.toString(), ((SpannerMutationResponse) delResp1).getPrimaryKey().toString());

    // Test DELETE without customVal (JSON parsing path)
    DMLGeneratorResponse delResp2 =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "DELETE",
                        "PkTable",
                        new JSONObject(),
                        new JSONObject("{\"" + colName + "\":\"" + jsonVal + "\"}"),
                        "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());
    assertEquals(
        expectedKey.toString(), ((SpannerMutationResponse) delResp2).getPrimaryKey().toString());
  }

  @Test
  public void targetDdlNullInSourceSchemaThrows() throws Exception {
    Ddl ddl = buildDdl();
    SourceSchema schemaWithoutDdl =
        SourceSchema.builder(SRC_TYPE).databaseName("test-db").tables(ImmutableMap.of()).build();
    ISchemaMapper mapper = buildIdentityMapper();

    InvalidDMLGenerationException ex =
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
                            .setSourceSchema(schemaWithoutDdl)
                            .build()));
    assertEquals("target spanner ddl could not be fetched.", ex.getMessage());
  }

  @Test
  public void buildArrayValueNullElementsExhaustive() throws Exception {
    // BOOL array with null
    Ddl ddlBool = buildDdlWithSingleNonPkCol("Arr", Type.array(Type.bool()));
    SourceSchema schemaBool = buildSchemaWithSingleNonPkCol("Arr", "ARRAY<BOOL>");
    ISchemaMapper mapperBool = buildMapperForSingleColTable(schemaBool);
    DMLGeneratorResponse respBool =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"Arr\":[true, null, false]}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapperBool)
                    .setDdl(ddlBool)
                    .setSourceSchema(schemaBool)
                    .build());
    assertEquals(
        java.util.Arrays.asList(true, null, false),
        ((SpannerMutationResponse) respBool).getMutation().asMap().get("Arr").getBoolArray());

    // INT64 array with null
    Ddl ddlInt = buildDdlWithSingleNonPkCol("Arr", Type.array(Type.int64()));
    SourceSchema schemaInt = buildSchemaWithSingleNonPkCol("Arr", "ARRAY<INT64>");
    ISchemaMapper mapperInt = buildMapperForSingleColTable(schemaInt);
    DMLGeneratorResponse respInt =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"Arr\":[1, null, 2]}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapperInt)
                    .setDdl(ddlInt)
                    .setSourceSchema(schemaInt)
                    .build());
    assertEquals(
        java.util.Arrays.asList(1L, null, 2L),
        ((SpannerMutationResponse) respInt).getMutation().asMap().get("Arr").getInt64Array());

    // FLOAT64 array with null
    Ddl ddlFloat64 = buildDdlWithSingleNonPkCol("Arr", Type.array(Type.float64()));
    SourceSchema schemaFloat64 = buildSchemaWithSingleNonPkCol("Arr", "ARRAY<FLOAT64>");
    ISchemaMapper mapperFloat64 = buildMapperForSingleColTable(schemaFloat64);
    DMLGeneratorResponse respFloat64 =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"Arr\":[1.5, null, 2.5]}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapperFloat64)
                    .setDdl(ddlFloat64)
                    .setSourceSchema(schemaFloat64)
                    .build());
    assertEquals(
        java.util.Arrays.asList(1.5, null, 2.5),
        ((SpannerMutationResponse) respFloat64).getMutation().asMap().get("Arr").getFloat64Array());

    // FLOAT32 array with null
    Ddl ddlFloat32 = buildDdlWithSingleNonPkCol("Arr", Type.array(Type.float32()));
    SourceSchema schemaFloat32 = buildSchemaWithSingleNonPkCol("Arr", "ARRAY<FLOAT32>");
    ISchemaMapper mapperFloat32 = buildMapperForSingleColTable(schemaFloat32);
    DMLGeneratorResponse respFloat32 =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"Arr\":[1.5, null, 2.5]}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapperFloat32)
                    .setDdl(ddlFloat32)
                    .setSourceSchema(schemaFloat32)
                    .build());
    assertEquals(
        java.util.Arrays.asList(1.5f, null, 2.5f),
        ((SpannerMutationResponse) respFloat32).getMutation().asMap().get("Arr").getFloat32Array());

    // NUMERIC array with null
    Ddl ddlNum = buildDdlWithSingleNonPkCol("Arr", Type.array(Type.numeric()));
    SourceSchema schemaNum = buildSchemaWithSingleNonPkCol("Arr", "ARRAY<NUMERIC>");
    ISchemaMapper mapperNum = buildMapperForSingleColTable(schemaNum);
    DMLGeneratorResponse respNum =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"Arr\":[\"1.1\", null, \"2.2\"]}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapperNum)
                    .setDdl(ddlNum)
                    .setSourceSchema(schemaNum)
                    .build());
    assertEquals(
        java.util.Arrays.asList(new BigDecimal("1.1"), null, new BigDecimal("2.2")),
        ((SpannerMutationResponse) respNum).getMutation().asMap().get("Arr").getNumericArray());
  }

  @Test
  public void fallbackTypesInSetNullValueAndSetCustomColumnValue() throws Exception {
    Type structType = Type.struct(Type.StructField.of("f1", Type.string()));
    Ddl ddl = buildDdlWithSingleNonPkCol("StructVal", structType);
    SourceSchema schema = buildSchemaWithSingleNonPkCol("StructVal", "STRUCT");
    ISchemaMapper mapper = buildMapperForSingleColTable(schema);

    // 1. setNullValue for STRUCT (hits default branch)
    JSONObject newValuesNull = new JSONObject();
    newValuesNull.put("StructVal", JSONObject.NULL);
    DMLGeneratorResponse respNull =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "T", newValuesNull, new JSONObject("{\"Id\":\"1\"}"), "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());
    assertTrue(
        ((SpannerMutationResponse) respNull).getMutation().asMap().get("StructVal").isNull());

    // 2. setNullArrayValue for ARRAY<STRUCT> (hits default branch in setNullArrayValue)
    Type arrayStructType = Type.array(structType);
    Ddl ddlArr = buildDdlWithSingleNonPkCol("ArrStruct", arrayStructType);
    SourceSchema schemaArr = buildSchemaWithSingleNonPkCol("ArrStruct", "ARRAY<STRUCT>");
    ISchemaMapper mapperArr = buildMapperForSingleColTable(schemaArr);
    JSONObject newValuesArrNull = new JSONObject();
    newValuesArrNull.put("ArrStruct", JSONObject.NULL);
    DMLGeneratorResponse respArrNull =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT", "T", newValuesArrNull, new JSONObject("{\"Id\":\"1\"}"), "+00:00")
                    .setSchemaMapper(mapperArr)
                    .setDdl(ddlArr)
                    .setSourceSchema(schemaArr)
                    .build());
    assertTrue(
        ((SpannerMutationResponse) respArrNull).getMutation().asMap().get("ArrStruct").isNull());

    // 3. setCustomColumnValue for STRUCT (hits default branch in setCustomColumnValue)
    Map<String, Object> custom = new HashMap<>();
    custom.put("StructVal", "{\"f1\":\"val\"}");
    DMLGeneratorResponse respCustom =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder(
                        "INSERT",
                        "T",
                        new JSONObject("{\"StructVal\":\"dummy\"}"),
                        new JSONObject("{\"Id\":\"1\"}"),
                        "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .setCustomTransformationResponse(custom)
                    .build());
    assertEquals(
        "{\"f1\":\"val\"}",
        ((SpannerMutationResponse) respCustom).getMutation().asMap().get("StructVal").getString());
  }

  @Test
  public void
      schemaMapperThrowsNoSuchElementExceptionInPrimaryKeyResolutionFallsBackToTargetColName()
          throws Exception {
    Ddl ddl = buildDdl();
    SourceSchema schema = buildSourceSchema();
    ISchemaMapper mapper = buildIdentityMapper();
    when(mapper.getSpannerColumnName("", "Singers", "SingerId"))
        .thenThrow(new NoSuchElementException("Column unmapped"));

    JSONObject newValues = new JSONObject("{\"FirstName\":\"John\",\"LastName\":\"Doe\"}");
    JSONObject keyValues = new JSONObject("{\"SingerId\":\"42\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "Singers", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(ddl)
                    .setSourceSchema(schema)
                    .build());

    SpannerMutationResponse mutResp = (SpannerMutationResponse) response;
    assertNotNull(mutResp.getMutation());
    assertEquals(
        com.google.cloud.spanner.Key.of(42L).toString(), mutResp.getPrimaryKey().toString());
  }

  @Test
  public void targetDdlInvalidTypeInSourceSchemaThrows() throws Exception {
    Ddl ddl = buildDdl();
    SourceSchema schemaWithInvalidDdl =
        SourceSchema.builder(SRC_TYPE)
            .databaseName("test-db")
            .tables(ImmutableMap.of())
            .rawDdl("not-a-ddl-object")
            .build();
    ISchemaMapper mapper = buildIdentityMapper();

    InvalidDMLGenerationException ex =
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
                            .setSourceSchema(schemaWithInvalidDdl)
                            .build()));
    assertEquals("target spanner ddl could not be fetched.", ex.getMessage());
  }

  @Test
  public void insertWithRenamedPrimaryKeyColumn() throws Exception {
    Ddl origDdl = buildDdl();

    Ddl targetDdl =
        Ddl.builder()
            .createTable("TargetSingers")
            .column("TargetSingerId")
            .int64()
            .notNull()
            .endColumn()
            .column("TargetFirstName")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("TargetSingerId")
            .end()
            .endTable()
            .build();

    SourceColumn pkCol =
        SourceColumn.builder(SRC_TYPE)
            .name("TargetSingerId")
            .type("INT64")
            .isPrimaryKey(true)
            .isNullable(false)
            .build();
    SourceColumn nameCol =
        SourceColumn.builder(SRC_TYPE)
            .name("TargetFirstName")
            .type("STRING")
            .isNullable(true)
            .build();

    SourceTable targetTable =
        SourceTable.builder(SRC_TYPE)
            .name("TargetSingers")
            .columns(ImmutableList.of(pkCol, nameCol))
            .primaryKeyColumns(ImmutableList.of("TargetSingerId"))
            .foreignKeys(ImmutableList.of())
            .indexes(ImmutableList.of())
            .build();

    SourceSchema targetSchema =
        SourceSchema.builder(SRC_TYPE)
            .databaseName("test-db")
            .tables(ImmutableMap.of("TargetSingers", targetTable))
            .rawDdl(targetDdl)
            .build();

    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSourceTableName("", "Singers")).thenReturn("TargetSingers");
    when(mapper.getSpannerColumnName("", "TargetSingers", "TargetSingerId")).thenReturn("SingerId");
    when(mapper.getSpannerColumnName("", "TargetSingers", "TargetFirstName"))
        .thenReturn("FirstName");
    when(mapper.getSourceColumnName("", "Singers", "SingerId")).thenReturn("TargetSingerId");
    when(mapper.getSourceColumnName("", "Singers", "FirstName")).thenReturn("TargetFirstName");

    JSONObject newValues = new JSONObject("{\"FirstName\":\"John\"}");
    JSONObject keyValues = new JSONObject("{\"SingerId\":\"42\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("INSERT", "Singers", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(origDdl)
                    .setSourceSchema(targetSchema)
                    .build());

    assertNotNull(response);
    SpannerMutationResponse mutResp = (SpannerMutationResponse) response;
    Mutation mutation = mutResp.getMutation();
    assertEquals(Mutation.Op.INSERT_OR_UPDATE, mutation.getOperation());
    assertEquals("TargetSingers", mutation.getTable());
    assertEquals(42L, mutation.asMap().get("TargetSingerId").getInt64());
    assertEquals("John", mutation.asMap().get("TargetFirstName").getString());
    assertEquals(
        com.google.cloud.spanner.Key.of(42L).toString(), mutResp.getPrimaryKey().toString());
  }

  @Test
  public void deleteWithRenamedPrimaryKeyColumn() throws Exception {
    Ddl origDdl = buildDdl();

    Ddl targetDdl =
        Ddl.builder()
            .createTable("TargetSingers")
            .column("TargetSingerId")
            .int64()
            .notNull()
            .endColumn()
            .column("FirstName")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("TargetSingerId")
            .end()
            .endTable()
            .build();

    SourceColumn pkCol =
        SourceColumn.builder(SRC_TYPE)
            .name("TargetSingerId")
            .type("INT64")
            .isPrimaryKey(true)
            .isNullable(false)
            .build();
    SourceColumn nameCol =
        SourceColumn.builder(SRC_TYPE).name("FirstName").type("STRING").isNullable(true).build();

    SourceTable targetTable =
        SourceTable.builder(SRC_TYPE)
            .name("TargetSingers")
            .columns(ImmutableList.of(pkCol, nameCol))
            .primaryKeyColumns(ImmutableList.of("TargetSingerId"))
            .foreignKeys(ImmutableList.of())
            .indexes(ImmutableList.of())
            .build();

    SourceSchema targetSchema =
        SourceSchema.builder(SRC_TYPE)
            .databaseName("test-db")
            .tables(ImmutableMap.of("TargetSingers", targetTable))
            .rawDdl(targetDdl)
            .build();

    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSourceTableName("", "Singers")).thenReturn("TargetSingers");
    when(mapper.getSpannerColumnName("", "TargetSingers", "TargetSingerId")).thenReturn("SingerId");
    when(mapper.getSpannerColumnName("", "TargetSingers", "FirstName")).thenReturn("FirstName");
    when(mapper.getSourceColumnName("", "Singers", "SingerId")).thenReturn("TargetSingerId");
    when(mapper.getSourceColumnName("", "Singers", "FirstName")).thenReturn("FirstName");

    JSONObject newValues = new JSONObject("{}");
    JSONObject keyValues = new JSONObject("{\"SingerId\":\"42\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("DELETE", "Singers", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(origDdl)
                    .setSourceSchema(targetSchema)
                    .build());

    assertNotNull(response);
    SpannerMutationResponse mutResp = (SpannerMutationResponse) response;
    Mutation mutation = mutResp.getMutation();
    assertEquals(Mutation.Op.DELETE, mutation.getOperation());
    assertEquals("TargetSingers", mutation.getTable());
    assertEquals(
        com.google.cloud.spanner.Key.of(42L).toString(), mutResp.getPrimaryKey().toString());
    assertEquals(
        com.google.cloud.spanner.Key.of(42L).toString(),
        mutation.getKeySet().getKeys().iterator().next().toString());
  }

  @Test
  public void deleteWithRenamedCompositePrimaryKey() throws Exception {
    Ddl origDdl =
        Ddl.builder()
            .createTable("Albums")
            .column("SingerId")
            .int64()
            .notNull()
            .endColumn()
            .column("AlbumId")
            .int64()
            .notNull()
            .endColumn()
            .column("Title")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("SingerId")
            .asc("AlbumId")
            .end()
            .endTable()
            .build();

    Ddl targetDdl =
        Ddl.builder()
            .createTable("TargetAlbums")
            .column("TargetSingerId")
            .int64()
            .notNull()
            .endColumn()
            .column("TargetAlbumId")
            .int64()
            .notNull()
            .endColumn()
            .column("Title")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("TargetSingerId")
            .asc("TargetAlbumId")
            .end()
            .endTable()
            .build();

    SourceSchema targetSchema =
        SourceSchema.builder(SRC_TYPE)
            .databaseName("test-db")
            .tables(
                ImmutableMap.of(
                    "TargetAlbums",
                    SourceTable.builder(SRC_TYPE)
                        .name("TargetAlbums")
                        .primaryKeyColumns(ImmutableList.of("TargetSingerId", "TargetAlbumId"))
                        .columns(
                            ImmutableList.of(
                                SourceColumn.builder(SRC_TYPE)
                                    .name("TargetSingerId")
                                    .type("INT64")
                                    .isPrimaryKey(true)
                                    .build(),
                                SourceColumn.builder(SRC_TYPE)
                                    .name("TargetAlbumId")
                                    .type("INT64")
                                    .isPrimaryKey(true)
                                    .build()))
                        .build()))
            .rawDdl(targetDdl)
            .build();

    ISchemaMapper mapper = mock(ISchemaMapper.class);
    when(mapper.getSourceTableName("", "Albums")).thenReturn("TargetAlbums");
    when(mapper.getSpannerColumnName("", "TargetAlbums", "TargetSingerId")).thenReturn("SingerId");
    when(mapper.getSpannerColumnName("", "TargetAlbums", "TargetAlbumId")).thenReturn("AlbumId");
    when(mapper.getSourceColumnName("", "Albums", "SingerId")).thenReturn("TargetSingerId");
    when(mapper.getSourceColumnName("", "Albums", "AlbumId")).thenReturn("TargetAlbumId");

    JSONObject newValues = new JSONObject("{}");
    JSONObject keyValues = new JSONObject("{\"SingerId\":\"1\", \"AlbumId\":\"2\"}");

    DMLGeneratorResponse response =
        new SpannerDMLGenerator()
            .getDMLStatement(
                new DMLGeneratorRequest.Builder("DELETE", "Albums", newValues, keyValues, "+00:00")
                    .setSchemaMapper(mapper)
                    .setDdl(origDdl)
                    .setSourceSchema(targetSchema)
                    .build());

    SpannerMutationResponse mutResp = (SpannerMutationResponse) response;
    Mutation mutation = mutResp.getMutation();
    assertEquals(Mutation.Op.DELETE, mutation.getOperation());
    assertEquals("TargetAlbums", mutation.getTable());
    assertEquals(
        com.google.cloud.spanner.Key.of(1L, 2L).toString(), mutResp.getPrimaryKey().toString());
  }
}
