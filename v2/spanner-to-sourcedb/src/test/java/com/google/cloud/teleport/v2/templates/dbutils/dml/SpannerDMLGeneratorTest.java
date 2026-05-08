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
}
