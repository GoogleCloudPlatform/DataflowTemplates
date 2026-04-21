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
package com.google.cloud.teleport.v2.templates.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.spanner.SpannerDataWriter.SpannerAccessorFactory;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link SpannerDataWriter}. */
@RunWith(JUnit4.class)
public class SpannerDataWriterTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Mock private SpannerAccessor mockAccessor;
  @Mock private DatabaseClient mockDatabaseClient;

  private SpannerAccessorFactory factory;

  private SpannerConfig testConfig() {
    return SpannerConfig.create()
        .withProjectId(StaticValueProvider.of("project"))
        .withInstanceId(StaticValueProvider.of("instance"))
        .withDatabaseId(StaticValueProvider.of("database"));
  }

  @Before
  public void setUp() {
    when(mockAccessor.getDatabaseClient()).thenReturn(mockDatabaseClient);
    factory = config -> mockAccessor;
  }

  private DataGeneratorTable simpleTable() {
    DataGeneratorColumn idCol =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isGenerated(false)
            .size(null)
            .precision(null)
            .scale(null)
            .build();
    DataGeneratorColumn nameCol =
        DataGeneratorColumn.builder()
            .name("name")
            .logicalType(LogicalType.STRING)
            .isNullable(true)
            .isGenerated(false)
            .size(128L)
            .precision(null)
            .scale(null)
            .build();
    return DataGeneratorTable.builder()
        .name("Users")
        .columns(ImmutableList.of(idCol, nameCol))
        .primaryKeys(ImmutableList.of("id"))
        .foreignKeys(ImmutableList.of())
        .uniqueKeys(ImmutableList.of())
        .isRoot(true)
        .insertQps(0)
        .updateQps(0)
        .deleteQps(0)
        .recordsPerTick(1.0)
        .build();
  }

  private Row simpleRow(Long id, String name) {
    Schema schema =
        Schema.builder()
            .addNullableField("id", Schema.FieldType.INT64)
            .addNullableField("name", Schema.FieldType.STRING)
            .build();
    return Row.withSchema(schema).addValues(id, name).build();
  }

  private SpannerDataWriter writer() {
    return new SpannerDataWriter(testConfig(), factory);
  }

  @Test
  public void testInsert_emptyRowsIsNoOp() {
    SpannerDataWriter w = writer();
    w.insert(ImmutableList.of(), simpleTable(), "", 10);
    verify(mockDatabaseClient, never()).writeAtLeastOnce(any());
  }

  @Test
  public void testInsert_nullRowsIsNoOp() {
    SpannerDataWriter w = writer();
    w.insert(null, simpleTable(), "", 10);
    verify(mockDatabaseClient, never()).writeAtLeastOnce(any());
  }

  @Test
  public void testInsert_nullTableThrows() {
    SpannerDataWriter w = writer();
    assertThrows(
        IllegalArgumentException.class,
        () -> w.insert(ImmutableList.of(simpleRow(1L, "a")), null, "", 10));
  }

  @Test
  public void testInsert_buildsInsertOrUpdateMutation() {
    SpannerDataWriter w = writer();
    w.insert(ImmutableList.of(simpleRow(1L, "a"), simpleRow(2L, "b")), simpleTable(), "", 10);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Iterable<Mutation>> muts = ArgumentCaptor.forClass(Iterable.class);
    verify(mockDatabaseClient).writeAtLeastOnce(muts.capture());
    List<Mutation> list = ImmutableList.copyOf(muts.getValue());
    assertThat(list).hasSize(2);
    for (Mutation m : list) {
      // INSERT is treated as INSERT_OR_UPDATE so that transient retries are idempotent.
      assertThat(m.getOperation()).isEqualTo(Mutation.Op.INSERT_OR_UPDATE);
      assertThat(m.getTable()).isEqualTo("Users");
    }
  }

  @Test
  public void testUpdate_buildsInsertOrUpdateMutation() {
    SpannerDataWriter w = writer();
    w.update(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "", 10);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Iterable<Mutation>> muts = ArgumentCaptor.forClass(Iterable.class);
    verify(mockDatabaseClient).writeAtLeastOnce(muts.capture());
    Mutation m = ImmutableList.copyOf(muts.getValue()).get(0);
    assertThat(m.getOperation()).isEqualTo(Mutation.Op.INSERT_OR_UPDATE);
  }

  @Test
  public void testDelete_buildsDeleteMutation() {
    SpannerDataWriter w = writer();
    w.delete(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "", 10);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Iterable<Mutation>> muts = ArgumentCaptor.forClass(Iterable.class);
    verify(mockDatabaseClient).writeAtLeastOnce(muts.capture());
    Mutation m = ImmutableList.copyOf(muts.getValue()).get(0);
    assertThat(m.getOperation()).isEqualTo(Mutation.Op.DELETE);
    assertThat(m.getTable()).isEqualTo("Users");
  }

  @Test
  public void testInsert_runtimeExceptionPropagates() {
    when(mockDatabaseClient.writeAtLeastOnce(any()))
        .thenThrow(new RuntimeException("spanner exception"));
    SpannerDataWriter w = writer();
    RuntimeException ex =
        assertThrows(
            RuntimeException.class,
            () -> w.insert(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "", 10));
    assertThat(ex).hasMessageThat().contains("spanner exception");
  }

  @Test
  public void testDelete_missingPrimaryKeyThrows() {
    Schema schema =
        Schema.builder()
            .addNullableField("id", Schema.FieldType.INT64)
            .addNullableField("name", Schema.FieldType.STRING)
            .build();
    Row row = Row.withSchema(schema).addValues(null, "a").build();
    SpannerDataWriter w = writer();
    assertThrows(
        IllegalStateException.class, () -> w.delete(ImmutableList.of(row), simpleTable(), "", 10));
  }

  @Test
  public void testDelete_noPrimaryKeyTableThrows() {
    DataGeneratorTable noPk = simpleTable().toBuilder().primaryKeys(ImmutableList.of()).build();
    SpannerDataWriter w = writer();
    assertThrows(
        IllegalStateException.class,
        () -> w.delete(ImmutableList.of(simpleRow(1L, "a")), noPk, "", 10));
  }

  private static DataGeneratorColumn col(String name, LogicalType type) {
    return DataGeneratorColumn.builder()
        .name(name)
        .logicalType(type)
        .isNullable(true)
        .isGenerated(false)
        .size(null)
        .precision(null)
        .scale(null)
        .build();
  }

  private static DataGeneratorTable singleColTable(DataGeneratorColumn c) {
    return DataGeneratorTable.builder()
        .name("t")
        .columns(ImmutableList.of(c))
        .primaryKeys(ImmutableList.of(c.name()))
        .foreignKeys(ImmutableList.of())
        .uniqueKeys(ImmutableList.of())
        .isRoot(true)
        .insertQps(0)
        .updateQps(0)
        .deleteQps(0)
        .recordsPerTick(1.0)
        .build();
  }

  private static Row rowFor(String name, Schema.FieldType type, Object v) {
    Schema s = Schema.builder().addNullableField(name, type).build();
    return Row.withSchema(s).addValue(v).build();
  }

  @Test
  public void testRowToMutation_stringValue() {
    DataGeneratorColumn c = col("s", LogicalType.STRING);
    Mutation m =
        writer()
            .rowToMutation(
                singleColTable(c),
                rowFor("s", Schema.FieldType.STRING, "foo"),
                SpannerDataWriter.MutationType.INSERT);
    assertThat(m.asMap().get("s").getString()).isEqualTo("foo");
  }

  @Test
  public void testRowToMutation_jsonValue() {
    DataGeneratorColumn c = col("j", LogicalType.JSON);
    Mutation m =
        writer()
            .rowToMutation(
                singleColTable(c),
                rowFor("j", Schema.FieldType.STRING, "{\"a\":1}"),
                SpannerDataWriter.MutationType.INSERT);
    assertThat(m.asMap().get("j")).isEqualTo(Value.json("{\"a\":1}"));
  }

  @Test
  public void testRowToMutation_intValue() {
    DataGeneratorColumn c = col("i", LogicalType.INT64);
    Mutation m =
        writer()
            .rowToMutation(
                singleColTable(c),
                rowFor("i", Schema.FieldType.INT64, 42L),
                SpannerDataWriter.MutationType.INSERT);
    assertThat(m.asMap().get("i").getInt64()).isEqualTo(42L);
  }

  @Test
  public void testRowToMutation_doubleValue() {
    DataGeneratorColumn c = col("f", LogicalType.FLOAT64);
    Mutation m =
        writer()
            .rowToMutation(
                singleColTable(c),
                rowFor("f", Schema.FieldType.DOUBLE, 2.5),
                SpannerDataWriter.MutationType.INSERT);
    assertThat(m.asMap().get("f").getFloat64()).isEqualTo(2.5);
  }

  @Test
  public void testRowToMutation_booleanValue() {
    DataGeneratorColumn c = col("b", LogicalType.BOOLEAN);
    Mutation m =
        writer()
            .rowToMutation(
                singleColTable(c),
                rowFor("b", Schema.FieldType.BOOLEAN, true),
                SpannerDataWriter.MutationType.INSERT);
    assertThat(m.asMap().get("b").getBool()).isTrue();
  }

  @Test
  public void testRowToMutation_numericValue() {
    DataGeneratorColumn c = col("n", LogicalType.NUMERIC);
    Mutation m =
        writer()
            .rowToMutation(
                singleColTable(c),
                rowFor("n", Schema.FieldType.DECIMAL, new BigDecimal("1.23")),
                SpannerDataWriter.MutationType.INSERT);
    assertThat(m.asMap().get("n").getNumeric()).isEqualTo(new BigDecimal("1.23"));
  }

  @Test
  public void testRowToMutation_bytesValue() {
    DataGeneratorColumn c = col("bt", LogicalType.BYTES);
    byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
    Mutation m =
        writer()
            .rowToMutation(
                singleColTable(c),
                rowFor("bt", Schema.FieldType.BYTES, bytes),
                SpannerDataWriter.MutationType.INSERT);
    assertThat(m.asMap().get("bt").getBytes()).isEqualTo(ByteArray.copyFrom(bytes));
  }

  @Test
  public void testRowToMutation_dateValue() {
    DataGeneratorColumn c = col("d", LogicalType.DATE);
    DateTime dt = new DateTime(2024, 1, 2, 0, 0).toDateTime(org.joda.time.DateTimeZone.UTC);
    Row row = rowFor("d", Schema.FieldType.DATETIME, dt);
    Mutation m =
        writer().rowToMutation(singleColTable(c), row, SpannerDataWriter.MutationType.INSERT);
    Date expected = Date.fromJavaUtilDate(new java.util.Date(dt.getMillis()));
    assertThat(m.asMap().get("d").getDate()).isEqualTo(expected);
  }

  @Test
  public void testRowToMutation_timestampValue() {
    DataGeneratorColumn c = col("t", LogicalType.TIMESTAMP);
    DateTime dt = new DateTime(1_700_000_000_000L);
    Row row = rowFor("t", Schema.FieldType.DATETIME, dt);
    Mutation m =
        writer().rowToMutation(singleColTable(c), row, SpannerDataWriter.MutationType.INSERT);
    Timestamp expected = Timestamp.ofTimeMicroseconds(dt.getMillis() * 1000L);
    assertThat(m.asMap().get("t").getTimestamp()).isEqualTo(expected);
  }

  @Test
  public void testRowToMutation_nullValuesEmitTypedNulls() {
    // Both columns present but null - the write mutation should still carry typed null values so
    // that Spanner applies the expected NULL update rather than rejecting the mutation.
    Schema s =
        Schema.builder()
            .addNullableField("id", Schema.FieldType.INT64)
            .addNullableField("name", Schema.FieldType.STRING)
            .build();
    Row row = Row.withSchema(s).addValues(null, null).build();
    Mutation m = writer().rowToMutation(simpleTable(), row, SpannerDataWriter.MutationType.UPDATE);
    assertThat(m.asMap().get("id").isNull()).isTrue();
    assertThat(m.asMap().get("name").isNull()).isTrue();
  }

  @Test
  public void testRowToMutation_missingFieldBindsTypedNull() {
    // Row schema is missing "name" - writer should bind a typed null for it.
    Schema s = Schema.builder().addNullableField("id", Schema.FieldType.INT64).build();
    Row row = Row.withSchema(s).addValue(1L).build();
    Mutation m = writer().rowToMutation(simpleTable(), row, SpannerDataWriter.MutationType.INSERT);
    assertThat(m.asMap().get("id").getInt64()).isEqualTo(1L);
    assertThat(m.asMap().get("name").isNull()).isTrue();
  }

  @Test
  public void testRowToMutation_skipsGeneratedAndSkippedColumns() {
    DataGeneratorColumn id = col("id", LogicalType.INT64);
    DataGeneratorColumn gen =
        DataGeneratorColumn.builder()
            .name("g")
            .logicalType(LogicalType.STRING)
            .isNullable(true)
            .isGenerated(true)
            .size(null)
            .precision(null)
            .scale(null)
            .build();
    DataGeneratorColumn skipped =
        DataGeneratorColumn.builder()
            .name("s")
            .logicalType(LogicalType.STRING)
            .isNullable(true)
            .isGenerated(false)
            .isSkipped(true)
            .size(null)
            .precision(null)
            .scale(null)
            .build();
    DataGeneratorTable t =
        DataGeneratorTable.builder()
            .name("t")
            .columns(ImmutableList.of(id, gen, skipped))
            .primaryKeys(ImmutableList.of("id"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .isRoot(true)
            .insertQps(0)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();
    Schema schema =
        Schema.builder()
            .addNullableField("id", Schema.FieldType.INT64)
            .addNullableField("g", Schema.FieldType.STRING)
            .addNullableField("s", Schema.FieldType.STRING)
            .build();
    Row row = Row.withSchema(schema).addValues(1L, "ignored", "ignored").build();
    Mutation m = writer().rowToMutation(t, row, SpannerDataWriter.MutationType.INSERT);
    assertThat(m.asMap()).containsKey("id");
    assertThat(m.asMap()).doesNotContainKey("g");
    assertThat(m.asMap()).doesNotContainKey("s");
  }

  @Test
  public void testRowToMutation_deleteKeyWithAllTypes() {
    // Verify that DELETE appending PK values succeeds for each scalar type.
    for (LogicalType type : LogicalType.values()) {
      DataGeneratorColumn c = col("pk", type);
      Object sample;
      Schema.FieldType beamType;
      switch (type) {
        case INT64:
          sample = 1L;
          beamType = Schema.FieldType.INT64;
          break;
        case FLOAT64:
          sample = 1.0;
          beamType = Schema.FieldType.DOUBLE;
          break;
        case BOOLEAN:
          sample = true;
          beamType = Schema.FieldType.BOOLEAN;
          break;
        case BYTES:
          sample = new byte[] {1, 2, 3};
          beamType = Schema.FieldType.BYTES;
          break;
        case DATE:
        case TIMESTAMP:
          sample = new DateTime(1_700_000_000_000L);
          beamType = Schema.FieldType.DATETIME;
          break;
        case NUMERIC:
          sample = new BigDecimal("1.5");
          beamType = Schema.FieldType.DECIMAL;
          break;
        case STRING:
        case JSON:
        default:
          sample = "sample";
          beamType = Schema.FieldType.STRING;
      }
      Row row = rowFor("pk", beamType, sample);
      Mutation m =
          writer().rowToMutation(singleColTable(c), row, SpannerDataWriter.MutationType.DELETE);
      assertThat(m.getOperation()).isEqualTo(Mutation.Op.DELETE);
      assertThat(m.getTable()).isEqualTo("t");
    }
  }

  @Test
  public void testRowToMutation_nullValuesForAllTypes() {
    for (LogicalType type : LogicalType.values()) {
      DataGeneratorColumn c = col("col", type);
      Schema.FieldType beamType;
      switch (type) {
        case INT64:
          beamType = Schema.FieldType.INT64;
          break;
        case FLOAT64:
          beamType = Schema.FieldType.DOUBLE;
          break;
        case BOOLEAN:
          beamType = Schema.FieldType.BOOLEAN;
          break;
        case BYTES:
          beamType = Schema.FieldType.BYTES;
          break;
        case DATE:
        case TIMESTAMP:
          beamType = Schema.FieldType.DATETIME;
          break;
        case NUMERIC:
          beamType = Schema.FieldType.DECIMAL;
          break;
        case STRING:
        case JSON:
        default:
          beamType = Schema.FieldType.STRING;
      }

      Row row = rowFor("col", beamType, null);

      Mutation m =
          writer().rowToMutation(singleColTable(c), row, SpannerDataWriter.MutationType.INSERT);

      assertThat(m.asMap().get("col").isNull()).isTrue();

      switch (type) {
        case INT64:
          assertThat(m.asMap().get("col").getType())
              .isEqualTo(com.google.cloud.spanner.Type.int64());
          break;
        case FLOAT64:
          assertThat(m.asMap().get("col").getType())
              .isEqualTo(com.google.cloud.spanner.Type.float64());
          break;
        case BOOLEAN:
          assertThat(m.asMap().get("col").getType())
              .isEqualTo(com.google.cloud.spanner.Type.bool());
          break;
        case BYTES:
          assertThat(m.asMap().get("col").getType())
              .isEqualTo(com.google.cloud.spanner.Type.bytes());
          break;
        case DATE:
          assertThat(m.asMap().get("col").getType())
              .isEqualTo(com.google.cloud.spanner.Type.date());
          break;
        case TIMESTAMP:
          assertThat(m.asMap().get("col").getType())
              .isEqualTo(com.google.cloud.spanner.Type.timestamp());
          break;
        case NUMERIC:
          assertThat(m.asMap().get("col").getType())
              .isEqualTo(com.google.cloud.spanner.Type.numeric());
          break;
        case STRING:
          assertThat(m.asMap().get("col").getType())
              .isEqualTo(com.google.cloud.spanner.Type.string());
          break;
        case JSON:
          assertThat(m.asMap().get("col").getType())
              .isEqualTo(com.google.cloud.spanner.Type.json());
          break;
      }
    }
  }

  @Test
  public void testLoadSpannerConfig_readsJsonFile() throws Exception {
    File f = tempFolder.newFile("spanner.json");
    String json = "{\"projectId\":\"p\",\"instanceId\":\"i\",\"databaseId\":\"d\"}";
    Files.write(f.toPath(), json.getBytes(StandardCharsets.UTF_8));

    SpannerDataWriter w = new SpannerDataWriter(f.getAbsolutePath(), factory);
    w.ensureInitialized();

    // Force a write to prove that the loaded config was used to obtain a Spanner accessor.
    w.insert(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "", 10);
    verify(mockDatabaseClient).writeAtLeastOnce(any());
  }

  @Test
  public void testLoadSpannerConfig_missingPathThrows() {
    SpannerDataWriter w = new SpannerDataWriter((String) null, factory);
    assertThrows(IllegalArgumentException.class, w::ensureInitialized);

    SpannerDataWriter w2 = new SpannerDataWriter("", factory);
    assertThrows(IllegalArgumentException.class, w2::ensureInitialized);
  }

  @Test
  public void testLoadSpannerConfig_missingFileThrows() {
    SpannerDataWriter w =
        new SpannerDataWriter(
            new File(tempFolder.getRoot(), "does-not-exist.json").getAbsolutePath(), factory);
    assertThrows(RuntimeException.class, w::ensureInitialized);
  }

  @Test
  public void testClose_noopIfNeverInitialized() {
    SpannerDataWriter w = writer();
    w.close(); // should not throw or touch the mock
    verify(mockAccessor, never()).close();
  }

  @Test
  public void testFactoryReturningNullCausesFailureOnWrite() {
    SpannerAccessorFactory nullFactory = config -> null;
    SpannerDataWriter w = new SpannerDataWriter(testConfig(), nullFactory);
    assertThrows(
        NullPointerException.class,
        () -> w.insert(ImmutableList.of(simpleRow(1L, "a")), simpleTable(), "", 10));
  }
}
