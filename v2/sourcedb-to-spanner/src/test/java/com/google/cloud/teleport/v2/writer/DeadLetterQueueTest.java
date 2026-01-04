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
package com.google.cloud.teleport.v2.writer;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaTestUtils;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.avro.GenericRecordTypeConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.cloud.teleport.v2.templates.RowContext;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform.WriteDLQ;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class DeadLetterQueueTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private Ddl spannerDdl;

  @Before
  public void setup() {
    spannerDdl =
        Ddl.builder()
            .createTable("new_cart")
            .column("new_quantity")
            .int64()
            .notNull()
            .endColumn()
            .column("new_user_id")
            .string()
            .size(10)
            .endColumn()
            .primaryKey()
            .asc("new_user_id")
            .asc("new_quantity")
            .end()
            .endTable()
            .createTable("new_people")
            .column("synth_id")
            .int64()
            .notNull()
            .endColumn()
            .column("new_name")
            .string()
            .size(10)
            .endColumn()
            .primaryKey()
            .asc("synth_id")
            .end()
            .endTable()
            .build();
  }

  @Test
  public void testCreateGCSDLQ() {
    DeadLetterQueue dlq =
        DeadLetterQueue.create(
            "testDir",
            spannerDdl,
            new HashMap<>(),
            SQLDialect.MYSQL,
            getIdentityMapper(spannerDdl));
    assertEquals("testDir", dlq.getDlqDirectory());

    assertTrue(dlq.createDLQTransform("testDir") instanceof WriteDLQ);

    assertTrue(((WriteDLQ) dlq.createDLQTransform("testDir")).dlqDirectory().endsWith("testDir/"));

    assertNotNull(((WriteDLQ) dlq.createDLQTransform("testDir")).fileNamePrefix());
  }

  @Test
  public void testCreateLogDlq() {

    final String testTable = "srcTable";
    var schemaRef = SchemaTestUtils.generateSchemaReference("", "mydb");
    SourceTableSchema schema =
        SourceTableSchema.builder(SQLDialect.MYSQL)
            .setTableName(testTable)
            .addSourceColumnNameToSourceColumnType(
                "new_quantity", new SourceColumnType("Bigint", new Long[] {}, null))
            .addSourceColumnNameToSourceColumnType(
                "timestamp_col", new SourceColumnType("timestamp", new Long[] {}, null))
            .build();

    Ddl spannerDdlWithLogicalTypes =
        Ddl.builder()
            .createTable(testTable)
            .column("new_quantity")
            .int64()
            .notNull()
            .endColumn()
            .column("timestamp_col")
            .timestamp()
            .endColumn()
            .endTable()
            .build();

    DeadLetterQueue dlq =
        DeadLetterQueue.create(
            "LOG",
            spannerDdlWithLogicalTypes,
            new HashMap<>(),
            SQLDialect.MYSQL,
            getIdentityMapper(spannerDdlWithLogicalTypes));

    RowContext r1 =
        RowContext.builder()
            .setRow(
                SourceRow.builder(schemaRef, schema, null, 12412435345L)
                    .setField("new_quantity", 42L)
                    .setField("timestamp_col", "1749630376")
                    .build())
            .setErr(new Exception("test exception"))
            .build();
    String expectedDataWithSuccessfulConversion =
        "\"timestamp_col\":\"1970-01-01T00:29:09.630376Z\",\"new_quantity\":42";
    String expectedDataForConversionException =
        "\"timestamp_col\":\"1749630376\",\"new_quantity\":42";
    assertThat(dlq.rowContextToDlqElement(r1).getPayload())
        .contains(expectedDataWithSuccessfulConversion);
    try (MockedStatic<GenericRecordTypeConvertor> genericRecordTypeConvertorMockedStatic =
        Mockito.mockStatic(GenericRecordTypeConvertor.class)) {
      genericRecordTypeConvertorMockedStatic
          .when(
              () ->
                  GenericRecordTypeConvertor.getJsonNodeObjectFromGenericRecord(
                      Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
          .thenThrow(new RuntimeException("testException"));

      assertThat(dlq.rowContextToDlqElement(r1).getPayload())
          .contains(expectedDataForConversionException);
    }
  }

  @Test
  public void testCreateIgnoreDlq() {
    DeadLetterQueue dlq =
        DeadLetterQueue.create(
            "IGNORE", spannerDdl, new HashMap<>(), SQLDialect.MYSQL, getIdentityMapper(spannerDdl));
    assertEquals("IGNORE", dlq.getDlqDirectory());
    assertNull(dlq.createDLQTransform("IGNORE"));
  }

  @Test(expected = RuntimeException.class)
  public void testNoDlqDirectory() {
    DeadLetterQueue dlq =
        DeadLetterQueue.create(
            null, spannerDdl, new HashMap<>(), SQLDialect.MYSQL, getIdentityMapper(spannerDdl));
    dlq.createDLQTransform(null);
  }

  @Test
  public void testFilteredRowsToLog() {
    DeadLetterQueue dlq =
        DeadLetterQueue.create(
            "LOG", spannerDdl, new HashMap<>(), SQLDialect.MYSQL, getIdentityMapper(spannerDdl));
    final String testTable = "srcTable";
    var schemaRef = SchemaTestUtils.generateSchemaReference("public", "mydb");
    SourceTableSchema schema = SchemaTestUtils.generateTestTableSchema(testTable);
    RowContext r1 =
        RowContext.builder()
            .setRow(
                SourceRow.builder(schemaRef, schema, null, 12412435345L)
                    .setField("firstName", "abc")
                    .setField("lastName", "def")
                    .build())
            .setMutation(
                Mutation.newInsertOrUpdateBuilder(testTable)
                    .set("firstName")
                    .to("abc")
                    .set("lastName")
                    .to("def")
                    .build())
            .build();

    PCollection<RowContext> filteredRows = pipeline.apply(Create.of(r1));
    dlq.filteredEventsToDLQ(filteredRows);
    pipeline.run();
  }

  @Test
  public void testLogicalTypes() {
    DeadLetterQueue dlq =
        DeadLetterQueue.create(
            "LOG", spannerDdl, new HashMap<>(), SQLDialect.MYSQL, getIdentityMapper(spannerDdl));
  }

  @Test
  public void testFailedRowsToLog() {
    DeadLetterQueue dlq =
        DeadLetterQueue.create(
            "LOG",
            spannerDdl,
            new HashMap<>(),
            SQLDialect.POSTGRESQL,
            getIdentityMapper(spannerDdl));
    final String testTable = "srcTable";
    var schemaRef = SchemaTestUtils.generateSchemaReference("public", "mydb");
    SourceTableSchema schema = SchemaTestUtils.generateTestTableSchema(testTable);
    RowContext r1 =
        RowContext.builder()
            .setRow(
                SourceRow.builder(schemaRef, schema, null, 12412435345L)
                    .setField("firstName", "abc")
                    .setField("lastName", "def")
                    .build())
            .setMutation(
                Mutation.newInsertOrUpdateBuilder(testTable)
                    .set("firstName")
                    .to("abc")
                    .set("lastName")
                    .to("def")
                    .build())
            .build();

    PCollection<RowContext> failedRows = pipeline.apply(Create.of(r1));
    dlq.failedTransformsToDLQ(failedRows);
    pipeline.run();
  }

  @Test
  public void testRowContextToDlqElementMysql() {
    final String testTable = "srcTable";
    var schemaRef = SchemaTestUtils.generateSchemaReference("public", "mydb");
    SourceTableSchema schema = SchemaTestUtils.generateTestTableSchema(testTable);

    Map<String, String> srcTableToShardId = Map.of(testTable, "migration_id");
    DeadLetterQueue dlq =
        DeadLetterQueue.create(
            "testDir",
            spannerDdl,
            srcTableToShardId,
            SQLDialect.MYSQL,
            getIdentityMapper(spannerDdl));

    RowContext r1 =
        RowContext.builder()
            .setRow(
                SourceRow.builder(schemaRef, schema, null, 12412435345L)
                    .setField("firstName", "abc")
                    .setField("lastName", "def")
                    .setShardId("shard-1")
                    .build())
            .setErr(new Exception("test exception"))
            .build();
    FailsafeElement<String, String> dlqElement = dlq.rowContextToDlqElement(r1);
    assertNotNull(dlqElement);
    assertTrue(dlqElement.getErrorMessage().contains("test exception"));
    assertTrue(dlqElement.getOriginalPayload().contains("\"_metadata_table\":\"srcTable\""));
    assertTrue(dlqElement.getOriginalPayload().contains("\"firstName\":\"abc\""));
    assertTrue(dlqElement.getOriginalPayload().contains("\"lastName\":\"def\""));
    assertTrue(dlqElement.getOriginalPayload().contains("\"migration_id\":\"shard-1\""));
    assertTrue(dlqElement.getOriginalPayload().contains("\"_metadata_source_type\":\"mysql\""));
    assertTrue(
        dlqElement.getOriginalPayload().contains("\"_metadata_change_type\":\"UPDATE-INSERT\""));
  }

  @Test
  public void testRowContextToDlqElementMissingShardIdColumn() {
    var schemaRef = SchemaTestUtils.generateSchemaReference("public", "mydb");
    SourceTableSchema schema = SchemaTestUtils.generateTestTableSchema("nonExistentTable");
    DeadLetterQueue dlq =
        DeadLetterQueue.create(
            "testDir",
            spannerDdl,
            new HashMap<>(),
            SQLDialect.MYSQL,
            getIdentityMapper(spannerDdl));

    RowContext r1 =
        RowContext.builder()
            .setRow(
                SourceRow.builder(schemaRef, schema, null, 12412435345L)
                    .setField("firstName", "abc")
                    .setField("lastName", "def")
                    .setShardId("shard-1")
                    .build())
            .setErr(new Exception("test exception"))
            .build();
    FailsafeElement<String, String> dlqElement = dlq.rowContextToDlqElement(r1);
    assertTrue(dlqElement.getOriginalPayload().contains("\"migration_shard_id\":\"shard-1\""));
  }

  @Test
  public void testRowContextToDlqElementPG() {
    final String testTable = "srcTable";
    var schemaRef = SchemaTestUtils.generateSchemaReference("public", "mydb");
    SourceTableSchema schema = SchemaTestUtils.generateTestTableSchema(testTable);

    DeadLetterQueue dlq =
        DeadLetterQueue.create(
            "testDir",
            spannerDdl,
            new HashMap<>(),
            SQLDialect.POSTGRESQL,
            getIdentityMapper(spannerDdl));

    RowContext r1 =
        RowContext.builder()
            .setRow(
                SourceRow.builder(schemaRef, schema, null, 12412435345L)
                    .setField("firstName", "abc")
                    .setField("lastName", "def")
                    .build())
            .setErr(new Exception("test exception"))
            .build();
    FailsafeElement<String, String> dlqElement = dlq.rowContextToDlqElement(r1);
    assertNotNull(dlqElement);
    assertTrue(dlqElement.getErrorMessage().contains("test exception"));
    assertTrue(dlqElement.getOriginalPayload().contains("\"_metadata_table\":\"srcTable\""));
    assertTrue(dlqElement.getOriginalPayload().contains("\"firstName\":\"abc\""));
    assertTrue(dlqElement.getOriginalPayload().contains("\"lastName\":\"def\""));
    assertTrue(
        dlqElement.getOriginalPayload().contains("\"_metadata_source_type\":\"postgresql\""));
  }

  @Test
  public void testMutationToDlqElement() {
    DeadLetterQueue dlq =
        DeadLetterQueue.create(
            "testDir",
            spannerDdl,
            new HashMap<>(),
            SQLDialect.MYSQL,
            getIdentityMapper(spannerDdl));
    Mutation m =
        Mutation.newInsertOrUpdateBuilder("srcTable")
            .set("firstName")
            .to("abc")
            .set("lastName")
            .to("def")
            .build();
    FailsafeElement<String, String> dlqElement = dlq.mutationToDlqElement(m);
    assertNotNull(dlqElement);
    assertTrue(dlqElement.getOriginalPayload().contains("\"_metadata_table\":\"srcTable\""));
    assertTrue(dlqElement.getOriginalPayload().contains("\"firstName\":\"abc\""));
    assertTrue(dlqElement.getOriginalPayload().contains("\"lastName\":\"def\""));
  }

  @Test
  public void testRowContextToDlqElementWithIntegralTypes() {
    final String testTable = "srcTable";
    var schemaRef = SchemaTestUtils.generateSchemaReference("public", "mydb");
    SourceTableSchema schema =
        SourceTableSchema.builder(SQLDialect.MYSQL)
            .setTableName(testTable)
            .addSourceColumnNameToSourceColumnType(
                "id", new SourceColumnType("Bigint", new Long[] {}, null))
            .addSourceColumnNameToSourceColumnType(
                "bit_col", new SourceColumnType("Bit", new Long[] {}, null))
            .addSourceColumnNameToSourceColumnType(
                "int_col", new SourceColumnType("Integer", new Long[] {}, null))
            .build();

    DeadLetterQueue dlq =
        DeadLetterQueue.create(
            "testDir",
            spannerDdl,
            new HashMap<>(),
            SQLDialect.MYSQL,
            getIdentityMapper(spannerDdl));

    RowContext r1 =
        RowContext.builder()
            .setRow(
                SourceRow.builder(schemaRef, schema, null, 12412435345L)
                    .setField("id", 123L)
                    .setField("bit_col", 1L)
                    .setField("int_col", 456)
                    .build())
            .setErr(new Exception("test exception"))
            .build();
    FailsafeElement<String, String> dlqElement = dlq.rowContextToDlqElement(r1);
    assertNotNull(dlqElement);
    // Verify integral types are not quoted
    assertTrue(dlqElement.getOriginalPayload().contains("\"id\":123"));
    assertTrue(dlqElement.getOriginalPayload().contains("\"bit_col\":1"));
    assertTrue(dlqElement.getOriginalPayload().contains("\"int_col\":456"));

    RowContext r2 =
        RowContext.builder()
            .setRow(
                SourceRow.builder(schemaRef, schema, null, 12412435345L)
                    .setField("id", 123L)
                    .setField("bit_col", null)
                    .setField("int_col", null)
                    .build())
            .setErr(new Exception("test exception"))
            .build();
    FailsafeElement<String, String> dlqElement2 = dlq.rowContextToDlqElement(r2);
    assertNotNull(dlqElement2);
    // Verify null integral types are handled correctly
    assertTrue(dlqElement2.getOriginalPayload().contains("\"id\":123"));
    assertFalse(dlqElement2.getOriginalPayload().contains("\"bit_col\":"));
    assertFalse(dlqElement2.getOriginalPayload().contains("\"int_col\":"));
  }

  @Test
  public void testMutationToDlqElementWithBinaryAndNumericTypes() {
    DeadLetterQueue dlq = DeadLetterQueue.create("testDir", null, null, SQLDialect.MYSQL, null);
    Mutation mutation =
        Mutation.newInsertBuilder("testTable")
            .set("id")
            .to(1)
            .set("binary_col")
            .to(Value.bytes(com.google.cloud.ByteArray.copyFrom("test".getBytes())))
            .set("int_col")
            .to(123)
            .set("float_col")
            .to(1.23)
            .set("numeric_col")
            .to(Value.numeric(new java.math.BigDecimal("123.456")))
            .set("null_col")
            .to(Value.string(null))
            .set("bit_col")
            .to(
                Value.bytes(
                    ByteArray.copyFrom(
                        new java.math.BigInteger("9223372036854775807").toByteArray())))
            .build();

    FailsafeElement<String, String> dlqElement = dlq.mutationToDlqElement(mutation);

    assertNotNull(dlqElement);
    String payload = dlqElement.getOriginalPayload();
    assertTrue(payload.contains("\"id\":1"));
    // "test" in hex is 74657374
    assertTrue(payload.contains("\"binary_col\":\"74657374\""));
    assertTrue(payload.contains("\"int_col\":123"));
    assertTrue(payload.contains("\"float_col\":1.23"));
    assertTrue(payload.contains("\"numeric_col\":123.456"));
    assertTrue(payload.contains("\"bit_col\":\"7fffffffffffffff\""));
  }

  @Test
  public void testMutationToDlqElementWithBytesArray() {
    DeadLetterQueue dlq = DeadLetterQueue.create("testDir", null, null, SQLDialect.MYSQL, null);
    Mutation mutation =
        Mutation.newInsertBuilder("testTable")
            .set("id")
            .to(1)
            .set("bytes_array_col")
            .toBytesArray(
                java.util.List.of(
                    com.google.cloud.ByteArray.copyFrom("test1".getBytes()),
                    com.google.cloud.ByteArray.copyFrom("test2".getBytes())))
            .build();

    FailsafeElement<String, String> dlqElement = dlq.mutationToDlqElement(mutation);

    assertNotNull(dlqElement);
    String payload = dlqElement.getOriginalPayload();
    assertTrue(payload.contains("\"id\":1"));
    // "test1" in hex is 7465737431
    // "test2" in hex is 7465737432
    assertTrue(payload.contains("\"bytes_array_col\":[\"7465737431\",\"7465737432\"]"));
  }

  private static ISchemaMapper getIdentityMapper(Ddl spannerDdl) {
    return new IdentityMapper(spannerDdl);
  }
}
