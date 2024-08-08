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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaTestUtils;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.templates.RowContext;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform.WriteDLQ;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

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
    DeadLetterQueue dlq = DeadLetterQueue.create("testDir", spannerDdl);
    assertEquals("testDir", dlq.getDlqDirectory());

    assertTrue(dlq.getDlqTransform() instanceof WriteDLQ);

    assertTrue(((WriteDLQ) dlq.getDlqTransform()).dlqDirectory().endsWith("testDir/"));
  }

  @Test
  public void testCreateLogDlq() {
    DeadLetterQueue dlq = DeadLetterQueue.create("LOG", spannerDdl);
    assertEquals("LOG", dlq.getDlqDirectory());
    assertTrue(dlq.getDlqTransform() instanceof DeadLetterQueue.WriteToLog);
  }

  @Test
  public void testCreateIgnoreDlq() {
    DeadLetterQueue dlq = DeadLetterQueue.create("IGNORE", spannerDdl);
    assertEquals("IGNORE", dlq.getDlqDirectory());
    assertNull(dlq.getDlqTransform());
  }

  @Test(expected = RuntimeException.class)
  public void testNoDlqDirectory() {
    DeadLetterQueue.create(null, spannerDdl).getDlqDirectory();
  }

  @Test
  public void testFilteredRowsToLog() {
    DeadLetterQueue dlq = DeadLetterQueue.create("LOG", spannerDdl);
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
  public void testFailedRowsToLog() {
    DeadLetterQueue dlq = DeadLetterQueue.create("LOG", spannerDdl);
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
  public void testRowContextToDlqElement() {
    final String testTable = "srcTable";
    var schemaRef = SchemaTestUtils.generateSchemaReference("public", "mydb");
    SourceTableSchema schema = SchemaTestUtils.generateTestTableSchema(testTable);

    DeadLetterQueue dlq = DeadLetterQueue.create("testDir", spannerDdl);

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
  }

  @Test
  public void testMutationToDlqElement() {
    DeadLetterQueue dlq = DeadLetterQueue.create("testDir", spannerDdl);
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
}
