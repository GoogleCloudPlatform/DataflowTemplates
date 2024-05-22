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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaTestUtils;
import com.google.cloud.teleport.v2.templates.RowContext;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform.WriteDLQ;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class DeadLetterQueueTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testCreateGCSDLQ() {
    DeadLetterQueue dlq = DeadLetterQueue.create("testDir");
    assertEquals("testDir", dlq.getDlqDirectory());

    assertTrue(dlq.getDlqTransform() instanceof WriteDLQ);

    assertTrue(((WriteDLQ) dlq.getDlqTransform()).dlqDirectory().endsWith("testDir/"));
  }

  @Test
  public void testCreateLogDlq() {
    DeadLetterQueue dlq = DeadLetterQueue.create("LOG");
    assertEquals("LOG", dlq.getDlqDirectory());
    assertTrue(dlq.getDlqTransform() instanceof DeadLetterQueue.WriteToLog);
  }

  @Test
  public void testCreateIgnoreDlq() {
    DeadLetterQueue dlq = DeadLetterQueue.create("IGNORE");
    assertEquals("IGNORE", dlq.getDlqDirectory());
    assertNull(dlq.getDlqTransform());
  }

  @Test(expected = RuntimeException.class)
  public void testNoDlqDirectory() {
    DeadLetterQueue.create(null).getDlqDirectory();
  }

  @Test
  public void testFailedRowsToLog() {
    DeadLetterQueue dlq = DeadLetterQueue.create("LOG");
    final String testTable = "srcTable";
    var schema = SchemaTestUtils.generateTestTableSchema(testTable);
    RowContext r1 =
        RowContext.builder()
            .setRow(
                SourceRow.builder(schema, 12412435345L)
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
}
