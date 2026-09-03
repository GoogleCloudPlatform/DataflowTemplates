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
package com.google.cloud.teleport.v2.transforms;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.google.cloud.teleport.v2.transforms.DocumentWithMetadata.OperationType;
import com.google.cloud.teleport.v2.transforms.StatefulDeduplication.StatefulDeduplicationFn;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.bson.Document;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link StatefulDeduplication} and {@link StatefulDeduplicationFn}. */
@RunWith(JUnit4.class)
public class StatefulDeduplicationTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static class TestValueState<T> implements ValueState<T> {
    private T value;

    @Override
    public void write(T input) {
      this.value = input;
    }

    @Override
    public T read() {
      return value;
    }

    @Override
    public ValueState<T> readLater() {
      return this;
    }

    @Override
    public void clear() {
      this.value = null;
    }
  }

  private static class Harness {
    final StatefulDeduplicationFn fn = new StatefulDeduplicationFn();
    final TestValueState<TimestampSortKey> state = new TestValueState<>();
    final List<DocumentWithMetadata> outputs = new ArrayList<>();
    @SuppressWarnings("unchecked")
    final OutputReceiver<DocumentWithMetadata> receiver = mock(OutputReceiver.class);

    Harness() {
      doAnswer(
              invocation -> {
                outputs.add(invocation.getArgument(0));
                return null;
              })
          .when(receiver)
          .output(any(DocumentWithMetadata.class));
    }

    void process(DocumentWithMetadata doc) {
      fn.processElement(KV.of(doc.getDedupKey(), doc), receiver, state);
    }
  }

  private static DocumentWithMetadata createCdc(
      String id, String name, long sec, long subSec, String collection) {
    Document doc = new Document("_id", id).append("name", name);
    TimestampSortKey key = TimestampSortKey.cdc(sec, subSec);
    return DocumentWithMetadata.cdcEvent(
        doc,
        doc.toJson(),
        collection,
        collection,
        OperationType.INSERT,
        key,
        new Document("_id", id).toJson());
  }

  private static DocumentWithMetadata createBackfill(
      String id, String name, long sec, String collection) {
    Document doc = new Document("_id", id).append("name", name);
    TimestampSortKey key = TimestampSortKey.backfill(sec);
    return DocumentWithMetadata.backfillEvent(doc, collection, collection, key);
  }

  @Test
  public void testMonotonicOrdering_inOrderCdcEvents() {
    Harness h = new Harness();

    DocumentWithMetadata e1 = createCdc("1", "v1", 1000L, 1L, "users");
    DocumentWithMetadata e2 = createCdc("1", "v2", 1000L, 2L, "users");
    DocumentWithMetadata e3 = createCdc("1", "v3", 1001L, 0L, "users");

    h.process(e1);
    h.process(e2);
    h.process(e3);

    assertEquals(3, h.outputs.size());
    assertEquals(TimestampSortKey.cdc(1001L, 0L), h.state.read());
  }

  @Test
  public void testOutOrderBackfillAndCdc_backfillArrivesAfterCdc_isDropped() {
    Harness h = new Harness();

    DocumentWithMetadata cdc = createCdc("1", "cdc_version", 1000L, 5L, "users");
    DocumentWithMetadata backfill = createBackfill("1", "snapshot_version", 1000L, "users");

    // Feed CDC first, then Backfill for the same document
    h.process(cdc);
    h.process(backfill);

    assertEquals(1, h.outputs.size());
    assertEquals("cdc_version", h.outputs.get(0).getDocument().getString("name"));
    assertEquals(TimestampSortKey.cdc(1000L, 5L), h.state.read());
  }

  @Test
  public void testCdcArrivesAfterBackfill_bothEmitted() {
    Harness h = new Harness();

    DocumentWithMetadata backfill = createBackfill("1", "snapshot_version", 1000L, "users");
    DocumentWithMetadata cdc = createCdc("1", "cdc_version", 1000L, 1L, "users");

    h.process(backfill);
    h.process(cdc);

    assertEquals(2, h.outputs.size());
    assertEquals("snapshot_version", h.outputs.get(0).getDocument().getString("name"));
    assertEquals("cdc_version", h.outputs.get(1).getDocument().getString("name"));
    assertEquals(TimestampSortKey.cdc(1000L, 1L), h.state.read());
  }

  @Test
  public void testMultipleDocumentKeysIndependent() {
    Harness h1 = new Harness();
    Harness h2 = new Harness();

    DocumentWithMetadata u1 = createCdc("1", "User1", 1000L, 1L, "users");
    DocumentWithMetadata u2 = createCdc("2", "User2", 1000L, 1L, "users");

    h1.process(u1);
    h2.process(u2);

    assertEquals(1, h1.outputs.size());
    assertEquals(1, h2.outputs.size());
  }

  @Test
  public void testDlqReconsumed_droppedWhenNewerEventArrived() {
    Harness h = new Harness();

    DocumentWithMetadata e1 = createCdc("1", "v1", 1000L, 1L, "users");
    DocumentWithMetadata e2 = createCdc("1", "v2", 1005L, 1L, "users");
    DocumentWithMetadata dlqRetryE1 =
        createCdc("1", "v1_retry", 1000L, 1L, "users").withDlqReconsumed(true);

    h.process(e1);
    h.process(e2);
    // dlqRetryE1 arrives late after e2 already advanced state to 1005
    h.process(dlqRetryE1);

    // e1 and e2 pass; dlqRetryE1 is dropped
    assertEquals(2, h.outputs.size());
    assertEquals("v1", h.outputs.get(0).getDocument().getString("name"));
    assertEquals("v2", h.outputs.get(1).getDocument().getString("name"));
  }

  @Test
  public void testDlqReconsumed_acceptedWhenStateNotChanged() {
    Harness h = new Harness();

    DocumentWithMetadata e1 = createCdc("1", "v1", 1000L, 1L, "users");
    DocumentWithMetadata dlqRetryE1 =
        createCdc("1", "v1_retry", 1000L, 1L, "users").withDlqReconsumed(true);

    h.process(e1);
    h.process(dlqRetryE1);

    assertEquals(2, h.outputs.size());
  }

  @Test
  public void testLegacyElementWithoutTimestamp_passesThrough() {
    Harness h = new Harness();

    Document doc = new Document("_id", "100").append("item", "legacy");
    DocumentWithMetadata legacy = DocumentWithMetadata.of(doc, "items", "items");

    h.process(legacy);

    assertEquals(1, h.outputs.size());
  }

  @Test
  public void testPipelineTransform_distinctKeys() {
    DocumentWithMetadata u1 = createCdc("1", "User1", 1000L, 1L, "users");
    DocumentWithMetadata u2 = createCdc("2", "User2", 1000L, 1L, "users");
    DocumentWithMetadata o1 = createCdc("1", "Order1", 1000L, 1L, "orders");

    PCollection<DocumentWithMetadata> output =
        pipeline
            .apply(Create.of(u1, u2, o1))
            .apply(StatefulDeduplication.of());

    PAssert.that(output)
        .satisfies(
            elements -> {
              List<DocumentWithMetadata> list = new ArrayList<>();
              elements.forEach(list::add);
              assertEquals(3, list.size());
              return null;
            });

    pipeline.run();
  }
}
