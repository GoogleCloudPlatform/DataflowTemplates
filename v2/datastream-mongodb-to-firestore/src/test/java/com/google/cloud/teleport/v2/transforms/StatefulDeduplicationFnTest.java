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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link StatefulDeduplicationFn}. */
@RunWith(JUnit4.class)
public class StatefulDeduplicationFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private MongoDbChangeEventContext createEventContext(
      String docId, long seconds, int nanos, String changeType, boolean isDlqReconsumed)
      throws Exception {
    return createEventContext(docId, seconds, nanos, changeType, isDlqReconsumed, "cdc");
  }

  private MongoDbChangeEventContext createEventContext(
      String docId,
      long seconds,
      int nanos,
      String changeType,
      boolean isDlqReconsumed,
      String readMethod)
      throws Exception {
    String payload =
        String.format(
            "{"
                + "\"_metadata_source\": {\"collection\": \"users\"},"
                + "\"_id\": \"\\\"%s\\\"\","
                + "\"_metadata_timestamp_seconds\": %d,"
                + "\"_metadata_timestamp_nanos\": %d,"
                + "\"_metadata_change_type\": \"%s\","
                + "\"_metadata_read_method\": \"%s\""
                + (isDlqReconsumed ? ",\"isDlqReconsumed\": \"true\"" : "")
                + ",\"data\": \"{\\\"name\\\": \\\"user_%s\\\"}\""
                + "}",
            docId,
            seconds,
            nanos,
            changeType,
            readMethod,
            docId);
    return new MongoDbChangeEventContext(OBJECT_MAPPER.readTree(payload), "shadow_");
  }

  @Test
  public void testInOrderEvents_emitsAll() throws Exception {
    MongoDbChangeEventContext event1 = createEventContext("doc1", 1000L, 100, "INSERT", false);
    MongoDbChangeEventContext event2 = createEventContext("doc1", 1000L, 200, "UPDATE", false);

    TestStream<KV<String, MongoDbChangeEventContext>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), MongoDbChangeEventContextCoder.of()))
            .addElements(
                TimestampedValue.of(KV.of("users#doc1", event1), new Instant(100)),
                TimestampedValue.of(KV.of("users#doc1", event2), new Instant(200)))
            .advanceWatermarkToInfinity();

    PCollection<MongoDbChangeEventContext> result =
        pipeline
            .apply(stream)
            .apply(Window.into(new GlobalWindows()))
            .apply(ParDo.of(new StatefulDeduplicationFn()));

    PAssert.that(result).containsInAnyOrder(event1, event2);
    pipeline.run();
  }

  @Test
  public void testOutOfOrderEvents_dropsStaleEvent() throws Exception {
    MongoDbChangeEventContext eventNewer = createEventContext("doc1", 1000L, 200, "UPDATE", false);
    MongoDbChangeEventContext eventStale = createEventContext("doc1", 1000L, 100, "INSERT", false);

    TestStream<KV<String, MongoDbChangeEventContext>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), MongoDbChangeEventContextCoder.of()))
            .addElements(TimestampedValue.of(KV.of("users#doc1", eventNewer), new Instant(100)))
            .addElements(TimestampedValue.of(KV.of("users#doc1", eventStale), new Instant(200)))
            .advanceWatermarkToInfinity();

    PCollection<MongoDbChangeEventContext> result =
        pipeline
            .apply(stream)
            .apply(Window.into(new GlobalWindows()))
            .apply(ParDo.of(new StatefulDeduplicationFn()));

    PAssert.that(result).containsInAnyOrder(eventNewer);
    PipelineResult pipelineResult = pipeline.run();

    MetricQueryResults metrics =
        pipelineResult
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(StatefulDeduplicationFn.class, "outOfOrderSkips"))
                    .build());

    long count = 0;
    if (metrics.getCounters().iterator().hasNext()) {
      count = metrics.getCounters().iterator().next().getAttempted();
    }
    assertEquals(1L, count);
  }

  @Test
  public void testDeleteAndStaleUpdate_preventsZombieResurrection() throws Exception {
    MongoDbChangeEventContext insertEvent = createEventContext("doc1", 1000L, 100, "INSERT", false);
    MongoDbChangeEventContext deleteEvent = createEventContext("doc1", 1000L, 300, "DELETE", false);
    MongoDbChangeEventContext staleUpdate = createEventContext("doc1", 1000L, 200, "UPDATE", false);

    TestStream<KV<String, MongoDbChangeEventContext>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), MongoDbChangeEventContextCoder.of()))
            .addElements(TimestampedValue.of(KV.of("users#doc1", insertEvent), new Instant(100)))
            .addElements(TimestampedValue.of(KV.of("users#doc1", deleteEvent), new Instant(200)))
            .addElements(TimestampedValue.of(KV.of("users#doc1", staleUpdate), new Instant(300)))
            .advanceWatermarkToInfinity();

    PCollection<MongoDbChangeEventContext> result =
        pipeline
            .apply(stream)
            .apply(Window.into(new GlobalWindows()))
            .apply(ParDo.of(new StatefulDeduplicationFn()));

    PAssert.that(result).containsInAnyOrder(insertEvent, deleteEvent);
    PipelineResult pipelineResult = pipeline.run();

    MetricQueryResults metrics =
        pipelineResult
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(StatefulDeduplicationFn.class, "outOfOrderSkips"))
                    .build());

    long skips = 0;
    if (metrics.getCounters().iterator().hasNext()) {
      skips = metrics.getCounters().iterator().next().getAttempted();
    }
    assertEquals(1L, skips);
  }

  @Test
  public void testDlqEqualTimestamp_passThrough() throws Exception {
    MongoDbChangeEventContext regularEvent =
        createEventContext("doc1", 1000L, 100, "INSERT", false);
    MongoDbChangeEventContext dlqReconsumedEvent =
        createEventContext("doc1", 1000L, 100, "INSERT", true);

    TestStream<KV<String, MongoDbChangeEventContext>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), MongoDbChangeEventContextCoder.of()))
            .addElements(TimestampedValue.of(KV.of("users#doc1", regularEvent), new Instant(100)))
            .addElements(
                TimestampedValue.of(KV.of("users#doc1", dlqReconsumedEvent), new Instant(200)))
            .advanceWatermarkToInfinity();

    PCollection<MongoDbChangeEventContext> result =
        pipeline
            .apply(stream)
            .apply(Window.into(new GlobalWindows()))
            .apply(ParDo.of(new StatefulDeduplicationFn()));

    PAssert.that(result).containsInAnyOrder(regularEvent, dlqReconsumedEvent);
    PipelineResult pipelineResult = pipeline.run();

    MetricQueryResults metrics =
        pipelineResult
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(
                            StatefulDeduplicationFn.class, "dlqEqualTimestampPassThrough"))
                    .build());

    long passThroughCount = 0;
    if (metrics.getCounters().iterator().hasNext()) {
      passThroughCount = metrics.getCounters().iterator().next().getAttempted();
    }
    assertEquals(1L, passThroughCount);
  }

  @Test
  public void testDuplicateEqualTimestamp_nonDlq_dropped() throws Exception {
    MongoDbChangeEventContext event1 = createEventContext("doc1", 1000L, 100, "INSERT", false);
    MongoDbChangeEventContext duplicateEvent =
        createEventContext("doc1", 1000L, 100, "INSERT", false);

    TestStream<KV<String, MongoDbChangeEventContext>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), MongoDbChangeEventContextCoder.of()))
            .addElements(TimestampedValue.of(KV.of("users#doc1", event1), new Instant(100)))
            .addElements(TimestampedValue.of(KV.of("users#doc1", duplicateEvent), new Instant(200)))
            .advanceWatermarkToInfinity();

    PCollection<MongoDbChangeEventContext> result =
        pipeline
            .apply(stream)
            .apply(Window.into(new GlobalWindows()))
            .apply(ParDo.of(new StatefulDeduplicationFn()));

    PAssert.that(result).containsInAnyOrder(event1);
    pipeline.run();
  }

  @Test
  public void testCompositeMapAndArrayDocumentId() throws Exception {
    String mapPayload =
        "{"
            + "\"_metadata_source\": {\"collection\": \"accounts\"},"
            + "\"_id\": \"{\\\"tenant\\\": \\\"t1\\\", \\\"account\\\": 12345}\","
            + "\"_metadata_timestamp_seconds\": 1000,"
            + "\"_metadata_timestamp_nanos\": 100,"
            + "\"_metadata_change_type\": \"INSERT\","
            + "\"data\": \"{\\\"balance\\\": 500}\""
            + "}";
    MongoDbChangeEventContext mapEvent =
        new MongoDbChangeEventContext(OBJECT_MAPPER.readTree(mapPayload), "shadow_");

    String arrayPayload =
        "{"
            + "\"_metadata_source\": {\"collection\": \"items\"},"
            + "\"_id\": \"[1, \\\"partA\\\", 2]\","
            + "\"_metadata_timestamp_seconds\": 1000,"
            + "\"_metadata_timestamp_nanos\": 200,"
            + "\"_metadata_change_type\": \"UPDATE\","
            + "\"data\": \"{\\\"qty\\\": 10}\""
            + "}";
    MongoDbChangeEventContext arrayEvent =
        new MongoDbChangeEventContext(OBJECT_MAPPER.readTree(arrayPayload), "shadow_");

    String mapKey = "accounts#" + Utils.documentIdToString(mapEvent.getDocumentId());
    String arrayKey = "items#" + Utils.documentIdToString(arrayEvent.getDocumentId());

    TestStream<KV<String, MongoDbChangeEventContext>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), MongoDbChangeEventContextCoder.of()))
            .addElements(TimestampedValue.of(KV.of(mapKey, mapEvent), new Instant(100)))
            .addElements(TimestampedValue.of(KV.of(arrayKey, arrayEvent), new Instant(200)))
            .advanceWatermarkToInfinity();

    PCollection<MongoDbChangeEventContext> result =
        pipeline
            .apply(stream)
            .apply(Window.into(new GlobalWindows()))
            .apply(ParDo.of(new StatefulDeduplicationFn()));

    PAssert.that(result).containsInAnyOrder(mapEvent, arrayEvent);
    pipeline.run();
  }

  @Test
  public void testBackfillSnapshotDoesNotResurrectCdcDelete() throws Exception {
    // Exact 30-orphan scenario: CDC DELETE at ts=1786382543:4752 followed by Backfill READ at
    // ts=1786382543:967669000
    MongoDbChangeEventContext cdcDelete =
        createEventContext("orphanDoc", 1786382543L, 4752, "DELETE", false, "cdc");
    MongoDbChangeEventContext backfillRead =
        createEventContext("orphanDoc", 1786382543L, 967669000, "READ", false, "backfill");

    TestStream<KV<String, MongoDbChangeEventContext>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), MongoDbChangeEventContextCoder.of()))
            .addElements(TimestampedValue.of(KV.of("users#orphanDoc", cdcDelete), new Instant(100)))
            .addElements(
                TimestampedValue.of(KV.of("users#orphanDoc", backfillRead), new Instant(200)))
            .advanceWatermarkToInfinity();

    PCollection<MongoDbChangeEventContext> result =
        pipeline
            .apply(stream)
            .apply(Window.into(new GlobalWindows()))
            .apply(ParDo.of(new StatefulDeduplicationFn()));

    // Only the CDC delete should be emitted; the backfill read snapshot must be dropped as stale
    PAssert.that(result).containsInAnyOrder(cdcDelete);
    PipelineResult pipelineResult = pipeline.run();

    MetricQueryResults metrics =
        pipelineResult
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(StatefulDeduplicationFn.class, "outOfOrderSkips"))
                    .build());

    long skips = 0;
    if (metrics.getCounters().iterator().hasNext()) {
      skips = metrics.getCounters().iterator().next().getAttempted();
    }
    assertEquals(1L, skips);
  }

  @Test
  public void testCdcUpdateSupersedesConcurrentBackfillRead() throws Exception {
    MongoDbChangeEventContext backfillRead =
        createEventContext("doc1", 1786382543L, 967669000, "READ", false, "backfill");
    MongoDbChangeEventContext cdcUpdate =
        createEventContext("doc1", 1786382543L, 100, "UPDATE", false, "cdc");

    TestStream<KV<String, MongoDbChangeEventContext>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), MongoDbChangeEventContextCoder.of()))
            .addElements(TimestampedValue.of(KV.of("users#doc1", backfillRead), new Instant(100)))
            .addElements(TimestampedValue.of(KV.of("users#doc1", cdcUpdate), new Instant(200)))
            .advanceWatermarkToInfinity();

    PCollection<MongoDbChangeEventContext> result =
        pipeline
            .apply(stream)
            .apply(Window.into(new GlobalWindows()))
            .apply(ParDo.of(new StatefulDeduplicationFn()));

    PAssert.that(result).containsInAnyOrder(backfillRead, cdcUpdate);
    pipeline.run();
  }

  @Test
  public void testDeleteAndRecreate_allowsNewerInsert() throws Exception {
    MongoDbChangeEventContext insertV1 = createEventContext("doc1", 1000L, 100, "INSERT", false);
    MongoDbChangeEventContext deleteV2 = createEventContext("doc1", 1000L, 200, "DELETE", false);
    MongoDbChangeEventContext insertV3 = createEventContext("doc1", 1001L, 50, "INSERT", false);

    TestStream<KV<String, MongoDbChangeEventContext>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), MongoDbChangeEventContextCoder.of()))
            .addElements(TimestampedValue.of(KV.of("users#doc1", insertV1), new Instant(100)))
            .addElements(TimestampedValue.of(KV.of("users#doc1", deleteV2), new Instant(200)))
            .addElements(TimestampedValue.of(KV.of("users#doc1", insertV3), new Instant(300)))
            .advanceWatermarkToInfinity();

    PCollection<MongoDbChangeEventContext> result =
        pipeline
            .apply(stream)
            .apply(Window.into(new GlobalWindows()))
            .apply(ParDo.of(new StatefulDeduplicationFn()));

    PAssert.that(result).containsInAnyOrder(insertV1, deleteV2, insertV3);
    pipeline.run();
  }

  @Test
  public void testAdvancingProcessingTimestamps_monotonicDeduplicationGovernedByTimestampSortKey()
      throws Exception {
    MongoDbChangeEventContext cdcUpdate1 =
        createEventContext("doc1", 2000L, 100, "UPDATE", false, "cdc");
    MongoDbChangeEventContext staleBackfill =
        createEventContext("doc1", 1500L, 50, "READ", false, "backfill");
    MongoDbChangeEventContext cdcUpdate2 =
        createEventContext("doc1", 2500L, 200, "UPDATE", false, "cdc");
    MongoDbChangeEventContext staleCdc =
        createEventContext("doc1", 1800L, 0, "UPDATE", false, "cdc");

    TestStream<KV<String, MongoDbChangeEventContext>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), MongoDbChangeEventContextCoder.of()))
            .addElements(TimestampedValue.of(KV.of("users#doc1", cdcUpdate1), new Instant(1000)))
            .addElements(TimestampedValue.of(KV.of("users#doc1", staleBackfill), new Instant(2000)))
            .addElements(TimestampedValue.of(KV.of("users#doc1", cdcUpdate2), new Instant(3000)))
            .addElements(TimestampedValue.of(KV.of("users#doc1", staleCdc), new Instant(4000)))
            .advanceWatermarkToInfinity();

    PCollection<MongoDbChangeEventContext> result =
        pipeline
            .apply(stream)
            .apply(Window.into(new GlobalWindows()))
            .apply(ParDo.of(new StatefulDeduplicationFn()));

    PAssert.that(result).containsInAnyOrder(cdcUpdate1, cdcUpdate2);
    PipelineResult pipelineResult = pipeline.run();

    MetricQueryResults metrics =
        pipelineResult
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(StatefulDeduplicationFn.class, "outOfOrderSkips"))
                    .build());

    long skips = 0;
    if (metrics.getCounters().iterator().hasNext()) {
      skips = metrics.getCounters().iterator().next().getAttempted();
    }
    assertEquals(2L, skips);
  }

  @Test
  public void testAdvancingProcessingTimestamps_cdcDeleteOverridesOlderBackfillRead()
      throws Exception {
    MongoDbChangeEventContext cdcDelete =
        createEventContext("doc1", 1000L, 200, "DELETE", false, "cdc");
    MongoDbChangeEventContext olderBackfill =
        createEventContext("doc1", 1000L, 100, "READ", false, "backfill");

    TestStream<KV<String, MongoDbChangeEventContext>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), MongoDbChangeEventContextCoder.of()))
            .addElements(TimestampedValue.of(KV.of("users#doc1", cdcDelete), new Instant(1000)))
            .addElements(TimestampedValue.of(KV.of("users#doc1", olderBackfill), new Instant(2000)))
            .advanceWatermarkToInfinity();

    PCollection<MongoDbChangeEventContext> result =
        pipeline
            .apply(stream)
            .apply(Window.into(new GlobalWindows()))
            .apply(ParDo.of(new StatefulDeduplicationFn()));

    PAssert.that(result).containsInAnyOrder(cdcDelete);
    PipelineResult pipelineResult = pipeline.run();

    MetricQueryResults metrics =
        pipelineResult
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(StatefulDeduplicationFn.class, "outOfOrderSkips"))
                    .build());

    long skips = 0;
    if (metrics.getCounters().iterator().hasNext()) {
      skips = metrics.getCounters().iterator().next().getAttempted();
    }
    assertEquals(1L, skips);
  }
}
