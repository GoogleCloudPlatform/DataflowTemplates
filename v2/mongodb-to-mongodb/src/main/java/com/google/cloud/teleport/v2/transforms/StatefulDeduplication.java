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

import com.google.cloud.teleport.v2.transforms.DocumentWithMetadata.OperationType;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateful deduplication PTransform that enforces monotonic ordering per document key
 * across concurrent backfill and change streams.
 *
 * <p>Uses Beam {@link ValueState} to track the highest {@link TimestampSortKey} observed
 * for each document (keyed by {@code collection#{_id}}).
 *
 * <p>3-Tier Comparison Rules:
 * <ol>
 *   <li>Epoch seconds: Higher timestamp wins.
 *   <li>Stream precedence: At identical epoch second $T_0$, live CDC wins over Backfill.
 *   <li>Sub-second increment: Higher sub-second counter wins.
 * </ol>
 */
public class StatefulDeduplication
    extends PTransform<PCollection<DocumentWithMetadata>, PCollection<DocumentWithMetadata>> {

  private StatefulDeduplication() {}

  public static StatefulDeduplication of() {
    return new StatefulDeduplication();
  }

  @Override
  public PCollection<DocumentWithMetadata> expand(PCollection<DocumentWithMetadata> input) {
    return input
        .apply("EnsureGlobalWindow", Window.into(new GlobalWindows()))
        .apply(
            "KeyByDedupKey",
            ParDo.of(
                new DoFn<DocumentWithMetadata, KV<String, DocumentWithMetadata>>() {
                  @ProcessElement
                  public void processElement(
                      @Element DocumentWithMetadata element,
                      OutputReceiver<KV<String, DocumentWithMetadata>> receiver) {
                    if (element != null) {
                      String key = element.getDedupKey();
                      receiver.output(KV.of(key, element));
                    }
                  }
                }))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), DocumentWithMetadataCoder.of()))
        .apply("DeduplicateStateful", ParDo.of(new StatefulDeduplicationFn()));
  }

  /**
   * Stateful DoFn maintaining the latest observed timestamp per document key.
   */
  public static class StatefulDeduplicationFn
      extends DoFn<KV<String, DocumentWithMetadata>, DocumentWithMetadata> {

    private static final Logger LOG = LoggerFactory.getLogger(StatefulDeduplicationFn.class);
    private static final String STATE_ID_LAST_SEEN_KEY = "lastSeenKey";

    private final Counter dedupAccepted =
        Metrics.counter(StatefulDeduplicationFn.class, "dedup_accepted");
    private final Counter dedupDropped =
        Metrics.counter(StatefulDeduplicationFn.class, "dedup_dropped");
    private final Counter dedupDlqPassed =
        Metrics.counter(StatefulDeduplicationFn.class, "dedup_dlq_passed");
    private final Counter dedupBackfillAccepted =
        Metrics.counter(StatefulDeduplicationFn.class, "dedup_backfill_accepted");
    private final Counter dedupCdcAccepted =
        Metrics.counter(StatefulDeduplicationFn.class, "dedup_cdc_accepted");
    private final Counter dedupBackfillDropped =
        Metrics.counter(StatefulDeduplicationFn.class, "dedup_backfill_dropped");
    private final Counter dedupCdcDropped =
        Metrics.counter(StatefulDeduplicationFn.class, "dedup_cdc_dropped");
    private final Counter dedupPassthrough =
        Metrics.counter(StatefulDeduplicationFn.class, "dedup_passthrough");

    @StateId(STATE_ID_LAST_SEEN_KEY)
    private final StateSpec<ValueState<TimestampSortKey>> lastSeenKeySpec =
        StateSpecs.value(TimestampSortKeyCoder.of());

    private void recordAccepted(DocumentWithMetadata element) {
      dedupAccepted.inc();
      if (element.getOperationType() == OperationType.BACKFILL) {
        dedupBackfillAccepted.inc();
      } else {
        dedupCdcAccepted.inc();
      }
    }

    private void recordDropped(DocumentWithMetadata element) {
      dedupDropped.inc();
      if (element.getOperationType() == OperationType.BACKFILL) {
        dedupBackfillDropped.inc();
      } else {
        dedupCdcDropped.inc();
      }
    }

    @ProcessElement
    public void processElement(
        @Element KV<String, DocumentWithMetadata> kv,
        OutputReceiver<DocumentWithMetadata> receiver,
        @StateId(STATE_ID_LAST_SEEN_KEY) ValueState<TimestampSortKey> lastSeenState) {

      DocumentWithMetadata element = kv.getValue();
      if (element == null) {
        return;
      }

      TimestampSortKey incomingKey = element.getTimestampSortKey();
      if (incomingKey == null) {
        // Elements without timestamps (e.g. legacy/batch) pass through unconditionally
        dedupPassthrough.inc();
        recordAccepted(element);
        receiver.output(element);
        return;
      }

      TimestampSortKey lastSeenKey = lastSeenState.read();

      if (lastSeenKey == null) {
        // First time seeing this document key
        lastSeenState.write(incomingKey);
        recordAccepted(element);
        if (element.isDlqReconsumed()) {
          dedupDlqPassed.inc();
        }
        receiver.output(element);
        return;
      }

      int cmp = incomingKey.compareTo(lastSeenKey);

      if (cmp >= 0) {
        // Incoming event is newer than or equal to last seen state
        lastSeenState.write(incomingKey);
        recordAccepted(element);
        if (element.isDlqReconsumed()) {
          dedupDlqPassed.inc();
        }
        receiver.output(element);
      } else {
        // Stale event arrived out-of-order or stale DLQ retry
        recordDropped(element);
        if (element.isDlqReconsumed()) {
          LOG.debug(
              "Dropping stale DLQ retry for key '{}'. Incoming: {}, Last seen: {}",
              kv.getKey(),
              incomingKey,
              lastSeenKey);
        } else {
          LOG.debug(
              "Dropping out-of-order event for key '{}'. Incoming: {}, Last seen: {}",
              kv.getKey(),
              incomingKey,
              lastSeenKey);
        }
      }
    }
  }
}
