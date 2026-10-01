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
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OnTimer;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.DoFn.TimerId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateful deduplication PTransform that enforces monotonic ordering per document key across
 * concurrent backfill and change streams.
 *
 * <p>Uses Beam {@link ValueState} to track the highest {@link TimestampSortKey} observed for each
 * document (keyed by {@code collection#{_id}}).
 *
 * <p>3-Tier Comparison Rules:
 *
 * <ol>
 *   <li>Epoch seconds: Higher timestamp wins.
 *   <li>Stream precedence: At identical epoch second $T_0$, live CDC wins over Backfill.
 *   <li>Sub-second increment: Higher sub-second counter wins.
 * </ol>
 *
 * <p>By default per-key state is retained for the life of the job. Retention can optionally be
 * bounded via {@link #of(Duration)} to limit state growth on collections with an unbounded number
 * of distinct documents, but note the tradeoff: a duplicate arriving after its key's state has
 * expired is treated as unseen and re-emitted. Because backfill records carry an older sort key
 * than any CDC event for the same document, an expiry that fires while backfill is still running
 * lets a stale backfill row overwrite newer CDC data. Any bound must therefore exceed the longest
 * expected backfill.
 */
public class StatefulDeduplication
    extends PTransform<PCollection<DocumentWithMetadata>, PCollection<DocumentWithMetadata>> {

  /**
   * Per-key state retention, or {@code null} to retain state for the life of the job.
   *
   * <p>Per-key state is small — a {@link TimestampSortKey} keyed by {@code collection#{_id}} — so
   * unbounded retention costs on the order of a hundred bytes per distinct document, while bounding
   * it risks re-applying stale backfill data. Hence unbounded by default.
   */
  @Nullable private final Duration stateRetention;

  private StatefulDeduplication(@Nullable Duration stateRetention) {
    if (stateRetention != null && stateRetention.getMillis() <= 0) {
      throw new IllegalArgumentException(
          "stateRetention must be positive when set, got " + stateRetention);
    }
    this.stateRetention = stateRetention;
  }

  /** Returns an instance retaining per-key deduplication state for the life of the job. */
  public static StatefulDeduplication of() {
    return new StatefulDeduplication(null);
  }

  /**
   * Returns an instance retaining per-key deduplication state for {@code stateRetention} after the
   * most recent accepted event, or for the life of the job when {@code null}.
   *
   * @param stateRetention must be positive when non-null
   */
  public static StatefulDeduplication of(@Nullable Duration stateRetention) {
    return new StatefulDeduplication(stateRetention);
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
        .apply("DeduplicateStateful", ParDo.of(new StatefulDeduplicationFn(stateRetention)));
  }

  /** Stateful DoFn maintaining the latest observed timestamp per document key. */
  public static class StatefulDeduplicationFn
      extends DoFn<KV<String, DocumentWithMetadata>, DocumentWithMetadata> {

    private static final Logger LOG = LoggerFactory.getLogger(StatefulDeduplicationFn.class);
    private static final String STATE_ID_LAST_SEEN_KEY = "lastSeenKey";
    private static final String TIMER_ID_STATE_EXPIRY = "lastSeenKeyExpiry";

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
    private final Counter dedupStateExpired =
        Metrics.counter(StatefulDeduplicationFn.class, "dedup_state_expired");

    @Nullable private final Duration stateRetention;

    public StatefulDeduplicationFn(@Nullable Duration stateRetention) {
      this.stateRetention = stateRetention;
    }

    /**
     * Slides the expiry window forward so state survives for as long as the document is active.
     * No-op when retention is unbounded, which also avoids a timer write per accepted event.
     */
    private void armExpiryTimer(Timer stateExpiryTimer) {
      if (stateRetention != null) {
        stateExpiryTimer.offset(stateRetention).setRelative();
      }
    }

    @StateId(STATE_ID_LAST_SEEN_KEY)
    private final StateSpec<ValueState<TimestampSortKey>> lastSeenKeySpec =
        StateSpecs.value(TimestampSortKeyCoder.of());

    @TimerId(TIMER_ID_STATE_EXPIRY)
    private final TimerSpec stateExpirySpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

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
        @StateId(STATE_ID_LAST_SEEN_KEY) ValueState<TimestampSortKey> lastSeenState,
        @TimerId(TIMER_ID_STATE_EXPIRY) Timer stateExpiryTimer) {

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
        armExpiryTimer(stateExpiryTimer);
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
        armExpiryTimer(stateExpiryTimer);
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

    /**
     * Releases the per-key state once no event has been accepted for the retention window. Without
     * this the transform holds one state cell per distinct document for the lifetime of the job.
     */
    @OnTimer(TIMER_ID_STATE_EXPIRY)
    public void onStateExpiry(
        @StateId(STATE_ID_LAST_SEEN_KEY) ValueState<TimestampSortKey> lastSeenState) {
      lastSeenState.clear();
      dedupStateExpired.inc();
    }
  }
}
