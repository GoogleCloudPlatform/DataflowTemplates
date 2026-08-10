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

import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateful DoFn that ensures monotonic timestamp ordering per document ID and drops out-of-order
 * events in memory using Dataflow Streaming Engine state without database round-trips.
 */
public class StatefulDeduplicationFn
    extends DoFn<KV<String, MongoDbChangeEventContext>, MongoDbChangeEventContext> {

  private static final Logger LOG = LoggerFactory.getLogger(StatefulDeduplicationFn.class);

  @StateId("latestTimestamp")
  private final StateSpec<ValueState<Long>> latestTimestampSpec = StateSpecs.value();

  private final Counter outOfOrderSkips =
      Metrics.counter(StatefulDeduplicationFn.class, "outOfOrderSkips");
  private final Counter dedupOutputs =
      Metrics.counter(StatefulDeduplicationFn.class, "dedupOutputs");
  private final Counter dlqEqualTimestampPassThrough =
      Metrics.counter(StatefulDeduplicationFn.class, "dlqEqualTimestampPassThrough");

  private final ThrottledLogger throttledLogger = new ThrottledLogger(30000L);
  private transient java.util.Map<String, Long> bundleStateCache;

  @StartBundle
  public void startBundle() {
    bundleStateCache = new java.util.HashMap<>();
  }

  @FinishBundle
  public void finishBundle() {
    if (bundleStateCache != null) {
      bundleStateCache.clear();
    }
  }

  @ProcessElement
  public void processElement(
      ProcessContext context,
      @StateId("latestTimestamp") ValueState<Long> latestTimestampState,
      OutputReceiver<MongoDbChangeEventContext> out) {
    KV<String, MongoDbChangeEventContext> element = context.element();
    if (element == null || element.getValue() == null) {
      return;
    }

    MongoDbChangeEventContext event = element.getValue();
    Document timestampDoc = event.getTimestampDoc();
    long currentTimestampNanos = Utils.getTimestampNanos(timestampDoc);

    String key = element.getKey();
    Long latestTimestampNanos =
        (bundleStateCache != null && key != null) ? bundleStateCache.get(key) : null;
    if (latestTimestampNanos == null) {
      latestTimestampNanos = latestTimestampState.read();
    }

    if (latestTimestampNanos == null || currentTimestampNanos > latestTimestampNanos) {
      latestTimestampState.write(currentTimestampNanos);
      if (bundleStateCache != null && key != null) {
        bundleStateCache.put(key, currentTimestampNanos);
      }
      out.output(event);
      dedupOutputs.inc();
    } else if (currentTimestampNanos == latestTimestampNanos && event.getIsDlqReconsumed()) {
      // Reconsumed DLQ events with identical timestamp are allowed to pass through
      out.output(event);
      dedupOutputs.inc();
      dlqEqualTimestampPassThrough.inc();
    } else {
      // Stale / out-of-order event - drop immediately without database RPCs
      if (bundleStateCache != null && key != null && latestTimestampNanos != null) {
        bundleStateCache.put(key, latestTimestampNanos);
      }
      outOfOrderSkips.inc();
      throttledLogger.logInfo(
          LOG,
          event.getDataCollection(),
          "Dropped out-of-order event for docId: {}, currentTimestampNanos: {}, latestTimestampNanos: {}",
          event.getDocumentId(),
          currentTimestampNanos,
          latestTimestampNanos);
    }
  }
}
