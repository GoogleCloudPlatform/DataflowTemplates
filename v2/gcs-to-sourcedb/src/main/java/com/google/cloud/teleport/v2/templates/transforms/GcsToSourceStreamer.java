/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.spanner.SpannerException;
import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import com.google.cloud.teleport.v2.templates.dao.SpannerDao;
import com.google.cloud.teleport.v2.templates.processing.handler.GCSToSourceStreamingHandler;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
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
import org.apache.beam.sdk.transforms.DoFn.OnTimer;
import org.apache.beam.sdk.transforms.DoFn.OnTimerContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles the per-shard processing from GCS to source DB. */
public class GcsToSourceStreamer extends DoFn<KV<String, ProcessingContext>, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(GcsToSourceStreamer.class);
  private int incrementIntervalInMilliSeconds = 1;
  private transient SpannerDao spannerDao;
  private String tableSuffix;
  private final SpannerConfig spannerConfig;
  private boolean isMetadataDbPostgres;

  private static final Counter num_shards =
      Metrics.counter(GcsToSourceStreamer.class, "num_shards");

  public GcsToSourceStreamer(
      int incrementIntervalInMilliSeconds,
      SpannerConfig spannerConfig,
      String tableSuffix,
      boolean isMetadataDbPostgres) {
    this.incrementIntervalInMilliSeconds = incrementIntervalInMilliSeconds;
    this.spannerConfig = spannerConfig;
    this.tableSuffix = tableSuffix;
    this.isMetadataDbPostgres = isMetadataDbPostgres;
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    boolean retry = true;
    while (retry) {
      try {
        spannerDao = new SpannerDao(spannerConfig, tableSuffix, isMetadataDbPostgres);
        retry = false;
      } catch (SpannerException e) {
        LOG.info("Exception in setup of AssignShardIdFn {}", e.getMessage());
        if (e.getMessage().contains("RESOURCE_EXHAUSTED")) {
          try {
            Thread.sleep(10000);
          } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
          }
        }
      } catch (Exception e) {
        throw e;
      }
    }
  }

  /** Teardown function disconnects from the Cloud Spanner. */
  @Teardown
  public void teardown() {
    spannerDao.close();
  }

  @SuppressWarnings("unused")
  @TimerId("timer")
  private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  @StateId("processingContext")
  private final StateSpec<ValueState<ProcessingContext>> processingContext =
      StateSpecs.value(SerializableCoder.of(ProcessingContext.class));

  @StateId("startString")
  private final StateSpec<ValueState<String>> startString = StateSpecs.value(StringUtf8Coder.of());

  @StateId("keyString")
  private final StateSpec<ValueState<String>> keyString = StateSpecs.value(StringUtf8Coder.of());

  @StateId("stopProcessing")
  private final StateSpec<ValueState<Boolean>> stopProcessing = StateSpecs.value(BooleanCoder.of());

  @ProcessElement
  public void processElement(
      ProcessContext c,
      @StateId("processingContext") ValueState<ProcessingContext> processingContext,
      @TimerId("timer") Timer timer,
      @StateId("keyString") ValueState<String> keyString,
      @StateId("startString") ValueState<String> startString,
      @StateId("stopProcessing") ValueState<Boolean> stopProcessing) {
    KV<String, ProcessingContext> element = c.element();
    Boolean failedShard = stopProcessing.read();
    if (failedShard != null && failedShard) {
      return;
    }
    ProcessingContext taskContext = processingContext.read();
    if (taskContext == null) {
      processingContext.write(element.getValue());
    }

    String shardId = keyString.read();

    String storedStartTime = startString.read();
    if (storedStartTime == null) {
      startString.write(element.getValue().getStartTimestamp());
    }

    // Set timer if not already running.
    if (shardId == null) {
      keyString.write(element.getKey());
      Instant outputTimestamp =
          Instant.now().plus(Duration.millis(incrementIntervalInMilliSeconds));
      timer.set(outputTimestamp);
    }
    num_shards.inc();
  }

  @OnTimer("timer")
  public void onExpiry(
      OnTimerContext context,
      @StateId("processingContext") ValueState<ProcessingContext> processingContext,
      @TimerId("timer") Timer timer,
      @StateId("keyString") ValueState<String> keyString,
      @StateId("startString") ValueState<String> startString,
      @StateId("stopProcessing") ValueState<Boolean> stopProcessing) {
    String shardId = keyString.read();
    LOG.info(
        "Shard " + shardId + ": started timer processing for expiry time: " + context.timestamp());
    ProcessingContext taskContext = processingContext.read();
    Boolean failedShard = stopProcessing.read();
    if (failedShard != null && failedShard) {
      return;
    }
    if (taskContext != null) {

      try {
        taskContext.setStartTimestamp(startString.read());

        String processedStartTs = GCSToSourceStreamingHandler.process(taskContext, spannerDao);
        Instant nextTimer = Instant.now().plus(Duration.millis(incrementIntervalInMilliSeconds));
        com.google.cloud.Timestamp startTs =
            com.google.cloud.Timestamp.parseTimestamp(processedStartTs);
        Instant startInst = new Instant(startTs.toSqlTimestamp());
        Instant endInst = startInst.plus(taskContext.getWindowDuration());
        startString.write(endInst.toString());
        timer.set(nextTimer);

      } catch (Exception e) {
        LOG.error(
            "The exception while processing shardId: {} is {} ",
            shardId,
            ExceptionUtils.getStackTrace(e));
        stopProcessing.write(true);
      }
    }
  }
}
