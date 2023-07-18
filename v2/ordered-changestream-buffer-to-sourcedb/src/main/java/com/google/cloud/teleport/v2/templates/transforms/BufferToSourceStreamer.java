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

import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import com.google.cloud.teleport.v2.templates.processing.handler.StreamingHandlerFactory;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
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
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles the per-shard processing from buffer to source. */
public class BufferToSourceStreamer extends DoFn<KV<String, ProcessingContext>, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(BufferToSourceStreamer.class);
  private int incrementIntervalInSeconds = 1;

  public BufferToSourceStreamer(int incrementIntervalInSeconds) {
    this.incrementIntervalInSeconds = incrementIntervalInSeconds;
  }

  @SuppressWarnings("unused")
  @TimerId("timer")
  private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  @StateId("processingContext")
  private final StateSpec<ValueState<ProcessingContext>> processingContext =
      StateSpecs.value(SerializableCoder.of(ProcessingContext.class));

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
    // Set timer if not already running.
    if (shardId == null) {

      Instant outputTimestamp =
          Instant.now().plus(Duration.standardSeconds(incrementIntervalInSeconds));
      timer.set(outputTimestamp);
      keyString.write(element.getKey());
    }
  }

  @OnTimer("timer")
  public void onExpiry(
      OnTimerContext context,
      @StateId("processingContext") ValueState<ProcessingContext> processingContext,
      @TimerId("timer") Timer timer,
      @StateId("keyString") ValueState<String> keyString,
      @StateId("stopProcessing") ValueState<Boolean> stopProcessing) {
    String shardId = keyString.read();
    LOG.info(
        "Shard " + shardId + ": started timer processing for expiry time: " + context.timestamp());
    ProcessingContext taskContext = processingContext.read();
    if (taskContext != null) {

      try {

        StreamingHandlerFactory.getStreamingHandler(taskContext).process();
        Instant nextTimer =
            Instant.now().plus(Duration.standardSeconds(incrementIntervalInSeconds));
        timer.set(nextTimer);

      } catch (Exception e) {
        LOG.error("Shard " + shardId + ": error while processing: " + e);
        stopProcessing.write(true);
      }
    }
  }
}
