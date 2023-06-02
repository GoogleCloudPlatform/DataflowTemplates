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

import com.google.cloud.teleport.v2.templates.common.TrimmedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.sinks.DataSink;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.BagState;
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

/**
 * This class buffers all records in a BagState and triggers a timer. On expiry, it sorts the
 * records ready to be written and writes them to Sink.
 */
public class OrderRecordsAndWriteToSinkFn extends DoFn<KV<String, TrimmedDataChangeRecord>, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(OrderRecordsAndWriteToSinkFn.class);

  private long incrementIntervalInSeconds = 10;

  private DataSink dataSink;

  private final Counter processedEvents =
      Metrics.counter(OrderRecordsAndWriteToSinkFn.class, "Processed events");

  public OrderRecordsAndWriteToSinkFn(long incrementIntervalInSeconds, DataSink dataSink) {
    this.incrementIntervalInSeconds = incrementIntervalInSeconds;
    this.dataSink = dataSink;
  }

  @SuppressWarnings("unused")
  @TimerId("timer")
  private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

  @StateId("buffer")
  private final StateSpec<BagState<TrimmedDataChangeRecord>> buffer =
      StateSpecs.bag(SerializableCoder.of(TrimmedDataChangeRecord.class));

  @StateId("keyString")
  private final StateSpec<ValueState<String>> keyString = StateSpecs.value(StringUtf8Coder.of());

  @StateId("stopProcessing")
  private final StateSpec<ValueState<Boolean>> stopProcessing = StateSpecs.value(BooleanCoder.of());

  @ProcessElement
  public void processElement(
      ProcessContext c,
      @StateId("buffer") BagState<TrimmedDataChangeRecord> buffer,
      @TimerId("timer") Timer timer,
      @StateId("keyString") ValueState<String> keyString,
      @StateId("stopProcessing") ValueState<Boolean> stopProcessing) {
    KV<String, TrimmedDataChangeRecord> element = c.element();
    Boolean failedShard = stopProcessing.read();
    if (failedShard != null && failedShard) {
      return;
    }
    buffer.add(element.getValue());
    String shardId = keyString.read();
    // Set timer if not already running.
    if (shardId == null) {
      Instant commitTimestamp =
          new Instant(element.getValue().getCommitTimestamp().toSqlTimestamp());
      Instant outputTimestamp =
          commitTimestamp.plus(Duration.standardSeconds(incrementIntervalInSeconds));
      timer.set(outputTimestamp);
      keyString.write(element.getKey());
    }
  }

  @OnTimer("timer")
  public void onExpiry(
      OnTimerContext context,
      @StateId("buffer") BagState<TrimmedDataChangeRecord> buffer,
      @TimerId("timer") Timer timer,
      @StateId("keyString") ValueState<String> keyString,
      @StateId("stopProcessing") ValueState<Boolean> stopProcessing) {
    String shardId = keyString.read();
    LOG.info(
        "Shard " + shardId + ": started timer processing for expiry time: " + context.timestamp());
    if (!buffer.isEmpty().read()) {
      final List<TrimmedDataChangeRecord> records =
          StreamSupport.stream(buffer.read().spliterator(), false).collect(Collectors.toList());
      buffer.clear();

      long writeBack = 0;
      int totalCount = records.size();
      List<TrimmedDataChangeRecord> recordsToOutput = new ArrayList<TrimmedDataChangeRecord>();
      for (TrimmedDataChangeRecord record : records) {
        Instant recordCommitTimestamp = new Instant(record.getCommitTimestamp().toSqlTimestamp());
        // When the watermark passes time T, this means that all records with
        // event time < T have been processed and successfully committed. Since the
        // timer fires when the watermark passes the expiration time, we should
        // only output records with event time < expiration time.
        if (recordCommitTimestamp.isBefore(context.timestamp())) {
          recordsToOutput.add(record);
        } else {
          writeBack++;
          buffer.add(record);
        }
      }
      LOG.info(
          "Shard "
              + shardId
              + ": Total "
              + totalCount
              + " records found. Wrote back "
              + writeBack
              + " recs. Outputting "
              + recordsToOutput.size()
              + " recs for expiry time: "
              + context.timestamp());

      if (!recordsToOutput.isEmpty()) {
        // Order the records in place, and output them.
        Instant sortStartTime = Instant.now();
        Collections.sort(
            recordsToOutput,
            Comparator.comparing(TrimmedDataChangeRecord::getCommitTimestamp)
                .thenComparing(TrimmedDataChangeRecord::getServerTransactionId)
                .thenComparing(TrimmedDataChangeRecord::getRecordSequence));
        Instant sortEndTime = Instant.now();
        LOG.info(
            "Shard "
                + shardId
                + ": Took "
                + (new Duration(sortStartTime, sortEndTime)).toString()
                + " to sort "
                + recordsToOutput.size()
                + " records for expiry time: "
                + context.timestamp());
        // TODO: Create common producer/consumer classes for buffer and use those for both reverse
        // replication templates.
        try {
          // TODO: Consider moving createClient() to setUp() if there are any benefits in doing so.
          dataSink.createClient();
          Instant writeStartTime = Instant.now();
          dataSink.write(shardId, recordsToOutput);
          Instant writeEndTime = Instant.now();
          LOG.info(
              "Shard "
                  + shardId
                  + ": Took "
                  + (new Duration(writeStartTime, writeEndTime)).toString()
                  + " to write "
                  + recordsToOutput.size()
                  + " records to sink for expiry time: "
                  + context.timestamp());
        } catch (Exception e) {
          LOG.error(
              "Shard "
                  + shardId
                  + ": Error occured while publishing to sink, stopping further processing of"
                  + " records for this shard: "
                  + e.toString());
          stopProcessing.write(true);
          return;
        }

      } else {
        LOG.info(
            "Shard " + shardId + ": Expired at {} with no records", context.timestamp().toString());
      }
    }

    Instant nextTimer =
        context.timestamp().plus(Duration.standardSeconds(incrementIntervalInSeconds));
    // 0 is the default, which means set next timer as current timestamp.
    if (incrementIntervalInSeconds == 0) {
      nextTimer = Instant.now();
    }

    if (buffer.isEmpty() != null && !buffer.isEmpty().read()) {
      timer.set(nextTimer);
    } else {
      keyString.clear();
    }
  }
}
