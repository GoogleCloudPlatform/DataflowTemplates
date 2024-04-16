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
package com.google.cloud.teleport.v2.transforms;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link SpannerChangeStreamsToOrderedKV} class is a {@link PTransform} that takes in {@link
 * PCollection} of DataChangeRecords. The transform returns KV of partition bucket index and ordered
 * list of DataChangeRecords (ordered by commit timestamp within partition bucket)
 */
@AutoValue
public abstract class SpannerChangeStreamsToOrderedKV
    extends PTransform<
        PCollection<DataChangeRecord>, PCollection<KV<String, Iterable<DataChangeRecord>>>> {

  /** Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreamsToOrderedKV.class);

  public static OrderingOptionsBuilder newBuilder() {
    return new AutoValue_SpannerChangeStreamsToOrderedKV.Builder();
  }

  protected abstract String orderingPartitionKey();

  protected abstract Integer orderingPartitionBucketCount();

  protected abstract Integer bufferTimerInterval();

  @Override
  public PCollection<KV<String, Iterable<DataChangeRecord>>> expand(
      PCollection<DataChangeRecord> records) {
    return records
        .apply(ParDo.of(new BreakRecordByModFn()))
        .apply(ParDo.of(new KeyByIdFn(orderingPartitionBucketCount())))
        .apply(ParDo.of(new BufferKeyUntilOutputTimestamp(bufferTimerInterval())));
  }

  private static class BreakRecordByModFn extends DoFn<DataChangeRecord, DataChangeRecord> {
    @ProcessElement
    public void processElement(
        @Element DataChangeRecord record, OutputReceiver<DataChangeRecord> outputReceiver) {
      record.getMods().stream()
          .map(
              mod ->
                  new DataChangeRecord(
                      record.getPartitionToken(),
                      record.getCommitTimestamp(),
                      record.getServerTransactionId(),
                      record.isLastRecordInTransactionInPartition(),
                      record.getRecordSequence(),
                      record.getTableName(),
                      record.getRowType(),
                      Collections.singletonList(mod),
                      record.getModType(),
                      record.getValueCaptureType(),
                      record.getNumberOfRecordsInTransaction(),
                      record.getNumberOfPartitionsInTransaction(),
                      record.getTransactionTag(),
                      record.isSystemTransaction(),
                      record.getMetadata()))
          .forEach(outputReceiver::output);
    }
  }

  private static class KeyByIdFn extends DoFn<DataChangeRecord, KV<String, DataChangeRecord>> {
    private int numberOfBuckets = 1000;

    private KeyByIdFn(int numberOfBuckets) {
      this.numberOfBuckets = numberOfBuckets;
    }

    // numberOfBuckets is configured by the user (via orderingPartitionBucketCount parameter)
    // to match their key cardinality
    // Here, we are choosing to hash the partition (Spanner primary keys) to a bucket index, in
    // order to have a deterministic number
    // of states and timers for performance purposes.
    // Note that having too many buckets might have undesirable effects if it
    // results in a low number of records per bucket
    // On the other hand, having too few buckets might also be problematic, since
    // many keys will be contained within them.

    @ProcessElement
    public void processElement(
        @Element DataChangeRecord record,
        OutputReceiver<KV<String, DataChangeRecord>> outputReceiver) {
      // TODO: generate hashcode dynamically based on orderingPartitionKey()
      int hashCode = (int) record.getMods().get(0).getKeysJson().hashCode();
      // Hash the received keys into a bucket in order to have a
      // deterministic number of buffers and timers.
      String bucketIndex = String.valueOf(hashCode % numberOfBuckets);

      outputReceiver.output(KV.of(bucketIndex, record));
    }
  }

  private static class BufferKeyUntilOutputTimestamp
      extends DoFn<KV<String, DataChangeRecord>, KV<String, Iterable<DataChangeRecord>>> {
    private static final Logger LOG = LoggerFactory.getLogger(BufferKeyUntilOutputTimestamp.class);

    private long incrementIntervalInSeconds = 2;

    private BufferKeyUntilOutputTimestamp(long incrementIntervalInSeconds) {
      this.incrementIntervalInSeconds = incrementIntervalInSeconds;
    }

    @SuppressWarnings("unused")
    @TimerId("timer")
    private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @StateId("buffer")
    private final StateSpec<BagState<DataChangeRecord>> buffer = StateSpecs.bag();

    @StateId("keyString")
    private final StateSpec<ValueState<String>> keyString = StateSpecs.value(StringUtf8Coder.of());

    @ProcessElement
    public void process(
        @Element KV<String, DataChangeRecord> element,
        @StateId("buffer") BagState<DataChangeRecord> buffer,
        @TimerId("timer") Timer timer,
        @StateId("keyString") ValueState<String> keyString) {
      buffer.add(element.getValue());

      // Only set the timer if this is the first time we are receiving a data change
      // record with this key.
      String elementKey = keyString.read();
      if (elementKey == null) {
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
        @StateId("buffer") BagState<DataChangeRecord> buffer,
        @TimerId("timer") Timer timer,
        @StateId("keyString") ValueState<String> keyString) {
      if (!buffer.isEmpty().read()) {
        String elementKey = keyString.read();

        final List<DataChangeRecord> records =
            StreamSupport.stream(buffer.read().spliterator(), false).collect(Collectors.toList());
        buffer.clear();

        List<DataChangeRecord> recordsToOutput = new ArrayList<>();
        for (DataChangeRecord record : records) {
          Instant recordCommitTimestamp = new Instant(record.getCommitTimestamp().toSqlTimestamp());
          // When the watermark passes time T, this means that all records with
          // event time < T have been processed and successfully committed. Since the
          // timer fires when the watermark passes the expiration time, we should
          // only output records with event time < expiration time.
          if (recordCommitTimestamp.isBefore(context.timestamp())) {
            LOG.info(
                "Outputting record with key {} at expiration timestamp {}",
                elementKey,
                context.timestamp().toString());
            recordsToOutput.add(record);
          } else {
            LOG.info(
                "Expired at {} but adding record with key {} back to "
                    + "buffer due to commit timestamp {}",
                context.timestamp().toString(),
                elementKey,
                recordCommitTimestamp.toString());
            buffer.add(record);
          }
        }

        // Output records, if there are any to output.
        if (!recordsToOutput.isEmpty()) {
          // Order the records in place, and output them.
          // DataChangeRecordComparator sorts the
          // data change records by commit timestamp.
          Collections.sort(recordsToOutput, new DataChangeRecordComparator());
          context.outputWithTimestamp(KV.of(elementKey, recordsToOutput), context.timestamp());
          LOG.info(
              "Expired at {}, outputting records for key {}",
              context.timestamp().toString(),
              elementKey);
        } else {
          LOG.info("Expired at {} with no records", context.timestamp().toString());
        }
      }

      Instant nextTimer =
          context.timestamp().plus(Duration.standardSeconds(incrementIntervalInSeconds));
      if (buffer.isEmpty() != null && !buffer.isEmpty().read()) {
        LOG.info("Setting next timer to {}", nextTimer.toString());
        timer.set(nextTimer);
      } else {
        LOG.info("Timer not being set since the buffer is empty: ");
        keyString.clear();
      }
    }
  }

  private static class DataChangeRecordComparator implements Comparator<DataChangeRecord> {
    @Override
    public int compare(DataChangeRecord r1, DataChangeRecord r2) {
      return r1.getCommitTimestamp().compareTo(r2.getCommitTimestamp());
    }
  }

  /** Builder for {@link SpannerChangeStreamsToOrderedKV}. */
  @AutoValue.Builder
  public abstract static class OrderingOptionsBuilder {

    public abstract OrderingOptionsBuilder setOrderingPartitionKey(String value);

    public abstract OrderingOptionsBuilder setOrderingPartitionBucketCount(Integer value);

    public abstract OrderingOptionsBuilder setBufferTimerInterval(Integer value);

    abstract SpannerChangeStreamsToOrderedKV autoBuild();

    public SpannerChangeStreamsToOrderedKV build() {
      return autoBuild();
    }
  }
}
