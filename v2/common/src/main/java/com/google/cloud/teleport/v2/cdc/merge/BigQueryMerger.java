/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.v2.cdc.merge;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class BigQueryMerger. */
public class BigQueryMerger extends PTransform<PCollection<MergeInfo>, PCollection<Void>> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMerger.class);

  private BigQuery bigQueryClient;
  private MergeConfiguration mergeConfiguration;

  public BigQueryMerger(BigQuery bigQueryClient, MergeConfiguration mergeConfiguration) {
    this.bigQueryClient = bigQueryClient;
    this.mergeConfiguration = mergeConfiguration;
  }

  public static BigQueryMerger of(MergeConfiguration mergeConfiguration) {
    return new BigQueryMerger(null, mergeConfiguration);
  }

  @Override
  public PCollection<Void> expand(PCollection<MergeInfo> input) {
    return input
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.strings(), TypeDescriptor.of(MergeInfo.class)))
                .via(mergeInfo -> KV.of(mergeInfo.getReplicaTableReference(), mergeInfo)))
        .apply(
            new TriggerPerKeyOnFixedIntervals<String, MergeInfo>(
                mergeConfiguration.mergeWindowDuration()))
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.integers(), TypeDescriptor.of(MergeInfo.class)))
                .via(kv -> KV.of(createJobKey(kv.getKey()), kv.getValue())))
        .apply(Reshuffle.of())
        .apply(Values.create())
        .apply(ParDo.of(new BigQueryStatementIssuingFn(bigQueryClient, mergeConfiguration)))
        .apply(MapElements.into(TypeDescriptors.voids()).via(whatever -> (Void) null));
  }

  /**
   * Creates job keys on a per-table basis, w.r.t the mergeConcurrency limit so that the target
   * BigQuery isn't overloaded with tasks.
   *
   * @param tableReference the original table name key
   * @return a number between [0, mergeConcurrency-1] that is used as key for assignment
   */
  int createJobKey(String tableReference) {
    return Math.abs(tableReference.hashCode()) % mergeConfiguration.mergeConcurrency();
  }

  /**
   * Class {@link TriggerPerKeyOnFixedIntervals}.
   *
   * @param <K> key.
   * @param <V> value.
   */
  public static class TriggerPerKeyOnFixedIntervals<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> {

    private Duration intervalDuration;

    public TriggerPerKeyOnFixedIntervals(Duration intervalDuration) {
      this.intervalDuration = intervalDuration;
    }

    @Override
    public PCollection<KV<K, V>> expand(PCollection<KV<K, V>> input) {
      return input
          .apply(
              Window.<KV<K, V>>into(new GlobalWindows())
                  .discardingFiredPanes()
                  .triggering(
                      Repeatedly.forever(
                          AfterProcessingTime.pastFirstElementInPane()
                              .alignedTo(intervalDuration)
                              .plusDelayOf(Duration.standardMinutes(5)))))
          .apply(ParDo.of(new FilterPerGroupValues<K, V>()))
          .apply(GroupByKey.create())
          .apply(
              ParDo.of(
                  new DoFn<KV<K, Iterable<V>>, KV<K, V>>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                      LOG.debug(
                          "TS: {} | Element: {} | Pane: {}", c.timestamp(), c.element(), c.pane());
                      Iterator<V> it = c.element().getValue().iterator();
                      if (it.hasNext()) {
                        c.output(KV.of(c.element().getKey(), it.next()));
                      }
                    }
                  }));
    }

    private static class FilterPerGroupValues<K, V> extends DoFn<KV<K, V>, KV<K, V>> {
      private Set<K> keysProcessed;

      @Setup
      public void setUp() {
        if (keysProcessed == null) {
          keysProcessed = new HashSet<K>();
        }
      }

      @ProcessElement
      public void process(ProcessContext c) {
        K key = c.element().getKey();
        if (!keysProcessed.contains(key)) {
          c.output(c.element());
          keysProcessed.add(key);
        }
      }

      @FinishBundle
      public void cleanKeysProcessed(FinishBundleContext c) {
        keysProcessed.clear();
      }
    }
  }

  /** Class {@link BigQueryStatementIssuingFn}. */
  public static class BigQueryStatementIssuingFn extends DoFn<MergeInfo, Void> {

    public static final String JOB_ID_PREFIX = "bigstream_to_bq";
    private final Counter mergesIssued = Metrics.counter(BigQueryMerger.class, "mergesIssued");

    private BigQuery bigQueryClient;
    private final MergeConfiguration mergeConfiguration;

    public BigQueryStatementIssuingFn(
        BigQuery bigQueryClient, MergeConfiguration mergeConfiguration) {
      this.bigQueryClient = bigQueryClient;
      this.mergeConfiguration = mergeConfiguration;
    }

    @Setup
    public void setUp() {
      if (bigQueryClient == null) {
        BigQueryOptions.Builder optionsBuilder = BigQueryOptions.newBuilder();
        if (mergeConfiguration.projectId() != null && !mergeConfiguration.projectId().isEmpty()) {
          optionsBuilder = optionsBuilder.setProjectId(mergeConfiguration.projectId());
        }
        bigQueryClient = optionsBuilder.build().getService();
      }
    }

    @Override
    public TypeDescriptor getInputTypeDescriptor() {
      return TypeDescriptor.of(MergeInfo.class);
    }

    @ProcessElement
    public void process(ProcessContext c) throws InterruptedException {
      MergeInfo mergeInfo = c.element();
      String statement = mergeInfo.buildMergeStatement(mergeConfiguration);
      try {
        TableResult queryResult = issueQueryToBQ(statement);
        mergesIssued.inc();
        LOG.info("Merge job executed: {}", statement);
      } catch (Exception e) {
        LOG.warn("Merge Job Failed: Exception: {} Statement: {}", e.toString(), statement);
        throw e;
      }
    }

    private TableResult issueQueryToBQ(String statement) throws InterruptedException {
      QueryJobConfiguration jobConfiguration = QueryJobConfiguration.newBuilder(statement).build();

      String jobId = makeJobId(JOB_ID_PREFIX, statement);

      LOG.info("Triggering job {} for statement |{}|", jobId, statement);

      TableResult result = bigQueryClient.query(jobConfiguration, JobId.of(jobId));
      return result;
    }

    String makeJobId(String jobIdPrefix, String statement) {
      DateTimeFormatter formatter =
          DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ssz").withZone(ZoneId.of("UTC"));
      String randomId = UUID.randomUUID().toString();
      return String.format(
          "%s_%d_%s_%s",
          jobIdPrefix, Math.abs(statement.hashCode()), formatter.format(Instant.now()), randomId);
    }
  }
}
