/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.ServiceCallMetric;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This transform reads from Cloud Spanner using the {@link com.google.cloud.spanner.BatchClient}.
 * Reads from multiple partitions are executed concurrently yet in the same read-only transaction.
 */
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
abstract class LocalBatchSpannerRead
    extends PTransform<PCollection<ReadOperation>, PCollection<Struct>> {

  public static LocalBatchSpannerRead create(
      SpannerConfig spannerConfig,
      PCollectionView<Transaction> txView,
      TimestampBound timestampBound) {
    return new AutoValue_LocalBatchSpannerRead(spannerConfig, txView, timestampBound);
  }

  abstract SpannerConfig getSpannerConfig();

  abstract @Nullable PCollectionView<Transaction> getTxView();

  abstract TimestampBound getTimestampBound();

  @Override
  public PCollection<Struct> expand(PCollection<ReadOperation> input) {
    PCollectionView<Transaction> txView = getTxView();
    if (txView == null) {
      Pipeline begin = input.getPipeline();
      SpannerIO.CreateTransaction createTx =
          SpannerIO.createTransaction()
              .withSpannerConfig(getSpannerConfig())
              .withTimestampBound(getTimestampBound());
      txView = begin.apply(createTx);
    }
    return input
        .apply(
            "Generate Partitions",
            ParDo.of(new GeneratePartitionsFn(getSpannerConfig(), txView)).withSideInputs(txView))
        .apply("Shuffle partitions", Reshuffle.<Partition>viaRandomKey())
        .apply(
            "Read from Partitions",
            ParDo.of(new ReadFromPartitionFn(getSpannerConfig(), txView)).withSideInputs(txView));
  }

  @VisibleForTesting
  static class GeneratePartitionsFn extends DoFn<ReadOperation, Partition> {

    private final SpannerConfig config;
    private final PCollectionView<? extends Transaction> txView;

    private transient LocalSpannerAccessor spannerAccessor;

    public GeneratePartitionsFn(
        SpannerConfig config, PCollectionView<? extends Transaction> txView) {
      this.config = config;
      this.txView = txView;
    }

    @Setup
    public void setup() throws Exception {
      spannerAccessor = LocalSpannerAccessor.getOrCreate(config);
    }

    @Teardown
    public void teardown() throws Exception {
      spannerAccessor.close();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Transaction tx = c.sideInput(txView);
      BatchReadOnlyTransaction batchTx =
          spannerAccessor.getBatchClient().batchReadOnlyTransaction(tx.transactionId());
      ReadOperation op = c.element();
      boolean dataBoostEnabled =
          config.getDataBoostEnabled() != null
              && Boolean.TRUE.equals(config.getDataBoostEnabled().get());

      List<Partition> partitions;
      if (op.getQuery() != null) {
        // Query was selected.
        partitions =
            batchTx.partitionQuery(
                op.getPartitionOptions(),
                op.getQuery(),
                Options.priority(config.getRpcPriority().get()),
                Options.dataBoostEnabled(dataBoostEnabled));
      } else if (op.getIndex() != null) {
        // Read with index was selected.
        partitions =
            batchTx.partitionReadUsingIndex(
                op.getPartitionOptions(),
                op.getTable(),
                op.getIndex(),
                op.getKeySet(),
                op.getColumns(),
                Options.priority(config.getRpcPriority().get()),
                Options.dataBoostEnabled(dataBoostEnabled));
      } else {
        // Read from table was selected.
        partitions =
            batchTx.partitionRead(
                op.getPartitionOptions(),
                op.getTable(),
                op.getKeySet(),
                op.getColumns(),
                Options.priority(config.getRpcPriority().get()),
                Options.dataBoostEnabled(dataBoostEnabled));
      }

      for (Partition p : partitions) {
        c.output(p);
      }
    }
  }

  private static class ReadFromPartitionFn extends DoFn<Partition, Struct> {

    private final SpannerConfig config;
    private final PCollectionView<? extends Transaction> txView;

    private transient LocalSpannerAccessor spannerAccessor;
    private transient String projectId;
    private transient ServiceCallMetric serviceCallMetric;

    public ReadFromPartitionFn(
        SpannerConfig config, PCollectionView<? extends Transaction> txView) {
      this.config = config;
      this.txView = txView;
    }

    @Setup
    public void setup() throws Exception {
      spannerAccessor = LocalSpannerAccessor.getOrCreate(config);
      projectId =
          this.config.getProjectId() == null
                  || this.config.getProjectId().get() == null
                  || this.config.getProjectId().get().isEmpty()
              ? SpannerOptions.getDefaultProjectId()
              : this.config.getProjectId().get();
    }

    @Teardown
    public void teardown() throws Exception {
      spannerAccessor.close();
    }

    @StartBundle
    public void startBundle() throws Exception {
      serviceCallMetric =
          createServiceCallMetric(
              projectId,
              this.config.getInstanceId().get(),
              this.config.getDatabaseId().get(),
              this.config.getInstanceId().get());
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Transaction tx = c.sideInput(txView);

      BatchReadOnlyTransaction batchTx =
          spannerAccessor.getBatchClient().batchReadOnlyTransaction(tx.transactionId());

      Partition p = c.element();
      try (ResultSet resultSet = batchTx.execute(p)) {
        while (resultSet.next()) {
          Struct s = resultSet.getCurrentRowAsStruct();
          c.output(s);
        }
      } catch (SpannerException e) {
        serviceCallMetric.call(e.getErrorCode().getGrpcStatusCode().toString());
        throw (e);
      }
      serviceCallMetric.call("ok");
    }

    private ServiceCallMetric createServiceCallMetric(
        String projectId, String instanceId, String databaseId, String tableId) {
      HashMap<String, String> baseLabels = new HashMap<>();
      baseLabels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
      baseLabels.put(MonitoringInfoConstants.Labels.SERVICE, "Spanner");
      baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "Read");
      baseLabels.put(
          MonitoringInfoConstants.Labels.RESOURCE,
          GcpResourceIdentifiers.spannerTable(projectId, instanceId, databaseId, tableId));
      baseLabels.put(MonitoringInfoConstants.Labels.SPANNER_PROJECT_ID, projectId);
      baseLabels.put(MonitoringInfoConstants.Labels.SPANNER_DATABASE_ID, databaseId);
      baseLabels.put(MonitoringInfoConstants.Labels.SPANNER_INSTANCE_ID, tableId);
      ServiceCallMetric serviceCallMetric =
          new ServiceCallMetric(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);
      return serviceCallMetric;
    }
  }
}
