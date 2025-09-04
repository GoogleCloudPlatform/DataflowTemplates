/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.spanner.spannerio;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DecodeMode;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.ProtobufResultSet;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.spanner.spannerio.SpannerIO.ReadAll;
import com.google.protobuf.Value;
import org.apache.beam.runners.core.metrics.ServiceCallMetric;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

/**
 * This transform reads from Cloud Spanner using the {@link com.google.cloud.spanner.BatchClient}.
 * Reads from multiple partitions are executed concurrently yet in the same read-only transaction.
 *
 * <p>WARNING: This file is forked from Apache Beam. Ensure corresponding changes are made in Apache
 * Beam to prevent code divergence. TODO: (b/402322178) Remove this local copy.
 */
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
abstract class BatchSpannerReadAsProto
    extends PTransform<PCollection<ReadOperation>, PCollection<List<Value>>> {
  private static final Logger LOG = LoggerFactory.getLogger(BatchSpannerReadAsProto.class);

  public static BatchSpannerReadAsProto create(
      SpannerConfig spannerConfig,
      PCollectionView<Transaction> txView,
      TimestampBound timestampBound) {
    return new AutoValue_BatchSpannerReadAsProto(spannerConfig, txView, timestampBound);
  }

  abstract SpannerConfig getSpannerConfig();

  abstract @Nullable
      PCollectionView<Transaction> getTxView();

  abstract TimestampBound getTimestampBound();

  /**
   * Container class to combine a ReadOperation with a Partition so that Metrics are implemented
   * properly.
   */
  @AutoValue
  protected abstract static class PartitionedReadOperation implements Serializable {
    abstract ReadOperation getReadOperation();

    abstract Partition getPartition();

    static PartitionedReadOperation create(ReadOperation readOperation, Partition partition) {
      return new AutoValue_BatchSpannerReadAsProto_PartitionedReadOperation(readOperation, partition);
    }
  }

  @Override
  public PCollection<List<Value>> expand(PCollection<ReadOperation> input) {
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
        .apply("Shuffle partitions", Reshuffle.viaRandomKey())
        .apply(
            "Read from Partitions",
            ParDo.of(new ReadFromPartitionProtoFn(getSpannerConfig(), txView)).withSideInputs(txView));
  }

  @VisibleForTesting
  static class GeneratePartitionsFn extends DoFn<ReadOperation, PartitionedReadOperation> {

    private final SpannerConfig config;
    private final PCollectionView<? extends Transaction>
        txView;

    private transient SpannerAccessor spannerAccessor;

    public GeneratePartitionsFn(
        SpannerConfig config,
        PCollectionView<? extends Transaction> txView) {
      this.config = config;
      this.txView = txView;
      checkNotNull(config.getRpcPriority());
    }

    @Setup
    public void setup() throws Exception {
      spannerAccessor =
          SpannerAccessor.getOrCreate(config);
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
          config.getDataBoostEnabled() != null && config.getDataBoostEnabled().get();

      // While this creates a ServiceCallMetric for every input element, in reality, the number
      // of input elements will either be very few (normally 1!), or they will differ and
      // need different metrics.
      ServiceCallMetric metric = ReadAll.buildServiceCallMetricForReadOp(config, op);

      List<Partition> partitions;
      try {
        if (op.getQuery() != null) {
          // Query was selected.
          partitions =
              batchTx.partitionQuery(
                  op.getPartitionOptions(),
                  op.getQuery(),
                  Options.priority(config.getRpcPriority().get()),
                  Options.dataBoostEnabled(dataBoostEnabled),
                  Options.decodeMode(DecodeMode.LAZY_PER_COL)); // todo luv:maybe only this is needed and others are not
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
                  Options.dataBoostEnabled(dataBoostEnabled),
                  Options.decodeMode(DecodeMode.LAZY_PER_COL));
        } else {
          // Read from table was selected.
          partitions =
              batchTx.partitionRead(
                  op.getPartitionOptions(),
                  op.getTable(),
                  op.getKeySet(),
                  op.getColumns(),
                  Options.priority(config.getRpcPriority().get()),
                  Options.dataBoostEnabled(dataBoostEnabled),
                  Options.decodeMode(DecodeMode.LAZY_PER_COL));
        }
        metric.call("ok");
      } catch (SpannerException e) {
        metric.call(e.getErrorCode().getGrpcStatusCode().toString());
        throw e;
      }
      for (Partition p : partitions) {
        c.output(PartitionedReadOperation.create(op, p));
      }
    }
  }

  private static class ReadFromPartitionFn extends DoFn<PartitionedReadOperation, Struct> {

    private final SpannerConfig config;
    private final PCollectionView<? extends Transaction>
        txView;

    private transient SpannerAccessor spannerAccessor;
    private transient LoadingCache<ReadOperation, ServiceCallMetric> metricsForReadOperation;

    // resolved at runtime for metrics report purpose. SpannerConfig may not have projectId set.
    private transient String projectId;
    private transient @Nullable String reportedLineage;

    public ReadFromPartitionFn(
        SpannerConfig config,
        PCollectionView<? extends Transaction> txView) {
      this.config = config;
      this.txView = txView;
    }

    @Setup
    public void setup() throws Exception {
      spannerAccessor = SpannerAccessor.getOrCreate(config);

      // Use a LoadingCache for metrics as there can be different read operations which result in
      // different service call metrics labels. ServiceCallMetric items are created on-demand and
      // added to the cache.
      metricsForReadOperation =
          CacheBuilder.newBuilder()
              .maximumSize(SpannerIO.METRICS_CACHE_SIZE)
              // worker.
              .build(
                  new CacheLoader<ReadOperation, ServiceCallMetric>() {
                    @Override
                    public ServiceCallMetric load(ReadOperation op) {
                      return ReadAll.buildServiceCallMetricForReadOp(config, op);
                    }
                  });
      projectId = SpannerIO.resolveSpannerProjectId(config);
    }

    @Teardown
    public void teardown() throws Exception {
      spannerAccessor.close();
      metricsForReadOperation.invalidateAll();
      metricsForReadOperation.cleanUp();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Transaction tx = c.sideInput(txView);

      BatchReadOnlyTransaction batchTx =
          spannerAccessor.getBatchClient().batchReadOnlyTransaction(tx.transactionId());

      PartitionedReadOperation op = c.element();
      ServiceCallMetric serviceCallMetric = metricsForReadOperation.get(op.getReadOperation());
      try (ResultSet resultSet = batchTx.execute(op.getPartition())) {
        while (resultSet.next()) {
          Struct s = resultSet.getCurrentRowAsStruct();
          c.output(s);
        }
      } catch (SpannerException e) {
        serviceCallMetric.call(e.getErrorCode().getGrpcStatusCode().toString());
        LOG.error(
            "Error while reading partition for operation: " + op.getReadOperation().toString(), e);
        throw (e);
      }
      serviceCallMetric.call("ok");
      // Report Lineage metrics
      @Nullable String tableName = op.getReadOperation().tryGetTableName();
      if (!Objects.equals(reportedLineage, tableName)) {
        ImmutableList.Builder<String> segments =
            ImmutableList.<String>builder()
                .add(
                    projectId,
                    spannerAccessor.getInstanceConfigId(),
                    config.getInstanceId().get(),
                    config.getDatabaseId().get());
        if (tableName != null) {
          segments.add(tableName);
        }
        Lineage.getSources().add("spanner", segments.build());
        reportedLineage = tableName;
      }
    }
  }

  private static class ReadFromPartitionProtoFn extends DoFn<PartitionedReadOperation, List<Value>> {

    private final SpannerConfig config;
    private final PCollectionView<? extends Transaction>
        txView;

    private transient SpannerAccessor spannerAccessor;
    private transient LoadingCache<ReadOperation, ServiceCallMetric> metricsForReadOperation;

    // resolved at runtime for metrics report purpose. SpannerConfig may not have projectId set.
    private transient String projectId;
    private transient @Nullable String reportedLineage;

    public ReadFromPartitionProtoFn(
        SpannerConfig config,
        PCollectionView<? extends Transaction> txView) {
      this.config = config;
      this.txView = txView;
    }

    @Setup
    public void setup() throws Exception {
      spannerAccessor = SpannerAccessor.getOrCreate(config);

      // Use a LoadingCache for metrics as there can be different read operations which result in
      // different service call metrics labels. ServiceCallMetric items are created on-demand and
      // added to the cache.
      metricsForReadOperation =
          CacheBuilder.newBuilder()
              .maximumSize(SpannerIO.METRICS_CACHE_SIZE)
              // worker.
              .build(
                  new CacheLoader<ReadOperation, ServiceCallMetric>() {
                    @Override
                    public ServiceCallMetric load(ReadOperation op) {
                      return ReadAll.buildServiceCallMetricForReadOp(config, op);
                    }
                  });
      projectId = SpannerIO.resolveSpannerProjectId(config);
    }

    @Teardown
    public void teardown() throws Exception {
      spannerAccessor.close();
      metricsForReadOperation.invalidateAll();
      metricsForReadOperation.cleanUp();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Transaction tx = c.sideInput(txView);

      BatchReadOnlyTransaction batchTx =
          spannerAccessor.getBatchClient().batchReadOnlyTransaction(tx.transactionId());

      PartitionedReadOperation op = c.element();
      ServiceCallMetric serviceCallMetric = metricsForReadOperation.get(op.getReadOperation());
      try (ProtobufResultSet resultSet = (ProtobufResultSet) batchTx.execute(op.getPartition())) {
        while (resultSet.next()) {
          List<Value> row = new ArrayList<>();
          for (int i = 0; i < resultSet.getColumnCount(); i++) {
            row.add(resultSet.getProtobufValue(i));
          }
          c.output(row);
        }
      } catch (SpannerException e) {
        serviceCallMetric.call(e.getErrorCode().getGrpcStatusCode().toString());
        LOG.error(
            "Error while reading partition for operation: " + op.getReadOperation().toString(), e);
        throw (e);
      }
      serviceCallMetric.call("ok");
      // Report Lineage metrics
      @Nullable String tableName = op.getReadOperation().tryGetTableName();
      if (!Objects.equals(reportedLineage, tableName)) {
        ImmutableList.Builder<String> segments =
            ImmutableList.<String>builder()
                .add(
                    projectId,
                    spannerAccessor.getInstanceConfigId(),
                    config.getInstanceId().get(),
                    config.getDatabaseId().get());
        if (tableName != null) {
          segments.add(tableName);
        }
        Lineage.getSources().add("spanner", segments.build());
        reportedLineage = tableName;
      }
    }
  }
}
