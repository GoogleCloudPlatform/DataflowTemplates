/*
 * Copyright (C) 2021 Google LLC
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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.values.BigQueryTable;
import com.google.cloud.teleport.v2.values.BigQueryTablePartition;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes (truncates) data in BigQuery tables/partitions.
 *
 * <p>If the partition in the input {@code <BigQueryTable, BigQueryTablePartition>} key/value pair
 * is {@code null} then truncates the whole table. Otherwise truncates a single partition only.
 */
public class DeleteBigQueryDataFn extends DoFn<KV<BigQueryTable, BigQueryTablePartition>, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(DeleteBigQueryDataFn.class);

  /**
   * List of special BigQuery partitions that we don't support deleting data in.
   *
   * <p>We can't delete these partitions (as per
   * https://cloud.google.com/bigquery/docs/managing-partitioned-tables#delete_a_partition) and we
   * can't write a query to delete data in these partitions without very sophisticated logic, e.g.
   * __UNPARTITIONED__ partition can contain values that didn't fit into any other partition -- we
   * don't know which values, and we won't be able to query this partition without specifying a
   * filter on the partitioning column if "require partition filter" flag is set.
   */
  public static final Set<String> SPECIAL_BIGQUERY_PARTITION_NAMES =
      new HashSet<>(Arrays.asList("__NULL__", "__UNPARTITIONED__"));

  private BigQuery bqClient;
  private BigQueryClientFactory testBqClientFactory;

  @Setup
  public void setup() {
    bqClient =
        testBqClientFactory != null
            ? testBqClientFactory.createClient()
            : BigQueryOptions.getDefaultInstance().getService();
  }

  @ProcessElement
  public void processElement(
      @Element KV<BigQueryTable, BigQueryTablePartition> input, PipelineOptions options) {

    BigQueryTable t = input.getKey();
    BigQueryTablePartition p = input.getValue();

    if (t.isPartitioned() && p == null) {
      throw new IllegalStateException(
          String.format(
              "No partition to delete provided for a partitioned table %s.", t.getTableName()));
    }
    if (!t.isPartitioned() && p != null) {
      throw new IllegalStateException(
          String.format(
              "Got unexpected partition %s to delete for a non-partitioned table %s.",
              p.getPartitionName(), t.getTableName()));
    }
    if (!options.as(Options.class).getDeleteSourceData()) {
      if (t.isPartitioned()) {
        LOG.info(
            "Skipping source BigQuery data deletion for partition {}${}.",
            t.getTableName(),
            p.getPartitionName());
      } else {
        LOG.info("Skipping source BigQuery data deletion for table {}.", t.getTableName());
      }
      return;
    }

    if (t.isPartitioned()) {
      deletePartition(t, p);
    } else {
      deleteTable(t);
    }
  }

  private void deletePartition(BigQueryTable t, BigQueryTablePartition p) {
    if (SPECIAL_BIGQUERY_PARTITION_NAMES.contains(p.getPartitionName())) {
      LOG.info(
          "Skipping source BigQuery data deletion for partition {}${}. "
              + "Deleting data from special partitions is not supported.",
          t.getTableName(),
          p.getPartitionName());
      return;
    }

    TableId tableWithPartitionId =
        TableId.of(t.getProject(), t.getDataset(), t.getTableName() + "$" + p.getPartitionName());

    LOG.info(
        "DELETING source BigQuery PARTITION {}:{}.{}.",
        tableWithPartitionId.getProject(),
        tableWithPartitionId.getDataset(),
        tableWithPartitionId.getTable());

    bqClient.delete(tableWithPartitionId);
  }

  private void deleteTable(BigQueryTable t) {
    LOG.info(
        "TRUNCATING source BigQuery TABLE {}:{}.{}.",
        t.getProject(),
        t.getDataset(),
        t.getTableName());

    String sql =
        String.format(
            "truncate table `%s.%s.%s`", t.getProject(), t.getDataset(), t.getTableName());
    try {
      bqClient.query(QueryJobConfiguration.newBuilder(sql).build());
    } catch (InterruptedException e) {
      LOG.warn("BigQuery query interrupted: " + sql, e);
      Thread.currentThread().interrupt();
    }
  }

  @VisibleForTesting
  public DeleteBigQueryDataFn withTestBqClientFactory(BigQueryClientFactory bqClientFactory) {
    this.testBqClientFactory = bqClientFactory;
    return this;
  }

  /**
   * The factory used by {@link DeleteBigQueryDataFn} to create {@link BigQuery} client instances.
   *
   * <p>See {@link #withTestBqClientFactory(BigQueryClientFactory)}. Use for testing only to provide
   * a mock client implementation.
   */
  @VisibleForTesting
  public interface BigQueryClientFactory extends Serializable {
    BigQuery createClient();
  }

  /** Pipeline options supported by {@link DeleteBigQueryDataFn}. */
  public interface Options extends PipelineOptions {
    @TemplateParameter.Boolean(
        order = 1,
        optional = true,
        description = "Delete source data from BigQuery.",
        helpText =
            "Whether to delete source data from BigQuery after a successful export. Format: true or false.")
    @Default.Boolean(false)
    @Required
    Boolean getDeleteSourceData();

    void setDeleteSourceData(Boolean deleteSourceData);
  }
}
