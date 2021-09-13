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
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.values.BigQueryTable;
import com.google.cloud.teleport.v2.values.BigQueryTablePartition;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads a single table partition from BigQuery and writes it to Cloud Storage.
 *
 * <p>Files will be written to gs://{@code targetBucketName}/&lt;table name&gt;/&lt;partitioning
 * column&gt;=&lt;partition id&gt;/ by default.
 *
 * <p>See {@link AbstractDataplexBigQueryToGcsTransform} for more details.
 */
public class DataplexBigQueryPartitionToGcsTransform
    extends AbstractDataplexBigQueryToGcsTransform {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataplexBigQueryPartitionToGcsTransform.class);

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
  public static final List<String> SPECIAL_BIGQUERY_PARTITION_NAMES =
      Arrays.asList("__NULL__", "__UNPARTITIONED__");

  private final String partitionName;
  private final String partitioningColumn;

  public DataplexBigQueryPartitionToGcsTransform(
      Options options,
      String targetBucketName,
      BigQueryTable table,
      BigQueryTablePartition partition) {

    super(options, targetBucketName, table);
    this.partitionName = partition.getPartitionName();
    this.partitioningColumn = table.getPartitioningColumn();
  }

  @Override
  protected TypedRead<GenericRecord> getBigQueryRead() {
    String sql =
        String.format(
            "select * from [%s.%s.%s$%s]",
            tableId.getProject(), tableId.getDataset(), tableId.getTable(), partitionName);

    return BigQueryIO.read(SchemaAndRecord::getRecord)
        .fromQuery(sql)
        .withTemplateCompatibility()
        // TODO: Switch to DIRECT_READ when the BigQueryIO bug is fixed.
        // There is probably a bug in BigQueryIO that causes "IllegalMutationException:
        // PTransform BigQueryIO.TypedRead/ParDo(Anonymous)/ParMultiDo(Anonymous) mutated value ...
        // after it was output" when using read() + DIRECT_READ + some other conditions.
        .withMethod(TypedRead.Method.EXPORT)
        .withCoder(AvroCoder.of(schemaSupplier.get()));
  }

  @Override
  protected String getTargetPath() {
    return String.format(
        "gs://%s/%s/%s=%s",
        targetBucketName, tableId.getTable(), partitioningColumn, partitionName);
  }

  @Override
  protected String entityName() {
    return String.format("%s-%s", tableId.getTable(), partitionName);
  }

  @Override
  protected DoFn<Void, Void> createTruncateBigQueryFn() {
    return new TruncateBigQueryFn();
  }

  private class TruncateBigQueryFn extends DoFn<Void, Void> {
    private BigQuery bqClient;

    @Setup
    public void setup() {
      bqClient = createBqClient();
    }

    @ProcessElement
    public void processElement(PipelineOptions options) {
      if (!options.as(Options.class).getDeleteSourceData()) {
        LOG.info(
            "Skipping source BigQuery data deletion for partition {}${}.",
            tableId.getTable(),
            partitionName);
        return;
      }

      if (SPECIAL_BIGQUERY_PARTITION_NAMES.contains(partitionName)) {
        LOG.info(
            "Skipping source BigQuery data deletion for partition {}${}. "
                + "Deleting data from special partitions is not supported.",
            tableId.getTable(),
            partitionName);
        return;
      }

      TableId tableWithPartitionId =
          TableId.of(
              tableId.getProject(), tableId.getDataset(), tableId.getTable() + "$" + partitionName);

      LOG.info(
          "DELETING source BigQuery PARTITION {}:{}.{}.",
          tableWithPartitionId.getProject(),
          tableWithPartitionId.getDataset(),
          tableWithPartitionId.getTable());

      bqClient.delete(tableWithPartitionId);
    }
  }
}
