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
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.teleport.v2.values.BigQueryTable;
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
 * Reads a table from BigQuery and writes it to Cloud Storage (all partitions, if partitioned).
 *
 * <p>Files will be written to gs://{@code targetBucketName}/&lt;table name&gt;/ by default.
 *
 * <p>See {@link AbstractDataplexBigQueryToGcsTransform} for more details.
 */
public class DataplexBigQueryTableToGcsTransform extends AbstractDataplexBigQueryToGcsTransform {
  private static final Logger LOG =
      LoggerFactory.getLogger(DataplexBigQueryTableToGcsTransform.class);

  public DataplexBigQueryTableToGcsTransform(
      Options options, String targetBucketName, BigQueryTable table) {
    super(options, targetBucketName, table);
  }

  @Override
  protected TypedRead<GenericRecord> getBigQueryRead() {
    String tableSpec =
        String.format("%s:%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());

    return BigQueryIO.read(SchemaAndRecord::getRecord)
        .from(tableSpec)
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
    return String.format("gs://%s/%s", targetBucketName, tableId.getTable());
  }

  @Override
  protected String entityName() {
    return tableId.getTable();
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
        LOG.info("Skipping source BigQuery data deletion for table {}.", tableId.getTable());
        return;
      }

      LOG.info(
          "TRUNCATING source BigQuery TABLE {}:{}.{}.",
          tableId.getProject(),
          tableId.getDataset(),
          tableId.getTable());

      String sql =
          String.format(
              "truncate table `%s.%s.%s`",
              tableId.getProject(), tableId.getDataset(), tableId.getTable());
      try {
        bqClient.query(QueryJobConfiguration.newBuilder(sql).build());
      } catch (InterruptedException e) {
        LOG.warn("BigQuery query interrupted: " + sql, e);
        Thread.currentThread().interrupt();
      }
    }
  }
}
