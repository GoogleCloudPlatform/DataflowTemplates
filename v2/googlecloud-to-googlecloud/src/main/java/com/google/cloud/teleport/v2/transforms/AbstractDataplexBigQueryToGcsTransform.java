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
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.utils.SerializableSchemaSupplier;
import com.google.cloud.teleport.v2.values.BigQueryTable;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Sink;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads data from BigQuery and writes it to Cloud Storage, optionally registering metadata for the
 * newly created files in Dataplex and deleting the source data afterwords.
 *
 * <p>To transform the whole table, use {@link DataplexBigQueryTableToGcsTransform} subclass, which
 * will export and delete all data in the table. To transform a single table partition, use {@link
 * DataplexBigQueryPartitionToGcsTransform}. To transform a subset of partitions in a table, combine
 * multiple {@link DataplexBigQueryPartitionToGcsTransform}s in the pipeline.
 *
 * <p>See {@link FileFormat} for the list of supported output formats.
 */
public abstract class AbstractDataplexBigQueryToGcsTransform extends PTransform<PBegin, PDone> {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractDataplexBigQueryToGcsTransform.class);

  protected final TableId tableId;
  protected final SerializableSchemaSupplier schemaSupplier;
  protected final String targetBucketName;
  private final FileFormat outputFileFormat;
  private final int numShards;

  protected abstract TypedRead<GenericRecord> getBigQueryRead();

  protected abstract String getTargetPath();

  protected abstract String entityName();

  protected abstract DoFn<Void, Void> createTruncateBigQueryFn();

  protected AbstractDataplexBigQueryToGcsTransform(
      Options options, String targetBucketName, BigQueryTable table) {
    this.schemaSupplier = SerializableSchemaSupplier.of(table.getSchema());
    this.targetBucketName = targetBucketName;
    this.tableId =
        TableId.of(
            table.getDatasetId().getProject(),
            table.getDatasetId().getDataset(),
            table.getTableName());
    this.outputFileFormat = options.getFileFormat();
    this.numShards = options.getNumShards();
  }

  @Override
  public PDone expand(PBegin begin) {
    Sink<GenericRecord> sink;
    switch (outputFileFormat) {
      case PARQUET:
        sink = ParquetIO.sink(schemaSupplier.get());
        break;
      case AVRO:
        sink = AvroIO.sink(schemaSupplier.get());
        break;
      default:
        throw new UnsupportedOperationException(
            "Output format is not implemented: " + outputFileFormat);
    }

    WriteFilesResult<Void> writeResult =
        begin
            .apply(nodeName("ReadFromBigQuery"), getBigQueryRead())
            .apply(
                nodeName("WriteToStorage"),
                FileIO.<GenericRecord>write()
                    .via(sink)
                    .to(getTargetPath())
                    .withNumShards(numShards)
                    .withSuffix(outputFileFormat.fileSuffix));

    PCollection<List<String>> updateMetadataResults =
        writeResult
            .getPerDestinationOutputFilenames()
            .apply(
                // We may potentially have more than 1 output file per table/partition (depending
                // on numShards). This will combine all these files into 1 list, to later make a
                // single call to Dataplex Metadata update API.
                nodeName("CombineOutputMetadata"),
                Combine.globally(
                    new AccumulateListFn<KV<Void, String>, String>() {
                      @Override
                      protected String convertInput(KV<Void, String> input) {
                        return input.getValue();
                      }
                    }))
            .apply(nodeName("UpdateDataplexMetadata"), ParDo.of(new UpdateDataplexMetadataFn()));

    begin
        // Wait.on can be applied to a PCollection only, so we need this fake 1-element collection.
        .apply(nodeName("PostProcess"), Create.of((Void) null))
        .apply(nodeName("WaitForWriteCompletion"), Wait.on(updateMetadataResults))
        .apply(nodeName("TruncateBigQueryData"), ParDo.of(createTruncateBigQueryFn()));

    return PDone.in(begin.getPipeline());
  }

  private class UpdateDataplexMetadataFn extends DoFn<List<String>, List<String>> {
    @ProcessElement
    public void processElement(
        @Element List<String> input, OutputReceiver<List<String>> out, PipelineOptions options) {
      if (!options.as(Options.class).getUpdateDataplexMetadata()) {
        LOG.info("Skipping Dataplex Metadata update for {}.", entityName());
      } else {
        // TODO(an2x): implement.
        LOG.warn(
            "Would've updated Dataplex Metadata for {}, but not implemented yet."
                + " Files created:\n{}",
            entityName(),
            input);
      }
      out.output(input);
    }
  }

  private String nodeName(String prefix) {
    return String.format("%s-%s", prefix, entityName());
  }

  /** Possible output file formats supported by {@link AbstractDataplexBigQueryToGcsTransform}. */
  public enum FileFormat {
    PARQUET(".parquet"),
    AVRO(".avro"),
    ORC(".orc");

    private final String fileSuffix;

    FileFormat(String fileSuffix) {
      this.fileSuffix = fileSuffix;
    }
  }

  /** Pipeline options supported by {@link AbstractDataplexBigQueryToGcsTransform}. */
  public interface Options extends PipelineOptions {
    @Description("Output file format in GCS. Format: PARQUET, AVRO, or ORC. Default: PARQUET.")
    @Default.Enum("PARQUET")
    @Required
    FileFormat getFileFormat();

    void setFileFormat(FileFormat fileFormat);

    @Description(
        "Whether to delete source data from BigQuery after a successful export. Default: NO.")
    @Default.Boolean(false)
    @Required
    Boolean getDeleteSourceData();

    void setDeleteSourceData(Boolean deleteSourceData);

    @Description("Whether to update Dataplex metadata for the newly created assets. Default: YES.")
    @Default.Boolean(true)
    @Required
    Boolean getUpdateDataplexMetadata();

    void setUpdateDataplexMetadata(Boolean updateDataplexMetadata);

    @Description("Optional: Number of shards for the output files. Default: 0 (auto-determined).")
    @Default.Integer(0)
    @Required
    Integer getNumShards();

    void setNumShards(Integer numShards);
  }

  @VisibleForTesting
  BigQuery createBqClient() {
    return BigQueryOptions.getDefaultInstance().getService();
  }
}
