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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.teleport.v2.clients.DataplexClient;
import com.google.cloud.teleport.v2.clients.DefaultDataplexClient;
import com.google.cloud.teleport.v2.transforms.BigQueryTableToGcsTransform;
import com.google.cloud.teleport.v2.transforms.BigQueryTableToGcsTransform.FileFormat;
import com.google.cloud.teleport.v2.transforms.DeleteBigQueryDataFn;
import com.google.cloud.teleport.v2.transforms.DeleteBigQueryDataFn.BigQueryClientFactory;
import com.google.cloud.teleport.v2.transforms.UpdateDataplexBigQueryToGcsExportMetadataTransform;
import com.google.cloud.teleport.v2.utils.BigQueryMetadataLoader;
import com.google.cloud.teleport.v2.utils.BigQueryUtils;
import com.google.cloud.teleport.v2.values.BigQueryTable;
import com.google.cloud.teleport.v2.values.BigQueryTablePartition;
import com.google.cloud.teleport.v2.values.DataplexAssetResourceSpec;
import com.google.cloud.teleport.v2.values.DataplexCompression;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DataplexBigQueryToGcs} pipeline exports all tables from a BigQuery dataset to Cloud
 * Storage, registering metadata for the newly created files in Dataplex.
 *
 * <p>Accepts a Dataplex asset as the source and the destination, which will be resolved to the
 * corresponding BigQuery dataset/Storage bucket via Dataplex API.
 *
 * <p>For partitioned tables, supports exporting a subset of partitions. Supports filtering tables
 * by name, filtering tables and partitions by modification time, deleting data in BigQuery after
 * export.
 *
 * <p>Please refer to <a href=
 * "https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/master/v2/googlecloud-to-googlecloud/docs/DataplexBigQueryToGcs/README.md">
 * README</a> for further information.
 */
public class DataplexBigQueryToGcs {
  private static final Logger LOG = LoggerFactory.getLogger(DataplexBigQueryToGcs.class);

  /**
   * The {@link DataplexBigQueryToGcsOptions} class provides the custom execution options passed by
   * the executor at the command-line.
   */
  public interface DataplexBigQueryToGcsOptions
      extends GcpOptions,
          ExperimentalOptions,
          DeleteBigQueryDataFn.Options,
          UpdateDataplexBigQueryToGcsExportMetadataTransform.Options {

    @Description(
        "Dataplex asset name for the the BigQuery dataset to tier data from. Format:"
            + " projects/<name>/locations/<loc>/lakes/<lake-name>/zones/<zone-name>/assets/<asset"
            + " name>.")
    @Required
    String getSourceBigQueryAssetName();

    void setSourceBigQueryAssetName(String sourceBigQueryAssetName);

    @Description(
        "A comma-separated list of BigQuery tables to tier. If none specified, all tables will be"
            + " tiered. Tables should be specified by their name only (no project/dataset prefix)."
            + " Case-sensitive!")
    String getTableRefs();

    void setTableRefs(String tableRefs);

    @Description(
        "Dataplex asset name for the the GCS bucket to tier data to. Format:"
            + " projects/<name>/locations/<loc>/lakes/<lake-name>/zones/<zone-name>/assets/<asset"
            + " name>.")
    @Required
    String getDestinationGcsBucketAssetName();

    void setDestinationGcsBucketAssetName(String destinationGcsBucketAssetName);

    @Description(
        "The parameter can either be: 1) unspecified, 2) date (and optional time) 3) Duration.\n"
            + "1) If not specified move all tables / partitions.\n"
            + "2) Move data older than this date (and optional time). For partitioned tables, move"
            + " partitions last modified before this date/time. For non-partitioned tables, move if"
            + " the table was last modified before this date/time. If not specified, move all"
            + " tables / partitions. The date/time is parsed in the default time zone by default,"
            + " but optinal suffixes Z and +HH:mm are supported. Format: YYYY-MM-DD or"
            + " YYYY-MM-DDTHH:mm:ss or YYYY-MM-DDTHH:mm:ss+03:00.\n"
            + "3) Similar to the above (2) but the effective date-time is derived from the current"
            + " time in the default/system timezone shifted by the provided duration in the format"
            + " based on ISO-8601 +/-PnDTnHnMn.nS "
            + "(https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-)."
            + " However only \"minus\" durations are accepted so only past effective date-times are"
            + " possible.")
    String getExportDataModifiedBeforeDateTime();

    void setExportDataModifiedBeforeDateTime(String exportDataModifiedBeforeDateTime);

    @Description(
        "The maximum number of parallel requests that will be sent to BigQuery when loading"
            + " table/partition metadata. Default: 5.")
    @Default.Integer(5)
    @Required
    Integer getMaxParallelBigQueryMetadataRequests();

    void setMaxParallelBigQueryMetadataRequests(Integer maxParallelBigQueryMetadataRequests);

    @Description("Output file format in GCS. Format: PARQUET, AVRO, or ORC. Default: PARQUET.")
    @Default.Enum("PARQUET")
    @Required
    FileFormat getFileFormat();

    void setFileFormat(FileFormat fileFormat);

    @Description(
        "Output file compression. Format: UNCOMPRESSED, SNAPPY, GZIP, or BZIP2. Default:"
            + " SNAPPY. BZIP2 not supported for PARQUET files.")
    @Default.Enum("SNAPPY")
    DataplexCompression getFileCompression();

    void setFileCompression(DataplexCompression fileCompression);

    @Description(
        "Process partitions with partition ID matching this regexp only. Default: process all.")
    String getPartitionIdRegExp();

    void setPartitionIdRegExp(String partitionIdRegExp);
  }

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args)
      throws IOException, InterruptedException, ExecutionException {

    DataplexBigQueryToGcsOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataplexBigQueryToGcsOptions.class);

    List<String> experiments = new ArrayList<>();
    if (options.getExperiments() != null) {
      experiments.addAll(options.getExperiments());
    }
    if (!experiments.contains("upload_graph")) {
      experiments.add("upload_graph");
    }
    options.setExperiments(experiments);

    Pipeline pipeline;

    DataplexClient dataplex = DefaultDataplexClient.withDefaultClient(options.getGcpCredential());
    BigQuery bqClient = BigQueryOptions.getDefaultInstance().getService();
    try (BigQueryStorageClient bqsClient = BigQueryStorageClient.create()) {
      LOG.info("Building the pipeline...");
      pipeline = buildPipeline(options, dataplex, bqClient, bqsClient);
    }

    LOG.info("Running the pipeline.");
    pipeline.run();
  }

  /**
   * Builds the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The resulting pipeline.
   */
  private static Pipeline buildPipeline(
      DataplexBigQueryToGcsOptions options,
      DataplexClient dataplex,
      BigQuery bqClient,
      BigQueryStorageClient bqsClient)
      throws IOException, ExecutionException, InterruptedException {

    Pipeline pipeline = Pipeline.create(options);

    int maxParallelBigQueryRequests = options.getMaxParallelBigQueryMetadataRequests();
    checkArgument(
        maxParallelBigQueryRequests >= 1,
        "maxParallelBigQueryMetadataRequests must be >= 1, but was: %s",
        maxParallelBigQueryRequests);

    String gcsResource =
        resolveAsset(
            dataplex,
            options.getDestinationGcsBucketAssetName(),
            DataplexAssetResourceSpec.STORAGE_BUCKET);
    String bqResource =
        resolveAsset(
            dataplex,
            options.getSourceBigQueryAssetName(),
            DataplexAssetResourceSpec.BIGQUERY_DATASET);

    String targetRootPath = "gs://" + gcsResource;
    DatasetId datasetId = BigQueryUtils.parseDatasetUrn(bqResource);
    BigQueryMetadataLoader metadataLoader =
        new BigQueryMetadataLoader(bqClient, bqsClient, maxParallelBigQueryRequests);

    LOG.info("Loading BigQuery metadata...");
    List<BigQueryTable> tables =
        metadataLoader.loadDatasetMetadata(datasetId, new MetadataFilter(options));
    LOG.info("Loaded {} table(s).", tables.size());

    if (!tables.isEmpty()) {
      transformPipeline(pipeline, tables, options, targetRootPath, null, null);
    }

    return pipeline;
  }

  @VisibleForTesting
  static void transformPipeline(
      Pipeline pipeline,
      List<BigQueryTable> tables,
      DataplexBigQueryToGcsOptions options,
      String targetRootPath,
      BigQueryServices testBqServices,
      BigQueryClientFactory testBqClientFactory) {

    List<PCollection<KV<BigQueryTable, KV<BigQueryTablePartition, String>>>> fileCollections =
        new ArrayList<>(tables.size());
    tables.forEach(
        table -> {
          fileCollections.add(
              pipeline
                  .apply(
                      String.format("ExportTable-%s", table.getTableName()),
                      new BigQueryTableToGcsTransform(
                              table,
                              targetRootPath,
                              options.getFileFormat(),
                              options.getFileCompression())
                          .withTestServices(testBqServices))
                  .apply(
                      String.format("AttachTableKeys-%s", table.getTableName()),
                      WithKeys.of(table)));
        });

    PCollection<KV<BigQueryTable, KV<BigQueryTablePartition, String>>> exportFileResults =
        PCollectionList.of(fileCollections).apply("FlattenTableResults", Flatten.pCollections());

    PCollection<Void> metadataUpdateResults =
        exportFileResults.apply(
            "UpdateDataplexMetadata", new UpdateDataplexBigQueryToGcsExportMetadataTransform());

    exportFileResults
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptor.of(BigQueryTable.class),
                        TypeDescriptor.of(BigQueryTablePartition.class)))
                .via(
                    (SerializableFunction<
                            KV<BigQueryTable, KV<BigQueryTablePartition, String>>,
                            KV<BigQueryTable, BigQueryTablePartition>>)
                        input -> KV.of(input.getKey(), input.getValue().getKey())))
        .apply("WaitForMetadataUpdate", Wait.on(metadataUpdateResults))
        .apply(
            "TruncateBigQueryData",
            ParDo.of(new DeleteBigQueryDataFn().withTestBqClientFactory(testBqClientFactory)));
  }

  /**
   * Resolves a Dataplex asset name into the corresponding resource spec, verifying that the asset
   * is of the correct type.
   */
  private static String resolveAsset(
      DataplexClient dataplex, String assetName, DataplexAssetResourceSpec expectedType)
      throws IOException {

    LOG.info("Resolving asset: {}", assetName);
    GoogleCloudDataplexV1Asset asset = dataplex.getAsset(assetName);
    checkNotNull(asset.getResourceSpec(), "Asset has no ResourceSpec.");

    String type = asset.getResourceSpec().getType();
    if (!expectedType.name().equals(type)) {
      throw new IllegalArgumentException(
          String.format(
              "Asset %s is of type %s, expected: %s.", assetName, type, expectedType.name()));
    }

    String resourceName = asset.getResourceSpec().getName();
    checkNotNull(resourceName, "Asset has no resource name.");
    LOG.info("Resolved resource name: {}", resourceName);
    return resourceName;
  }

  @VisibleForTesting
  static class MetadataFilter implements BigQueryMetadataLoader.Filter {
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private final Instant maxLastModifiedTime;
    private final Set<String> includeTables;
    private final Pattern includePartitions;

    @VisibleForTesting
    MetadataFilter(DataplexBigQueryToGcsOptions options) {
      String dateTime = options.getExportDataModifiedBeforeDateTime();
      if (dateTime != null && !dateTime.isEmpty()) {
        if (dateTime.startsWith("-P") || dateTime.startsWith("-p")) {
          this.maxLastModifiedTime = Instant.now().plus(Duration.parse(dateTime).toMillis());
        } else {
          this.maxLastModifiedTime =
              Instant.parse(dateTime, ISODateTimeFormat.dateOptionalTimeParser());
        }
      } else {
        this.maxLastModifiedTime = null;
      }

      String tableRefs = options.getTableRefs();
      if (tableRefs != null && !tableRefs.isEmpty()) {
        List<String> tableRefList = SPLITTER.splitToList(tableRefs);
        checkArgument(
            !tableRefList.isEmpty(),
            "Got an non-empty tableRefs param '%s', but couldn't parse it into a valid table list,"
                + " please check its format.",
            tableRefs);
        this.includeTables = new HashSet<>(tableRefList);
      } else {
        this.includeTables = null;
      }

      String partitionRegExp = options.getPartitionIdRegExp();
      if (partitionRegExp != null && !partitionRegExp.isEmpty()) {
        this.includePartitions = Pattern.compile(partitionRegExp);
      } else {
        this.includePartitions = null;
      }
    }

    private boolean shouldSkipTableName(BigQueryTable.Builder table) {
      if (includeTables != null && !includeTables.contains(table.getTableName())) {
        return true;
      }
      return false;
    }

    @Override
    public boolean shouldSkipUnpartitionedTable(BigQueryTable.Builder table) {
      if (shouldSkipTableName(table)) {
        return true;
      }
      // Check the last modified time only for NOT partitioned table.
      // If a table is partitioned, we check the last modified time on partition level only.
      if (maxLastModifiedTime != null
          // BigQuery timestamps are in microseconds so / 1000.
          && maxLastModifiedTime.isBefore(table.getLastModificationTime() / 1000)) {
        return true;
      }
      return false;
    }

    @Override
    public boolean shouldSkipPartitionedTable(
        BigQueryTable.Builder table, List<BigQueryTablePartition> partitions) {
      if (shouldSkipTableName(table)) {
        return true;
      }
      if (partitions.isEmpty()) {
        LOG.info(
            "Skipping table {}: "
                + "table is partitioned, but no eligible partitions found => nothing to export.",
            table.getTableName());
        return true;
      }
      return false;
    }

    @Override
    public boolean shouldSkipPartition(
        BigQueryTable.Builder table, BigQueryTablePartition partition) {
      if (maxLastModifiedTime != null
          // BigQuery timestamps are in microseconds so / 1000.
          && maxLastModifiedTime.isBefore(partition.getLastModificationTime() / 1000)) {
        return true;
      }
      if (includePartitions != null && !includePartitions.matches(partition.getPartitionName())) {
        LOG.info(
            "Skipping partition {} not matching regexp: {}",
            partition.getPartitionName(),
            includePartitions.pattern());
        return true;
      }
      return false;
    }
  }
}
