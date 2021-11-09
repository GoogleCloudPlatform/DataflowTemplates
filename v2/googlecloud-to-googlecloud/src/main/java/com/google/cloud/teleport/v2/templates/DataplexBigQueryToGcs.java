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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

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
import com.google.cloud.teleport.v2.utils.StorageUtils;
import com.google.cloud.teleport.v2.values.BigQueryTable;
import com.google.cloud.teleport.v2.values.BigQueryTablePartition;
import com.google.cloud.teleport.v2.values.DataplexAssetResourceSpec;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
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
            + " tiered. Table name format: <project>:<dataset>.<table>.")
    String getTableRefs();

    void setTableRefs(String tableRefs);

    @Description(
        "Dataplex asset name for the the GCS bucket to tier data to. Format:"
            + " projects/<name>/locations/<loc>/lakes/<lake-name>/zones/<zone-name>/assets/<asset"
            + " name>.")
    @Required
    String getDestinationGscBucketAssetName();

    void setDestinationGscBucketAssetName(String destinationGscBucketAssetName);

    // TODO(an2x): time zone handling?

    @Description(
        "Move data older than this date. For partitioned tables, move partitions last updated"
            + " before this date. For non-partitioned tables, move if the table was last updated"
            + " before this date. If not specified, move all tables / partitions. Format:"
            + " YYYY-MM-DD.")
    String getBeforeDate();

    void setBeforeDate(String beforeDate);

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

    Pipeline pipeline;

    DataplexClient dataplex = DefaultDataplexClient.withDefaultClient();
    BigQuery bqClient = BigQueryOptions.getDefaultInstance().getService();
    try (BigQueryStorageClient bqsClient = BigQueryStorageClient.create()) {
      pipeline = buildPipeline(options, dataplex, bqClient, bqsClient);
    }

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
            options.getDestinationGscBucketAssetName(),
            DataplexAssetResourceSpec.STORAGE_BUCKET);
    String bqResource =
        resolveAsset(
            dataplex,
            options.getSourceBigQueryAssetName(),
            DataplexAssetResourceSpec.BIGQUERY_DATASET);

    String targetRootPath = "gs://" + StorageUtils.parseBucketUrn(gcsResource);
    DatasetId datasetId = BigQueryUtils.parseDatasetUrn(bqResource);
    BigQueryMetadataLoader metadataLoader =
        new FilteringBigQueryMetadataLoader(bqClient, bqsClient, maxParallelBigQueryRequests);
    List<BigQueryTable> tables = metadataLoader.loadDatasetMetadata(datasetId);

    transformPipeline(pipeline, tables, options, targetRootPath, null, null);

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
                              table, targetRootPath, options.getFileFormat())
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

  private static class FilteringBigQueryMetadataLoader extends BigQueryMetadataLoader {
    public FilteringBigQueryMetadataLoader(
        BigQuery bqClient, BigQueryStorageClient bqsClient, int maxParallelRequests) {
      super(bqClient, bqsClient, maxParallelRequests);
    }

    protected boolean shouldSkipUnpartitionedTable(BigQueryTable.Builder table) {
      // TODO(an2x): check table name and last modification time.
      return false;
    }

    protected boolean shouldSkipPartitionedTable(
        BigQueryTable.Builder table, List<BigQueryTablePartition> partitions) {
      if (shouldSkipUnpartitionedTable(table)) {
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
    protected boolean shouldSkipPartition(
        BigQueryTable.Builder table, BigQueryTablePartition partition) {
      // TODO(an2x): check last modification time.
      return false;
    }
  }
}
