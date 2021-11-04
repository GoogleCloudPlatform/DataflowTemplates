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

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset;
import com.google.cloud.teleport.v2.clients.DataplexClient;
import com.google.cloud.teleport.v2.clients.DefaultDataplexClient;
import com.google.cloud.teleport.v2.io.DynamicJdbcIO;
import com.google.cloud.teleport.v2.io.DynamicJdbcIO.DynamicDataSourceConfiguration;
import com.google.cloud.teleport.v2.options.DataplexJdbcIngestionOptions;
import com.google.cloud.teleport.v2.transforms.BeamRowToGenericRecordFn;
import com.google.cloud.teleport.v2.transforms.GenericRecordsToGcsPartitioned;
import com.google.cloud.teleport.v2.utils.JdbcConverters;
import com.google.cloud.teleport.v2.utils.KMSEncryptedNestedValue;
import com.google.cloud.teleport.v2.utils.Schemas;
import com.google.cloud.teleport.v2.values.DataplexAssetResourceSpec;
import com.google.cloud.teleport.v2.values.PartitionMetadata;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.BeamSchemaUtil;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DataplexJdbcIngestion} pipeline reads from Jdbc and exports results to a BigQuery
 * dataset or to Cloud Storage, registering metadata in Dataplex.
 *
 * <p>Accepts a Dataplex asset as the destination, which will be resolved to the corresponding
 * BigQuery dataset/Storage bucket via Dataplex API.
 *
 * <p>TODO: add more comments later
 */
public class DataplexJdbcIngestion {

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(DataplexJdbcIngestion.class);

  private static KMSEncryptedNestedValue maybeDecrypt(String unencryptedValue, String kmsKey) {
    return new KMSEncryptedNestedValue(unencryptedValue, kmsKey);
  }

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) throws IOException {
    DataplexJdbcIngestionOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataplexJdbcIngestionOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    DataplexClient dataplexClient =
        DefaultDataplexClient.withDefaultClient(options.getGcpCredential());
    String assetName = options.getOutputAsset();
    GoogleCloudDataplexV1Asset asset = resolveAsset(assetName, dataplexClient);

    DynamicDataSourceConfiguration dataSourceConfig = configDataSource(options);
    String assetType = asset.getResourceSpec().getType();
    if (DataplexAssetResourceSpec.BIGQUERY_DATASET.name().equals(assetType)) {
      buildBigQueryPipeline(pipeline, options, dataSourceConfig);
    } else if (DataplexAssetResourceSpec.STORAGE_BUCKET.name().equals(assetType)) {
      String targetRootPath =
          "gs://" + asset.getResourceSpec().getName() + "/" + options.getOutputTable();
      buildGcsPipeline(pipeline, options, dataSourceConfig, targetRootPath);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Asset "
                  + assetName
                  + " is of type "
                  + assetType
                  + ". Only "
                  + DataplexAssetResourceSpec.BIGQUERY_DATASET.name()
                  + "and "
                  + DataplexAssetResourceSpec.STORAGE_BUCKET.name()
                  + " supported."));
    }

    pipeline.run();
  }

  /**
   * Resolves a Dataplex asset.
   *
   * @param assetName Asset name from which the Dataplex asset will be resolved.
   * @param dataplexClient Dataplex client to connect to Dataplex via asset name.
   * @return The resolved asset
   */
  private static GoogleCloudDataplexV1Asset resolveAsset(
      String assetName, DataplexClient dataplexClient) throws IOException {

    LOG.info("Resolving asset: {}", assetName);
    GoogleCloudDataplexV1Asset asset = dataplexClient.getAsset(assetName);
    checkNotNull(asset.getResourceSpec(), "Asset has no ResourceSpec.");
    String assetType = asset.getResourceSpec().getType();
    checkNotNull(assetType, "Asset has no type.");
    LOG.info("Resolved resource type: {}", assetType);

    String resourceName = asset.getResourceSpec().getName();
    checkNotNull(resourceName, "Asset has no resource name.");
    LOG.info("Resolved resource name: {}", resourceName);
    return asset;
  }

  @VisibleForTesting
  static void buildGcsPipeline(
      Pipeline pipeline,
      DataplexJdbcIngestionOptions options,
      DynamicDataSourceConfiguration dataSourceConfig,
      String targetRootPath) {

    // Auto inferring beam schema
    Schema beamSchema =
        Schemas.jdbcSchemaToBeamSchema(dataSourceConfig.buildDatasource(), options.getQuery());
    // Convert to Avro Schema
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);

    // Read from JdbcIO and convert ResultSet to Beam Row
    PCollection<Row> resultRows =
        pipeline.apply(
            "Read from JdbcIO",
            DynamicJdbcIO.<Row>read()
                .withDataSourceConfiguration(dataSourceConfig)
                .withQuery(options.getQuery())
                .withCoder(RowCoder.of(beamSchema))
                .withRowMapper(BeamSchemaUtil.of(beamSchema)));
    // Convert Beam Row to GenericRecord
    PCollection<GenericRecord> genericRecords =
        resultRows
            .apply("convert to GenericRecord", ParDo.of(new BeamRowToGenericRecordFn(avroSchema)))
            .setCoder(AvroCoder.of(avroSchema));
    // Write to GCS bucket
    PCollection<PartitionMetadata> metadata =
        genericRecords.apply(
            "Write to GCS",
            new GenericRecordsToGcsPartitioned(
                targetRootPath,
                Schemas.serialize(avroSchema),
                options.getParitionColumn(),
                options.getPartitioningScheme(),
                options.getFileFormat()));
  }

  @VisibleForTesting
  static void buildBigQueryPipeline(
      Pipeline pipeline,
      DataplexJdbcIngestionOptions options,
      DynamicDataSourceConfiguration dataSourceConfig) {
    pipeline
        .apply(
            "Read from JdbcIO",
            DynamicJdbcIO.<TableRow>read()
                .withDataSourceConfiguration(dataSourceConfig)
                .withQuery(options.getQuery())
                .withCoder(TableRowJsonCoder.of())
                .withRowMapper(JdbcConverters.getResultSetToTableRow()))
        .apply(
            "Write to BigQuery",
            BigQueryIO.writeTableRows()
                .withoutValidation()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(options.getWriteDisposition())
                .to(options.getOutputTable()));
  }

  static DynamicDataSourceConfiguration configDataSource(DataplexJdbcIngestionOptions options) {
    return DynamicJdbcIO.DynamicDataSourceConfiguration.create(
            options.getDriverClassName(),
            maybeDecrypt(options.getConnectionURL(), options.getKMSEncryptionKey()))
        .withUsername(maybeDecrypt(options.getUsername(), options.getKMSEncryptionKey()))
        .withPassword(maybeDecrypt(options.getPassword(), options.getKMSEncryptionKey()))
        .withDriverJars(options.getDriverJars())
        .withConnectionProperties(options.getConnectionProperties());
  }
}
