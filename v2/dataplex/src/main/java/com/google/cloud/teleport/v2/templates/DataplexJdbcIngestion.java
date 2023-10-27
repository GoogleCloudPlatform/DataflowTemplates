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

import static com.google.cloud.teleport.v2.utils.KMSUtils.maybeDecrypt;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Schema;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1SchemaPartitionField;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1StorageFormat;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.clients.DataplexClient;
import com.google.cloud.teleport.v2.clients.DataplexClientFactory;
import com.google.cloud.teleport.v2.clients.DefaultDataplexClient;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.DataplexJdbcIngestionOptions;
import com.google.cloud.teleport.v2.transforms.BeamRowToGenericRecordFn;
import com.google.cloud.teleport.v2.transforms.DataplexJdbcIngestionUpdateMetadata;
import com.google.cloud.teleport.v2.transforms.GenericRecordsToGcsPartitioned;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import com.google.cloud.teleport.v2.utils.DataplexJdbcIngestionFilter;
import com.google.cloud.teleport.v2.utils.DataplexJdbcIngestionNaming;
import com.google.cloud.teleport.v2.utils.DataplexUtils;
import com.google.cloud.teleport.v2.utils.GCSUtils;
import com.google.cloud.teleport.v2.utils.JdbcConverters;
import com.google.cloud.teleport.v2.utils.JdbcIngestionWriteDisposition.MapWriteDisposition;
import com.google.cloud.teleport.v2.utils.JdbcIngestionWriteDisposition.WriteDispositionException;
import com.google.cloud.teleport.v2.utils.JdbcIngestionWriteDisposition.WriteDispositionOptions;
import com.google.cloud.teleport.v2.utils.Schemas;
import com.google.cloud.teleport.v2.values.DataplexCompression;
import com.google.cloud.teleport.v2.values.DataplexEnums.DataplexAssetResourceSpec;
import com.google.cloud.teleport.v2.values.DataplexEnums.EntityType;
import com.google.cloud.teleport.v2.values.DataplexEnums.FieldType;
import com.google.cloud.teleport.v2.values.DataplexEnums.PartitionStyle;
import com.google.cloud.teleport.v2.values.DataplexEnums.StorageSystem;
import com.google.cloud.teleport.v2.values.DataplexPartitionMetadata;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DataplexJdbcIngestion} pipeline reads from Jdbc and exports results to a BigQuery
 * dataset or to Cloud Storage, registering metadata in Dataplex.
 *
 * <p>Accepts a Dataplex asset as the destination, which will be resolved to the corresponding
 * BigQuery dataset/Storage bucket via Dataplex API.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-googlecloud/README_Dataplex_JDBC_Ingestion.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Dataplex_JDBC_Ingestion",
    category = TemplateCategory.BATCH,
    displayName = "Dataplex JDBC Ingestion",
    description =
        "A pipeline that reads from a JDBC source and writes to to a Dataplex asset, which can be"
            + " either a BigQuery dataset or a Cloud Storage bucket. JDBC connection string, user"
            + " name and password can be passed in directly as plaintext or encrypted using the"
            + " Google Cloud KMS API.  If the parameter KMSEncryptionKey is specified,"
            + " connectionURL, username, and password should be all in encrypted format. A sample"
            + " curl command for the KMS API encrypt endpoint: curl -s -X POST"
            + " \"https://cloudkms.googleapis.com/v1/projects/your-project/locations/your-path/keyRings/your-keyring/cryptoKeys/your-key:encrypt\""
            + "  -d \"{\\\"plaintext\\\":\\\"PasteBase64EncodedString\\\"}\"  -H \"Authorization:"
            + " Bearer $(gcloud auth application-default print-access-token)\"  -H \"Content-Type:"
            + " application/json\"",
    optionsClass = DataplexJdbcIngestionOptions.class,
    flexContainerName = "dataplex-jdbc-ingestion",
    contactInformation = "https://cloud.google.com/support")
public class DataplexJdbcIngestion {

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(DataplexJdbcIngestion.class);
  private static final int MAX_CREATE_ENTITY_ATTEMPTS = 10;

  /** The tag for filtered records. */
  private static final TupleTag<GenericRecord> FILTERED_RECORDS_OUT =
      new TupleTag<GenericRecord>() {};

  /** The tag for existing target file names. */
  private static final TupleTag<String> EXISTING_TARGET_FILES_OUT = new TupleTag<String>() {};

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) throws IOException {
    UncaughtExceptionLogger.register();

    DataplexJdbcIngestionOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataplexJdbcIngestionOptions.class);

    BigQueryIOUtils.validateBQStorageApiOptionsBatch(options);

    Pipeline pipeline = Pipeline.create(options);

    DataplexClient dataplex = DefaultDataplexClient.withDefaultClient(options.getGcpCredential());
    String assetName = options.getOutputAsset();
    GoogleCloudDataplexV1Asset asset = resolveAsset(assetName, dataplex);

    DataSourceConfiguration dataSourceConfig = configDataSource(options);
    String assetType = asset.getResourceSpec().getType();
    if (DataplexAssetResourceSpec.BIGQUERY_DATASET.name().equals(assetType)) {
      buildBigQueryPipeline(pipeline, options, dataSourceConfig);
    } else if (DataplexAssetResourceSpec.STORAGE_BUCKET.name().equals(assetType)) {
      String targetRootPath =
          "gs://" + asset.getResourceSpec().getName() + "/" + options.getOutputTable();
      DataplexClientFactory dcf = DataplexClientFactory.defaultFactory(options.getGcpCredential());
      buildGcsPipeline(pipeline, options, dataSourceConfig, targetRootPath, dataplex, dcf);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Asset %s is of type %s. Only %s and %s supported.",
              assetName,
              assetType,
              DataplexAssetResourceSpec.BIGQUERY_DATASET.name(),
              DataplexAssetResourceSpec.STORAGE_BUCKET.name()));
    }

    pipeline.run();
  }

  /**
   * Resolves a Dataplex asset.
   *
   * @param assetName Asset name from which the Dataplex asset will be resolved.
   * @param dataplex Dataplex client to connect to Dataplex via asset name.
   * @return The resolved asset
   */
  private static GoogleCloudDataplexV1Asset resolveAsset(String assetName, DataplexClient dataplex)
      throws IOException {

    LOG.info("Resolving asset: {}", assetName);
    GoogleCloudDataplexV1Asset asset = dataplex.getAsset(assetName);
    checkNotNull(asset.getResourceSpec(), "Asset has no ResourceSpec.");
    String assetType = asset.getResourceSpec().getType();
    checkNotNull(assetType, "Asset has no type.");
    LOG.info("Resolved resource type: {}", assetType);

    String resourceName = asset.getResourceSpec().getName();
    checkNotNull(resourceName, "Asset has no resource name.");
    LOG.info("Resolved resource name: {}", resourceName);
    return asset;
  }

  private static boolean shouldSkipUnpartitionedTable(
      DataplexJdbcIngestionOptions options, String targetRootPath, List<String> existingFiles) {
    String expectedFilePath =
        new DataplexJdbcIngestionNaming(options.getFileFormat().getFileSuffix())
            .getSingleFilename();
    if (existingFiles.contains(expectedFilePath)) {
      // Target file exists, performing writeDispositionGcs strategy
      switch (options.getWriteDisposition()) {
        case WRITE_EMPTY:
          throw new WriteDispositionException(
              String.format(
                  "Target File %s already exists in the output asset bucket %s. Failing"
                      + " according to writeDisposition.",
                  expectedFilePath, targetRootPath));
        case SKIP:
          LOG.info(
              "Target File {} already exists in the output asset bucket {}. Skipping"
                  + " according to writeDisposition.",
              expectedFilePath,
              targetRootPath);
          return true;
        case WRITE_TRUNCATE:
          LOG.info(
              "Target File {} already exists in the output asset bucket {}. Overwriting"
                  + " according to writeDisposition.",
              expectedFilePath,
              targetRootPath);
          return false;
        default:
          throw new UnsupportedOperationException(
              options.getWriteDisposition()
                  + " writeDisposition not implemented for writing to GCS.");
      }
    }
    return false;
  }

  private static PCollection<GenericRecord> applyPartitionedWriteDispositionFilter(
      PCollection<GenericRecord> genericRecords,
      DataplexJdbcIngestionOptions options,
      String targetRootPath,
      org.apache.avro.Schema avroSchema,
      List<String> existingFiles) {

    WriteDispositionOptions writeDisposition = options.getWriteDisposition();

    PCollectionTuple filteredRecordsTuple =
        genericRecords.apply(
            "Filter pre-existing records",
            new DataplexJdbcIngestionFilter(
                targetRootPath,
                Schemas.serialize(avroSchema),
                options.getParitionColumn(),
                options.getPartitioningScheme(),
                options.getFileFormat().getFileSuffix(),
                writeDisposition,
                existingFiles,
                FILTERED_RECORDS_OUT,
                EXISTING_TARGET_FILES_OUT));

    filteredRecordsTuple
        .get(EXISTING_TARGET_FILES_OUT)
        // Getting unique filenames
        .apply(Distinct.create())
        .apply(
            "Log existing target file names",
            ParDo.of(
                // This transform logs the distinct existing target files. Logging is done in
                // a separate transform to prevent redundant logs.
                // OutputT is String here since DoFn will not accept void. The resulting
                // PCollection will be empty.
                new DoFn<String, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    String filename = c.element();
                    LOG.info(
                        "Target File {} already exists in the output asset bucket {}. Performing "
                            + " {} writeDisposition strategy.",
                        filename,
                        targetRootPath,
                        writeDisposition);
                  }
                }));
    return filteredRecordsTuple.get(FILTERED_RECORDS_OUT);
  }

  @VisibleForTesting
  static void buildBigQueryPipeline(
      Pipeline pipeline,
      DataplexJdbcIngestionOptions options,
      DataSourceConfiguration dataSourceConfig) {

    if (options.getUpdateDataplexMetadata()) {
      LOG.warn("Dataplex metadata updates enabled, but not supported for BigQuery targets.");
    }

    pipeline
        .apply(
            "Read from JdbcIO",
            JdbcIO.<TableRow>read()
                .withDataSourceConfiguration(dataSourceConfig)
                .withQuery(options.getQuery())
                .withCoder(TableRowJsonCoder.of())
                .withRowMapper(JdbcConverters.getResultSetToTableRow(options.getUseColumnAlias())))
        .apply(
            "Write to BigQuery",
            BigQueryIO.writeTableRows()
                .withoutValidation()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                // Mapping DataplexJdbcIngestionWriteDisposition.WriteDispositionOptions to
                // BigqueryIO.Write.WriteDisposition
                .withWriteDisposition(
                    MapWriteDisposition.mapWriteDispostion(options.getWriteDisposition()))
                .to(options.getOutputTable()));
  }

  @VisibleForTesting
  static void buildGcsPipeline(
      Pipeline pipeline,
      DataplexJdbcIngestionOptions options,
      DataSourceConfiguration dataSourceConfig,
      String targetRootPath,
      DataplexClient dataplex,
      DataplexClientFactory dcf)
      throws IOException {
    List<String> existingFiles = GCSUtils.getFilesInDirectory(targetRootPath);
    // Auto inferring beam schema
    Schema beamSchema =
        Schemas.jdbcSchemaToBeamSchema(dataSourceConfig.buildDatasource(), options.getQuery());
    // Convert to Avro Schema
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);

    // Read from JdbcIO and convert ResultSet to Beam Row
    PCollection<Row> resultRows =
        pipeline.apply(
            "Read from JdbcIO",
            JdbcIO.readRows()
                .withDataSourceConfiguration(dataSourceConfig)
                .withQuery(options.getQuery()));
    // Convert Beam Row to GenericRecord
    PCollection<GenericRecord> genericRecords =
        resultRows
            .apply("convert to GenericRecord", ParDo.of(new BeamRowToGenericRecordFn(avroSchema)))
            .setCoder(AvroCoder.of(avroSchema));

    boolean hasPartitions =
        options.getParitionColumn() != null && options.getPartitioningScheme() != null;

    // If targetRootPath is changed in the following lines, please also change the root path for
    // existingFiles
    if (!hasPartitions) {
      if (shouldSkipUnpartitionedTable(options, targetRootPath, existingFiles)) {
        return;
      }
    } else {
      genericRecords =
          applyPartitionedWriteDispositionFilter(
              genericRecords, options, targetRootPath, avroSchema, existingFiles);
    }

    // Write to GCS bucket
    PCollection<DataplexPartitionMetadata> metadata =
        genericRecords.apply(
            "Write to GCS",
            new GenericRecordsToGcsPartitioned(
                targetRootPath,
                Schemas.serialize(avroSchema),
                options.getParitionColumn(),
                options.getPartitioningScheme(),
                options.getFileFormat()));

    // Update Dataplex Metadata, if enabled
    if (options.getUpdateDataplexMetadata()) {
      GoogleCloudDataplexV1Entity outputEntity =
          loadDataplexMetadata(dataplex, options, avroSchema, targetRootPath, hasPartitions);

      // All entities should've been pre-created and updated with the latest schema in
      // loadDataplexMetadata(). Only partitions need to be updated after the ingestion.
      // If partitioning scheme is not set, then nothing to update.
      if (hasPartitions) {
        metadata.apply(
            "Update Metadata",
            new DataplexJdbcIngestionUpdateMetadata(dcf, outputEntity.getName()));
      }
    }
  }

  @VisibleForTesting
  static GoogleCloudDataplexV1Entity loadDataplexMetadata(
      DataplexClient dataplex,
      DataplexJdbcIngestionOptions options,
      org.apache.avro.Schema avroSchema,
      String targetRootPath,
      boolean hasPartitions)
      throws IOException {

    GoogleCloudDataplexV1Schema schema = DataplexUtils.toDataplexSchema(avroSchema, null);
    schema.setUserManaged(true);
    if (hasPartitions) {
      schema
          .setPartitionStyle(PartitionStyle.HIVE_COMPATIBLE.name())
          .setPartitionFields(
              options.getPartitioningScheme().getKeyNames().stream()
                  .map(
                      key ->
                          new GoogleCloudDataplexV1SchemaPartitionField()
                              .setName(key)
                              // Dataplex supports only STRING partition keys for GCS files.
                              .setType(FieldType.STRING.name()))
                  .collect(Collectors.toList()));
    }

    GoogleCloudDataplexV1Entity outputEntity =
        DataplexUtils.getEntityByDataPath(dataplex, options.getOutputAsset(), targetRootPath);
    if (outputEntity == null) {
      return createNewOutputEntity(dataplex, options, schema, targetRootPath);
    }
    // Reload the full entity once again as the listEntities API never returns entity schema.
    GoogleCloudDataplexV1Entity richOutputEntity = loadEntity(dataplex, outputEntity.getName());
    DataplexUtils.verifyEntityIsUserManaged(richOutputEntity);
    return updateEntitySchema(dataplex, options, schema, richOutputEntity);
  }

  private static GoogleCloudDataplexV1Entity loadEntity(DataplexClient dataplex, String entityName)
      throws IOException {
    GoogleCloudDataplexV1Entity richEntity = dataplex.getEntity(entityName);
    checkNotNull(richEntity, String.format("Could not load entity %s", entityName));
    return richEntity;
  }

  private static GoogleCloudDataplexV1Entity createNewOutputEntity(
      DataplexClient dataplex,
      DataplexJdbcIngestionOptions options,
      GoogleCloudDataplexV1Schema schema,
      String targetRootPath)
      throws IOException {

    String assetName = options.getOutputAsset();
    String zoneName = DataplexUtils.getZoneFromAsset(assetName);

    GoogleCloudDataplexV1Entity entity =
        DataplexUtils.createEntityWithUniqueId(
            dataplex,
            zoneName,
            new GoogleCloudDataplexV1Entity()
                .setId(options.getOutputTable())
                .setAsset(DataplexUtils.getShortAssetNameFromAsset(assetName))
                .setDataPath(targetRootPath)
                .setType(EntityType.TABLE.name())
                .setSystem(StorageSystem.CLOUD_STORAGE.name())
                .setSchema(schema)
                .setFormat(storageFormat(options)),
            MAX_CREATE_ENTITY_ATTEMPTS);
    if (entity.getName() == null || entity.getName().isEmpty()) {
      throw new IOException("Dataplex returned an entity with no name: " + entity);
    }
    LOG.info(
        "Created a new entity for data path {} in zone {}: {}",
        targetRootPath,
        zoneName,
        entity.getName());
    return entity;
  }

  private static GoogleCloudDataplexV1Entity updateEntitySchema(
      DataplexClient dataplex,
      DataplexJdbcIngestionOptions options,
      GoogleCloudDataplexV1Schema schema,
      GoogleCloudDataplexV1Entity richOutputEntity)
      throws IOException {

    GoogleCloudDataplexV1Entity entity =
        richOutputEntity
            .clone()
            .setType(EntityType.TABLE.name())
            .setSystem(StorageSystem.CLOUD_STORAGE.name())
            .setSchema(schema)
            .setFormat(storageFormat(options));

    GoogleCloudDataplexV1Entity updatedEntity;
    try {
      updatedEntity = dataplex.updateEntity(entity);
    } catch (IOException e) {
      throw new IOException(String.format("Error updating entity: %s", entity.getName()), e);
    }

    LOG.info("Updated metadata for entity: {}", entity.getName());
    return updatedEntity;
  }

  private static GoogleCloudDataplexV1StorageFormat storageFormat(
      DataplexJdbcIngestionOptions options) {
    // Snappy is the default compression for both ParquetIO and AvroIO,
    // and we don't override it in this template.
    return DataplexUtils.storageFormat(options.getFileFormat(), DataplexCompression.SNAPPY);
  }

  static DataSourceConfiguration configDataSource(DataplexJdbcIngestionOptions options) {
    JdbcIO.DataSourceConfiguration dataSourceConfiguration =
        JdbcIO.DataSourceConfiguration.create(
                StaticValueProvider.of(options.getDriverClassName()),
                maybeDecrypt(options.getConnectionURL(), options.getKMSEncryptionKey()))
            .withUsername(maybeDecrypt(options.getUsername(), options.getKMSEncryptionKey()))
            .withPassword(maybeDecrypt(options.getPassword(), options.getKMSEncryptionKey()))
            .withDriverJars(options.getDriverJars());

    if (options.getConnectionProperties() != null) {
      dataSourceConfiguration =
          dataSourceConfiguration.withConnectionProperties(options.getConnectionProperties());
    }
    return dataSourceConfiguration;
  }
}
