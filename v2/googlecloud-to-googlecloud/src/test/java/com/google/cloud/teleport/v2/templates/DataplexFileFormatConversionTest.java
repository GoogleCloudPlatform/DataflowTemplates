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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1AssetResourceSpec;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Schema;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1SchemaSchemaField;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1StorageFormat;
import com.google.cloud.teleport.v2.clients.DataplexClient;
import com.google.cloud.teleport.v2.templates.DataplexFileFormatConversion.EntityWithPartitions;
import com.google.cloud.teleport.v2.templates.DataplexFileFormatConversion.FileFormatConversionOptions;
import com.google.cloud.teleport.v2.templates.DataplexFileFormatConversion.InputFileFormat;
import com.google.cloud.teleport.v2.transforms.AvroConverters;
import com.google.cloud.teleport.v2.transforms.ParquetConverters;
import com.google.cloud.teleport.v2.utils.DataplexUtils;
import com.google.cloud.teleport.v2.utils.FileFormat.FileFormatOptions;
import com.google.cloud.teleport.v2.utils.WriteDisposition.WriteDispositionOptions;
import com.google.cloud.teleport.v2.values.DataplexCompression;
import com.google.cloud.teleport.v2.values.DataplexEnums;
import com.google.cloud.teleport.v2.values.DataplexEnums.CompressionFormat;
import com.google.cloud.teleport.v2.values.DataplexEnums.DataplexAssetResourceSpec;
import com.google.cloud.teleport.v2.values.DataplexEnums.EntityType;
import com.google.cloud.teleport.v2.values.DataplexEnums.StorageFormat;
import com.google.cloud.teleport.v2.values.DataplexEnums.StorageSystem;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/** Test cases for the {@link DataplexFileFormatConversion} class. */
public class DataplexFileFormatConversionTest {

  private static final String RESOURCES_DIR = "DataplexFileFormatConversionTest";

  private static final GoogleCloudDataplexV1Schema SCHEMA =
      new GoogleCloudDataplexV1Schema()
          .setFields(
              // Don't use ImmutableList here as it can't be cloned.
              Arrays.asList(
                  new GoogleCloudDataplexV1SchemaSchemaField()
                      .setName("Word")
                      .setType("STRING")
                      .setMode("REQUIRED"),
                  new GoogleCloudDataplexV1SchemaSchemaField()
                      .setName("Number")
                      .setType("INT32")
                      .setMode("REQUIRED")));
  private static final String EXPECT_SERIALIZED_AVRO_SCHEMA =
      "{"
          + "\"name\": \"Schema\","
          + "\"type\": \"record\","
          + "\"fields\": ["
          + "   {\"name\": \"Word\", \"type\": \"string\"},"
          + "   {\"name\": \"Number\", \"type\": \"int\"}"
          + "]"
          + "}";
  private static final Schema EXPECTED_AVRO_SCHEMA =
      new Parser().parse(EXPECT_SERIALIZED_AVRO_SCHEMA);
  private static final ImmutableList<GenericRecord> EXPECTED_GENERIC_RECORDS =
      expectedGenericRecords();

  private static final GoogleCloudDataplexV1Entity entity1 =
      new GoogleCloudDataplexV1Entity()
          .setName("projects/p1/locations/l1/lakes/l1/zones/z1/entities/e1")
          .setId("e1")
          .setSystem(DataplexEnums.StorageSystem.CLOUD_STORAGE.name())
          .setFormat(new GoogleCloudDataplexV1StorageFormat().setFormat(InputFileFormat.CSV.name()))
          .setSchema(SCHEMA);

  private static final GoogleCloudDataplexV1Partition partition11 =
      new GoogleCloudDataplexV1Partition()
          .setName("projects/p1/locations/l1/lakes/l1/zones/z1/entities/e1/partitions/p11")
          .setLocation(resourcePath("/entity1/partition11"));

  private static final GoogleCloudDataplexV1Partition partition12 =
      new GoogleCloudDataplexV1Partition()
          .setName("projects/p1/locations/l1/lakes/l1/zones/z1/entities/e1/partitions/p12")
          .setLocation(resourcePath("/entity1/partition12"));

  private static final GoogleCloudDataplexV1Asset asset2 =
      new GoogleCloudDataplexV1Asset()
          .setName("projects/p1/locations/l1/lakes/l1/zones/z1/assets/a2");
  private static final GoogleCloudDataplexV1Entity entity2 =
      new GoogleCloudDataplexV1Entity()
          .setName("projects/p1/locations/l1/lakes/l1/zones/z1/entities/e2")
          .setId("e2")
          .setAsset(asset2.getName())
          .setSystem(DataplexEnums.StorageSystem.CLOUD_STORAGE.name())
          .setFormat(
              new GoogleCloudDataplexV1StorageFormat().setFormat(InputFileFormat.JSON.name()))
          .setDataPath(resourcePath("/entity2"))
          .setSchema(SCHEMA);

  private static final GoogleCloudDataplexV1Entity entity3 =
      new GoogleCloudDataplexV1Entity()
          .setName("projects/p1/locations/l1/lakes/l1/zones/z1/entities/e3")
          .setId("e3")
          .setSystem(DataplexEnums.StorageSystem.CLOUD_STORAGE.name())
          .setFormat(
              new GoogleCloudDataplexV1StorageFormat().setFormat(InputFileFormat.AVRO.name()))
          .setDataPath(resourcePath("/entity3"))
          .setSchema(SCHEMA);

  private static final GoogleCloudDataplexV1Entity entity4 =
      new GoogleCloudDataplexV1Entity()
          .setName("projects/p1/locations/l1/lakes/l1/zones/z1/entities/e4")
          .setId("e4")
          .setSystem(DataplexEnums.StorageSystem.CLOUD_STORAGE.name())
          .setFormat(
              new GoogleCloudDataplexV1StorageFormat().setFormat(InputFileFormat.PARQUET.name()))
          .setDataPath(resourcePath("/entity4"))
          .setSchema(SCHEMA);

  @Rule public final transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule public final transient TestPipeline mainPipeline = TestPipeline.create();
  @Rule public final transient TestPipeline readPipeline = TestPipeline.create();

  private GoogleCloudDataplexV1Asset outputAsset;

  @Before
  public void setup() {
    final String tempDir = temporaryFolder.getRoot().getAbsolutePath();
    outputAsset =
        new GoogleCloudDataplexV1Asset()
            .setName("projects/p1/locations/l1/lakes/l1/zones/z1/assets/aout");
    outputAsset.setResourceSpec(
        new GoogleCloudDataplexV1AssetResourceSpec()
            .setType(DataplexAssetResourceSpec.STORAGE_BUCKET.name())
            .setName(tempDir));
  }

  /** Tests CSV to Avro conversion for an entity with partitions. */
  @Test
  @Category(NeedsRunner.class)
  public void testE2E_entityWithPartitionsCsvToAvro() throws IOException {
    DataplexClient dataplex = mock(DataplexClient.class);
    when(dataplex.getEntities(ImmutableList.of(entity1.getName())))
        .thenReturn(ImmutableList.of(entity1));
    when(dataplex.getPartitions(entity1.getName()))
        .thenReturn(ImmutableList.of(partition11, partition12));
    when(dataplex.getAsset(outputAsset.getName())).thenReturn(outputAsset);

    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);
    options.setInputAssetOrEntitiesList(entity1.getName());
    options.setOutputFileFormat(FileFormatOptions.AVRO);
    options.setOutputAsset(outputAsset.getName());

    DataplexFileFormatConversion.run(
        mainPipeline, options, dataplex, DataplexFileFormatConversionTest::outputPathProvider);

    PCollection<GenericRecord> readAvroFile =
        readPipeline.apply(
            "ReadAvroFile",
            AvroConverters.ReadAvroFile.newBuilder()
                .withInputFileSpec(temporaryFolder.getRoot().getAbsolutePath() + "/**/*.avro")
                .withSerializedSchema(EXPECT_SERIALIZED_AVRO_SCHEMA)
                .build());

    PAssert.that(readAvroFile).containsInAnyOrder(EXPECTED_GENERIC_RECORDS);

    readPipeline.run();
  }

  /** Tests JSON to Parquet conversion for an asset with entity using non-default compression. */
  @Test
  @Category(NeedsRunner.class)
  public void testE2E_assetWithEntityJsonToGzippedParquet() throws IOException {
    DataplexClient dataplex = mock(DataplexClient.class);
    when(dataplex.getCloudStorageEntities(asset2.getName())).thenReturn(ImmutableList.of(entity2));
    when(dataplex.getPartitions(entity2.getName())).thenReturn(ImmutableList.of());
    when(dataplex.getAsset(outputAsset.getName())).thenReturn(outputAsset);

    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);
    options.setInputAssetOrEntitiesList(asset2.getName());
    options.setOutputFileFormat(FileFormatOptions.PARQUET);
    options.setOutputAsset(outputAsset.getName());
    options.setOutputFileCompression(DataplexCompression.GZIP);

    DataplexFileFormatConversion.run(
        mainPipeline, options, dataplex, DataplexFileFormatConversionTest::outputPathProvider);

    PCollection<GenericRecord> readParquetFile =
        readPipeline.apply(
            "ReadParquetFile",
            ParquetConverters.ReadParquetFile.newBuilder()
                .withInputFileSpec(temporaryFolder.getRoot().getAbsolutePath() + "/**/*.parquet")
                .withSerializedSchema(EXPECT_SERIALIZED_AVRO_SCHEMA)
                .build());

    PAssert.that(readParquetFile).containsInAnyOrder(EXPECTED_GENERIC_RECORDS);

    readPipeline.run();
  }

  /** Tests Avro to Parquet conversion for an asset with entity. */
  @Test
  @Category(NeedsRunner.class)
  public void testE2E_assetWithEntityAvroToParquet() throws IOException {
    DataplexClient dataplex = mock(DataplexClient.class);
    when(dataplex.getEntities(ImmutableList.of(entity3.getName())))
        .thenReturn(ImmutableList.of(entity3));
    when(dataplex.getPartitions(entity3.getName())).thenReturn(ImmutableList.of());
    when(dataplex.getAsset(outputAsset.getName())).thenReturn(outputAsset);

    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);
    options.setInputAssetOrEntitiesList(entity3.getName());
    options.setOutputFileFormat(FileFormatOptions.PARQUET);
    options.setOutputAsset(outputAsset.getName());

    DataplexFileFormatConversion.run(
        mainPipeline, options, dataplex, DataplexFileFormatConversionTest::outputPathProvider);

    PCollection<GenericRecord> readParquetFile =
        readPipeline.apply(
            "ReadParquetFile",
            ParquetConverters.ReadParquetFile.newBuilder()
                .withInputFileSpec(temporaryFolder.getRoot().getAbsolutePath() + "/**/*.parquet")
                .withSerializedSchema(EXPECT_SERIALIZED_AVRO_SCHEMA)
                .build());

    PAssert.that(readParquetFile).containsInAnyOrder(EXPECTED_GENERIC_RECORDS);

    readPipeline.run();
  }

  /** Tests Parquet to Avro conversion for an asset with entity. */
  @Test
  @Category(NeedsRunner.class)
  public void testE2E_assetWithEntityParquetToAvro() throws IOException {
    DataplexClient dataplex = mock(DataplexClient.class);
    when(dataplex.getEntities(ImmutableList.of(entity4.getName())))
        .thenReturn(ImmutableList.of(entity4));
    when(dataplex.getPartitions(entity4.getName())).thenReturn(ImmutableList.of());
    when(dataplex.getAsset(outputAsset.getName())).thenReturn(outputAsset);

    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);
    options.setInputAssetOrEntitiesList(entity4.getName());
    options.setOutputFileFormat(FileFormatOptions.AVRO);
    options.setOutputAsset(outputAsset.getName());

    DataplexFileFormatConversion.run(
        mainPipeline, options, dataplex, DataplexFileFormatConversionTest::outputPathProvider);

    PCollection<GenericRecord> readParquetFile =
        readPipeline.apply(
            "ReadAvroFile",
            AvroConverters.ReadAvroFile.newBuilder()
                .withInputFileSpec(temporaryFolder.getRoot().getAbsolutePath() + "/**/*.avro")
                .withSerializedSchema(EXPECT_SERIALIZED_AVRO_SCHEMA)
                .build());

    PAssert.that(readParquetFile).containsInAnyOrder(EXPECTED_GENERIC_RECORDS);

    readPipeline.run();
  }

  /**
   * Tests JSON to Parquet conversion for an asset with entity when one of the files already exists
   * and the existing file behaviour is SKIP.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testE2E_assetWithEntityJsonToParquetSkipExistingFiles() throws IOException {
    // setup Dataplex client to return entity 2
    DataplexClient dataplex = mock(DataplexClient.class);
    when(dataplex.getCloudStorageEntities(asset2.getName())).thenReturn(ImmutableList.of(entity2));
    when(dataplex.getPartitions(entity2.getName())).thenReturn(ImmutableList.of());
    when(dataplex.getAsset(outputAsset.getName())).thenReturn(outputAsset);

    // setup options to skip existing files
    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);
    options.setInputAssetOrEntitiesList(asset2.getName());
    options.setOutputFileFormat(FileFormatOptions.PARQUET);
    options.setOutputAsset(outputAsset.getName());
    options.setWriteDisposition(WriteDispositionOptions.SKIP);

    // simulate the 1.json -> 1.parquet conversion already happened
    copyFileToOutputBucket("entity2.existing/1.parquet", "entity2/1.parquet");

    // run the pipeline, only  2.json -> 2.parquet conversion should happen
    DataplexFileFormatConversion.run(
        mainPipeline, options, dataplex, DataplexFileFormatConversionTest::outputPathProvider);

    // read the conversion results
    PCollection<GenericRecord> readParquetFile =
        readPipeline.apply(
            "ReadParquetFile",
            ParquetConverters.ReadParquetFile.newBuilder()
                .withInputFileSpec(temporaryFolder.getRoot().getAbsolutePath() + "/**/*.parquet")
                .withSerializedSchema(EXPECT_SERIALIZED_AVRO_SCHEMA)
                .build());

    // expect old 1.parquet (from entity2.existing) and newly converted 2.parquet (from entity2)
    ImmutableList.Builder<GenericRecord> expected = ImmutableList.builder();
    Record record = new Record(EXPECTED_AVRO_SCHEMA);
    record.put("Word", "abc.existing");
    record.put("Number", 1);
    expected.add(record);
    record = new Record(EXPECTED_AVRO_SCHEMA);
    record.put("Word", "def");
    record.put("Number", 2);
    expected.add(record);
    record = new Record(EXPECTED_AVRO_SCHEMA);
    record.put("Word", "ghi");
    record.put("Number", 3);
    expected.add(record);

    PAssert.that(readParquetFile).containsInAnyOrder(expected.build());

    readPipeline.run();
  }

  /**
   * Tests JSON to Parquet conversion for an asset with entity when one of the files already exists
   * and the existing file behaviour is FAIL.
   */
  @Test(expected = RuntimeException.class)
  @Category(NeedsRunner.class)
  public void testE2E_assetWithEntityJsonToParquetFailOnExistingFiles() throws IOException {
    // setup Dataplex client to return entity 2
    DataplexClient dataplex = mock(DataplexClient.class);
    when(dataplex.getCloudStorageEntities(asset2.getName())).thenReturn(ImmutableList.of(entity2));
    when(dataplex.getPartitions(entity2.getName())).thenReturn(ImmutableList.of());
    when(dataplex.getAsset(outputAsset.getName())).thenReturn(outputAsset);

    // setup options to fail on existing files
    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);
    options.setInputAssetOrEntitiesList(asset2.getName());
    options.setOutputFileFormat(FileFormatOptions.PARQUET);
    options.setOutputAsset(outputAsset.getName());
    options.setWriteDisposition(WriteDispositionOptions.FAIL);

    // simulate the 1.json -> 1.parquet conversion already happened
    copyFileToOutputBucket("entity2.existing/1.parquet", "entity2/1.parquet");
    // simulate the 2.json -> 2.parquet conversion already happened
    copyFileToOutputBucket("entity2.existing/1.parquet", "entity2/2.parquet");

    // run the pipeline, the job should fail because 1.parquet already exists
    DataplexFileFormatConversion.run(
            mainPipeline, options, dataplex, DataplexFileFormatConversionTest::outputPathProvider)
        .waitUntilFinish();
  }

  @Test
  public void testE2E_metadataUpdatedIfParamEnabled() throws IOException {
    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);
    options.setUpdateDataplexMetadata(true);
    options.setInputAssetOrEntitiesList(asset2.getName());
    options.setOutputFileFormat(FileFormatOptions.PARQUET);
    options.setOutputAsset(outputAsset.getName());

    DataplexClient dataplex = mock(DataplexClient.class);
    when(dataplex.getCloudStorageEntities(asset2.getName())).thenReturn(ImmutableList.of(entity2));
    when(dataplex.getPartitions(entity2.getName())).thenReturn(ImmutableList.of());
    when(dataplex.getAsset(outputAsset.getName())).thenReturn(outputAsset);
    when(dataplex.listEntities(any(), any())).thenReturn(ImmutableList.of(entity2));
    when(dataplex.createEntity(any(), any()))
        .thenAnswer(
            invocation -> {
              GoogleCloudDataplexV1Entity entity = invocation.getArgument(1);
              return entity.clone().setName("generated_name-" + entity.getId());
            });

    DataplexFileFormatConversion.run(
            mainPipeline, options, dataplex, DataplexFileFormatConversionTest::outputPathProvider)
        .waitUntilFinish();

    String expectedZone = DataplexUtils.getZoneFromAsset(outputAsset.getName());
    String expectedAsset = DataplexUtils.getShortAssetNameFromAsset(outputAsset.getName());
    verify(dataplex, times(1))
        .createEntity(
            eq(expectedZone),
            argThat(
                e ->
                    // Comparing the whole entity using equals() is too tricky (e.g. dataPath
                    // will be under the temp directory). Check only the main fields:
                    e.getId().equals(entity2.getId())
                        && e.getAsset().equals(expectedAsset)
                        && e.getSchema().equals(entity2.getSchema().clone().setUserManaged(true))));
  }

  @Test
  public void testE2E_metadataNotUpdatedIfParamDisabled() throws IOException {
    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);
    options.setUpdateDataplexMetadata(false);
    options.setInputAssetOrEntitiesList(asset2.getName());
    options.setOutputFileFormat(FileFormatOptions.PARQUET);
    options.setOutputAsset(outputAsset.getName());

    DataplexClient dataplex = mock(DataplexClient.class);
    when(dataplex.getCloudStorageEntities(asset2.getName())).thenReturn(ImmutableList.of(entity2));
    when(dataplex.getPartitions(entity2.getName())).thenReturn(ImmutableList.of());
    when(dataplex.getAsset(outputAsset.getName())).thenReturn(outputAsset);
    when(dataplex.listEntities(any(), any())).thenReturn(ImmutableList.of(entity2));
    when(dataplex.createEntity(any(), any()))
        .thenAnswer(
            invocation -> {
              GoogleCloudDataplexV1Entity entity = invocation.getArgument(1);
              return entity.clone().setName("generated_name-" + entity.getId());
            });

    DataplexFileFormatConversion.run(
            mainPipeline, options, dataplex, DataplexFileFormatConversionTest::outputPathProvider)
        .waitUntilFinish();

    verify(dataplex, never()).updateEntity(any());
    verify(dataplex, never()).createEntity(any(), any());
    verify(dataplex, never()).createOrUpdatePartition(any(), any());
  }

  @Test
  public void test_updateDataplexMetadata_createsEntitiesAndPartitions() throws IOException {
    // Arrange

    DataplexClient dataplex = mock(DataplexClient.class);
    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);
    options.setInputAssetOrEntitiesList(asset2.getName());
    options.setOutputFileFormat(FileFormatOptions.PARQUET);
    options.setOutputAsset(outputAsset.getName());
    options.setWriteDisposition(WriteDispositionOptions.OVERWRITE);
    options.setUpdateDataplexMetadata(true);

    String outputZoneName = DataplexUtils.getZoneFromAsset(outputAsset.getName());
    String shortOutputAssetName = DataplexUtils.getShortAssetNameFromAsset(outputAsset.getName());

    // Scenario:
    //   - Source entities 1, 2, 3 are being converted.
    //   - Output entities corresponding to source entities 1 and 2 don't exist (should be created).
    //   - Output entity corresponding to source entities 3 already exists (shouldn't be created,
    // but should have the schema updated).
    //   - Entity 1 is partitioned, 2 is unpartitioned.
    //   - Output asset also contains entity "should_not_be_used", which shouldn't be used.
    GoogleCloudDataplexV1Schema oldSchema =
        new GoogleCloudDataplexV1Schema()
            .setUserManaged(true)
            .setFields(
                Arrays.asList(
                    dataplexField("ts", "TIMESTAMP", "NULLABLE"),
                    dataplexField("s1", "STRING", "NULLABLE")));
    GoogleCloudDataplexV1Schema newSchema =
        new GoogleCloudDataplexV1Schema()
            .setUserManaged(true)
            .setFields(
                Arrays.asList(
                    dataplexField("ts", "TIMESTAMP", "NULLABLE"),
                    dataplexField("s1", "STRING", "NULLABLE"),
                    dataplexField("d1", "DATE", "NULLABLE")));
    GoogleCloudDataplexV1Entity sourceEntity1 =
        defaultDataplexEntity()
            .setName("partitioned_table_entity")
            .setId("partitioned_table")
            .setDataPath("gs://source_bucket/partitioned_table")
            .setSchema(newSchema.clone());
    GoogleCloudDataplexV1Partition p1 =
        dataplexPartition("20220101")
            .setLocation("gs://source_bucket/partitioned_table/date=20220101");
    GoogleCloudDataplexV1Partition p2 =
        dataplexPartition("20220202")
            .setLocation("gs://source_bucket/partitioned_table/date=20220202");
    GoogleCloudDataplexV1Entity sourceEntity2 =
        defaultDataplexEntity()
            .setName("unpartitioned_table_entity")
            .setId("unpartitioned_table")
            .setDataPath("gs://source_bucket/unpartitioned_table")
            .setSchema(newSchema.clone());
    GoogleCloudDataplexV1Entity sourceEntity3 =
        defaultDataplexEntity()
            .setName("existing_table_entity")
            .setId("existing_table")
            .setDataPath("gs://source_bucket/existing_table")
            .setSchema(newSchema.clone());
    GoogleCloudDataplexV1Entity existingOutputEntity1 =
        defaultDataplexEntity()
            .setAsset(DataplexUtils.getShortAssetNameFromAsset(outputAsset.getName()))
            .setName("existing_table_entity_5")
            .setId("existing_table_5")
            .setDataPath("gs://target_bucket/existing_table")
            .setSchema(oldSchema.clone());
    // This is an extra entity returned by Mock Dataplex with a different dataPath,
    // to verify only entities with matching dataPath are picked up:
    GoogleCloudDataplexV1Entity unusedEntity =
        defaultDataplexEntity()
            .setAsset(DataplexUtils.getShortAssetNameFromAsset(outputAsset.getName()))
            .setName("should_not_be_used")
            .setId("partitioned_table")
            .setDataPath("gs://wrong_bucket/partitioned_table");
    when(dataplex.listEntities(eq(DataplexUtils.getZoneFromAsset(asset2.getName())), any()))
        .thenAnswer(
            invocation -> {
              String filter = invocation.getArgument(1);
              if (filter.equals("asset=" + shortOutputAssetName)) {
                // listEntities should return entities without schema,
                // only getEntity returns entities with schemas.
                return ImmutableList.of(
                    existingOutputEntity1.clone().setSchema(null),
                    unusedEntity.clone().setSchema(null));
              } else {
                return ImmutableList.of();
              }
            });
    // Template should only load the existing entity to verify it's user-managed:
    when(dataplex.getEntity("existing_table_entity_5"))
        .thenReturn(existingOutputEntity1.clone().setEtag("etag123"));
    when(dataplex.createEntity(any(), any()))
        .thenAnswer(
            invocation -> {
              GoogleCloudDataplexV1Entity entity = invocation.getArgument(1);
              return entity.clone().setName("generated_name-" + entity.getId());
            });
    when(dataplex.createOrUpdatePartition(any(), any()))
        .thenAnswer(
            invocation -> {
              GoogleCloudDataplexV1Partition p = invocation.getArgument(1);
              return p.clone().setName("generated_name-" + p.getValues());
            });

    // Act

    DataplexFileFormatConversion.updateDataplexMetadata(
        dataplex,
        options,
        Arrays.asList(
            new EntityWithPartitions(sourceEntity1, Arrays.asList(p1, p2)),
            new EntityWithPartitions(sourceEntity2),
            new EntityWithPartitions(sourceEntity3)),
        DataplexFileFormatConversionTest::gcsOutputPathProvider,
        "target_bucket");

    // Assert

    GoogleCloudDataplexV1Entity expectedNewEntity1 =
        defaultDataplexEntity()
            .setAsset(shortOutputAssetName)
            .setId("partitioned_table")
            .setDataPath("gs://target_bucket/partitioned_table")
            .setSchema(newSchema.clone());
    GoogleCloudDataplexV1Partition expectedMergedPartition1 =
        dataplexPartition("20220101")
            .setLocation("gs://target_bucket/partitioned_table/date=20220101");
    GoogleCloudDataplexV1Partition expectedMergedPartition2 =
        dataplexPartition("20220202")
            .setLocation("gs://target_bucket/partitioned_table/date=20220202");
    GoogleCloudDataplexV1Entity expectedNewEntity2 =
        defaultDataplexEntity()
            .setAsset(shortOutputAssetName)
            .setId("unpartitioned_table")
            .setDataPath("gs://target_bucket/unpartitioned_table")
            .setSchema(newSchema.clone());
    GoogleCloudDataplexV1Entity expectedUpdatedEntity3 =
        defaultDataplexEntity()
            .setAsset(shortOutputAssetName)
            .setId("existing_table_5") // Note that the existing id_5 should've been reused.
            .setName("existing_table_entity_5")
            .setDataPath("gs://target_bucket/existing_table")
            .setSchema(newSchema.clone())
            .setEtag("etag123"); // The latest etag must be provided on update.

    verify(dataplex, times(1)).createEntity(outputZoneName, expectedNewEntity1);
    verify(dataplex, times(1)).createEntity(outputZoneName, expectedNewEntity2);
    verify(dataplex, times(1)).updateEntity(expectedUpdatedEntity3);
    verify(dataplex, times(1))
        .createOrUpdatePartition("generated_name-partitioned_table", expectedMergedPartition1);
    verify(dataplex, times(1))
        .createOrUpdatePartition("generated_name-partitioned_table", expectedMergedPartition2);
    verify(dataplex, atLeastOnce()).listEntities(any(), any());
    // Make sure no other entities were created or updated:
    verify(dataplex, times(2)).createEntity(any(), any());
    verify(dataplex, times(1)).updateEntity(any());
  }

  private void copyFileToOutputBucket(String sourceRelativePath, String destinationRelativePath)
      throws IOException {
    Path source =
        Paths.get(Resources.getResource(RESOURCES_DIR + '/' + sourceRelativePath).getPath());
    Path destination =
        Paths.get(temporaryFolder.getRoot().getAbsolutePath() + '/' + destinationRelativePath);
    Files.createDirectories(destination.getParent());
    Files.copy(source, destination);
  }

  private static ImmutableList<GenericRecord> expectedGenericRecords() {
    ImmutableList.Builder<GenericRecord> expected = ImmutableList.builder();
    Record record = new Record(EXPECTED_AVRO_SCHEMA);
    record.put("Word", "abc");
    record.put("Number", 1);
    expected.add(record);
    record = new Record(EXPECTED_AVRO_SCHEMA);
    record.put("Word", "def");
    record.put("Number", 2);
    expected.add(record);
    record = new Record(EXPECTED_AVRO_SCHEMA);
    record.put("Word", "ghi");
    record.put("Number", 3);
    expected.add(record);
    return expected.build();
  }

  private static String outputPathProvider(String inputPath, String outputBucket) {
    String relativeInputPath;
    try {
      relativeInputPath =
          Resources.getResource(RESOURCES_DIR)
              .toURI()
              .relativize(new File(inputPath).toURI())
              .getPath();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return outputBucket + '/' + relativeInputPath;
  }

  private static String gcsOutputPathProvider(String inputPath, String outputBucket) {
    return String.format("gs://%s/%s", outputBucket, GcsPath.fromUri(inputPath).getObject());
  }

  private static GoogleCloudDataplexV1Entity defaultDataplexEntity() {
    return new GoogleCloudDataplexV1Entity()
        .setAsset(DataplexUtils.getShortAssetNameFromAsset(asset2.getName()))
        .setType(EntityType.TABLE.name())
        .setSystem(StorageSystem.CLOUD_STORAGE.name())
        .setSchema(new GoogleCloudDataplexV1Schema().setUserManaged(true))
        .setFormat(
            new GoogleCloudDataplexV1StorageFormat()
                .setMimeType(StorageFormat.PARQUET.getMimeType())
                .setCompressionFormat(CompressionFormat.COMPRESSION_FORMAT_UNSPECIFIED.name()));
  }

  private static GoogleCloudDataplexV1SchemaSchemaField dataplexField(
      String name, String type, String mode) {
    return new GoogleCloudDataplexV1SchemaSchemaField().setName(name).setType(type).setMode(mode);
  }

  private static GoogleCloudDataplexV1Partition dataplexPartition(String... value) {
    return new GoogleCloudDataplexV1Partition().setValues(Arrays.asList(value));
  }

  private static String resourcePath(String relativePath) {
    return Resources.getResource(RESOURCES_DIR + relativePath).getPath();
  }
}
