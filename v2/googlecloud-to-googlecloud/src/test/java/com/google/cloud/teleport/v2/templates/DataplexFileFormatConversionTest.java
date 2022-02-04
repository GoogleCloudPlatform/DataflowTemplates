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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1AssetResourceSpec;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Schema;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1SchemaSchemaField;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1StorageFormat;
import com.google.cloud.teleport.v2.clients.DataplexClient;
import com.google.cloud.teleport.v2.templates.DataplexFileFormatConversion.FileFormatConversionOptions;
import com.google.cloud.teleport.v2.templates.DataplexFileFormatConversion.InputFileFormat;
import com.google.cloud.teleport.v2.templates.DataplexFileFormatConversion.OutputFileFormat;
import com.google.cloud.teleport.v2.transforms.AvroConverters;
import com.google.cloud.teleport.v2.transforms.ParquetConverters;
import com.google.cloud.teleport.v2.values.DataplexAssetResourceSpec;
import com.google.cloud.teleport.v2.values.DataplexCompression;
import com.google.cloud.teleport.v2.values.EntityMetadata.StorageSystem;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
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
              ImmutableList.of(
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
          .setName("projects/p1/locations/l1/lakes/l1}/zones/z1/entities/e1")
          .setSystem(StorageSystem.CLOUD_STORAGE.name())
          .setFormat(new GoogleCloudDataplexV1StorageFormat().setFormat(InputFileFormat.CSV.name()))
          .setSchema(SCHEMA);

  private static final GoogleCloudDataplexV1Partition partition11 =
      new GoogleCloudDataplexV1Partition()
          .setName("projects/p1/locations/l1/lakes/l1}/zones/z1/entities/e1/partitions/p11")
          .setLocation(Resources.getResource(RESOURCES_DIR + "/entity1/partition11").getPath());

  private static final GoogleCloudDataplexV1Partition partition12 =
      new GoogleCloudDataplexV1Partition()
          .setName("projects/p1/locations/l1/lakes/l1}/zones/z1/entities/e1/partitions/p12")
          .setLocation(Resources.getResource(RESOURCES_DIR + "/entity1/partition12").getPath());

  private static final GoogleCloudDataplexV1Asset asset2 =
      new GoogleCloudDataplexV1Asset()
          .setName("projects/p1/locations/l1/lakes/l1/zones/z1/assets/a2");
  private static final GoogleCloudDataplexV1Entity entity2 =
      new GoogleCloudDataplexV1Entity()
          .setName("projects/p1/locations/l1/lakes/l1}/zones/z1/entities/e2")
          .setAsset(asset2.getName())
          .setSystem(StorageSystem.CLOUD_STORAGE.name())
          .setFormat(
              new GoogleCloudDataplexV1StorageFormat().setFormat(InputFileFormat.JSON.name()))
          .setDataPath(Resources.getResource(RESOURCES_DIR + "/entity2").getPath())
          .setSchema(SCHEMA);

  private static final GoogleCloudDataplexV1Entity entity3 =
      new GoogleCloudDataplexV1Entity()
          .setName("projects/p1/locations/l1/lakes/l1/zones/z1/entities/e3")
          .setId("e3")
          .setSystem(StorageSystem.CLOUD_STORAGE.name())
          .setFormat(
              new GoogleCloudDataplexV1StorageFormat().setFormat(InputFileFormat.AVRO.name()))
          .setDataPath(Resources.getResource(RESOURCES_DIR + "/entity3").getPath())
          .setSchema(SCHEMA);

  private static final GoogleCloudDataplexV1Entity entity4 =
      new GoogleCloudDataplexV1Entity()
          .setName("projects/p1/locations/l1/lakes/l1/zones/z1/entities/e4")
          .setId("e4")
          .setSystem(StorageSystem.CLOUD_STORAGE.name())
          .setFormat(
              new GoogleCloudDataplexV1StorageFormat().setFormat(InputFileFormat.PARQUET.name()))
          .setDataPath(Resources.getResource(RESOURCES_DIR + "/entity4").getPath())
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
  public void testEntityWithPartitionsCsvToAvroE2E() throws IOException {
    DataplexClient dataplex = mock(DataplexClient.class);
    when(dataplex.getEntities(ImmutableList.of(entity1.getName())))
        .thenReturn(ImmutableList.of(entity1));
    when(dataplex.getPartitions(entity1.getName()))
        .thenReturn(ImmutableList.of(partition11, partition12));
    when(dataplex.getAsset(outputAsset.getName())).thenReturn(outputAsset);

    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);
    options.setInputAssetOrEntitiesList(entity1.getName());
    options.setOutputFileFormat(OutputFileFormat.AVRO);
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
  public void testAssetWithEntityJsonToGzippedParquetE2E() throws IOException {
    DataplexClient dataplex = mock(DataplexClient.class);
    when(dataplex.getCloudStorageEntities(asset2.getName())).thenReturn(ImmutableList.of(entity2));
    when(dataplex.getPartitions(entity2.getName())).thenReturn(ImmutableList.of());
    when(dataplex.getAsset(outputAsset.getName())).thenReturn(outputAsset);

    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);
    options.setInputAssetOrEntitiesList(asset2.getName());
    options.setOutputFileFormat(OutputFileFormat.PARQUET);
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
  public void testAssetWithEntityAvroToParquetE2E() throws IOException {
    DataplexClient dataplex = mock(DataplexClient.class);
    when(dataplex.getEntities(ImmutableList.of(entity3.getName())))
        .thenReturn(ImmutableList.of(entity3));
    when(dataplex.getPartitions(entity3.getName())).thenReturn(ImmutableList.of());
    when(dataplex.getAsset(outputAsset.getName())).thenReturn(outputAsset);

    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);
    options.setInputAssetOrEntitiesList(entity3.getName());
    options.setOutputFileFormat(OutputFileFormat.PARQUET);
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
  public void testAssetWithEntityParquetToAvroE2E() throws IOException {
    DataplexClient dataplex = mock(DataplexClient.class);
    when(dataplex.getEntities(ImmutableList.of(entity4.getName())))
        .thenReturn(ImmutableList.of(entity4));
    when(dataplex.getPartitions(entity4.getName())).thenReturn(ImmutableList.of());
    when(dataplex.getAsset(outputAsset.getName())).thenReturn(outputAsset);

    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);
    options.setInputAssetOrEntitiesList(entity4.getName());
    options.setOutputFileFormat(OutputFileFormat.AVRO);
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
    String relativeInputPath =
        new File(RESOURCES_DIR).toURI().relativize(new File(inputPath).toURI()).getPath();
    return outputBucket + '/' + relativeInputPath;
  }
}
