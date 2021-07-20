/*
 * Copyright (C) 2019 Google LLC
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

import com.google.cloud.teleport.v2.templates.FileFormatConversion.FileFormatConversionOptions;
import com.google.cloud.teleport.v2.transforms.AvroConverters;
import com.google.cloud.teleport.v2.transforms.ParquetConverters;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.common.io.Resources;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

/** Test cases for the {@link FileFormatConversion} class. */
public class FileFormatConversionTest {
  @Rule public final transient TestPipeline mainPipeline = TestPipeline.create();

  @Rule public final transient TestPipeline readPipeline = TestPipeline.create();

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Rule public final transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String RESOURCES_DIR = "FileFormatConversionTest/";

  private static final String CSV_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "csv_file.csv").getPath();

  private static final String CSV_FILE_WITH_MISSING_FIELD_PATH =
      Resources.getResource(RESOURCES_DIR + "missing_field.csv").getPath();

  private static final String AVRO_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "avro_file.avro").getPath();

  private static final String PARQUET_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "parquet_file.parquet").getPath();

  private static final String SCHEMA_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "avro_schema.json").getPath();

  private static final String SCHEMA_FILE_TWO_PATH =
      Resources.getResource(RESOURCES_DIR + "avro_schema_two.json").getPath();

  private static final String CSV = "CSV";

  private static final String AVRO = "AVRO";

  private static final String PARQUET = "PARQUET";

  /**
   * Tests {@link FileFormatConversion#run(FileFormatConversionOptions)} throws an exception if an
   * invalid file format is provided.
   */
  @Test
  public void testInvalidFileFormat() {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Provide correct input/output file format.");

    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);

    options.setInputFileFormat("INVALID");
    options.setOutputFileFormat(AVRO);

    FileFormatConversion.run(options);
  }

  /**
   * Tests {@link FileFormatConversion#run(FileFormatConversionOptions)} throws an exception if the
   * same input and output file formats are provided.
   */
  @Test
  public void testSameInputAndOutputFileFormat() {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Provide correct input/output file format.");

    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);

    options.setInputFileFormat(AVRO);
    options.setOutputFileFormat(AVRO);

    FileFormatConversion.run(options);
  }

  /** Tests if the Csv to Avro pipeline transforms data correctly and stores it in an Avro file. */
  @Test
  public void testCsvToAvroE2E() {
    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);

    String tempDir = temporaryFolder.getRoot().getAbsolutePath() + "/";

    options.setInputFileFormat(CSV);
    options.setOutputFileFormat(AVRO);
    options.setInputFileSpec(CSV_FILE_PATH);
    options.setOutputBucket(tempDir);
    options.setContainsHeaders(true);
    options.setSchema(SCHEMA_FILE_PATH);
    options.setDelimiter("|");

    Schema schema = SchemaUtils.getAvroSchema(SCHEMA_FILE_PATH);
    GenericRecord genericRecords = new GenericData.Record(schema);
    genericRecords.put("id", "007");
    genericRecords.put("state", "CA");
    genericRecords.put("price", 26.23);

    mainPipeline.apply(
        "TestCsvToAvro",
        FileFormatConversionFactory.FileFormat.newBuilder()
            .setOptions(options)
            .setInputFileFormat(CSV)
            .setOutputFileFormat(AVRO)
            .build());
    mainPipeline.run();

    PCollection<GenericRecord> readAvroFile =
        readPipeline.apply(
            "ReadAvroFile",
            AvroConverters.ReadAvroFile.newBuilder()
                .withInputFileSpec(tempDir + "*")
                .withSchema(SCHEMA_FILE_PATH)
                .build());

    PAssert.that(readAvroFile).containsInAnyOrder(genericRecords);

    readPipeline.run();
  }

  /**
   * Tests if the Csv to Parquet pipeline transforms data correctly and stores it in a Parquet file.
   */
  @Test
  public void testCsvToParquetE2E() {
    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);

    final String tempDir = temporaryFolder.getRoot().getAbsolutePath() + "/";

    options.setInputFileFormat(CSV);
    options.setOutputFileFormat(PARQUET);
    options.setInputFileSpec(CSV_FILE_PATH);
    options.setOutputBucket(tempDir);
    options.setContainsHeaders(true);
    options.setSchema(SCHEMA_FILE_PATH);
    options.setDelimiter("|");

    Schema schema = SchemaUtils.getAvroSchema(SCHEMA_FILE_PATH);
    GenericRecord genericRecords = new GenericData.Record(schema);
    genericRecords.put("id", "007");
    genericRecords.put("state", "CA");
    genericRecords.put("price", 26.23);

    mainPipeline.apply(
        "TestCsvToParquet",
        FileFormatConversionFactory.FileFormat.newBuilder()
            .setOptions(options)
            .setInputFileFormat(CSV)
            .setOutputFileFormat(PARQUET)
            .build());
    mainPipeline.run();

    PCollection<GenericRecord> readParquetFile =
        readPipeline.apply(
            "ReadParquetFile",
            ParquetConverters.ReadParquetFile.newBuilder()
                .withInputFileSpec(tempDir + "*")
                .withSchema(SCHEMA_FILE_PATH)
                .build());

    PAssert.that(readParquetFile).containsInAnyOrder(genericRecords);

    readPipeline.run();
  }

  /**
   * Tests if the Avro to Parquet pipeline transforms data correctly and stores it in a Parquet
   * file.
   */
  @Test
  public void testAvroToParquetE2E() {
    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);

    final String tempDir = temporaryFolder.getRoot().getAbsolutePath() + "/";

    options.setInputFileFormat(AVRO);
    options.setOutputFileFormat(PARQUET);
    options.setInputFileSpec(AVRO_FILE_PATH);
    options.setOutputBucket(tempDir);
    options.setSchema(SCHEMA_FILE_PATH);

    Schema schema = SchemaUtils.getAvroSchema(SCHEMA_FILE_PATH);
    GenericRecord genericRecords = new GenericData.Record(schema);
    genericRecords.put("id", "007");
    genericRecords.put("state", "CA");
    genericRecords.put("price", 26.23);

    mainPipeline.apply(
        "TestAvroToParquet",
        FileFormatConversionFactory.FileFormat.newBuilder()
            .setOptions(options)
            .setInputFileFormat(AVRO)
            .setOutputFileFormat(PARQUET)
            .build());
    mainPipeline.run();

    PCollection<GenericRecord> readParquetFile =
        readPipeline.apply(
            "ReadParquetFile",
            ParquetConverters.ReadParquetFile.newBuilder()
                .withInputFileSpec(tempDir + "*")
                .withSchema(SCHEMA_FILE_PATH)
                .build());

    PAssert.that(readParquetFile).containsInAnyOrder(genericRecords);

    readPipeline.run();
  }

  /**
   * Tests if the Parquet to Avro pipeline transforms data correctly and stores it in an Avro file.
   */
  @Test
  public void testParquetToAvroE2E() {
    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);

    final String tempDir = temporaryFolder.getRoot().getAbsolutePath() + "/";

    options.setInputFileFormat(PARQUET);
    options.setOutputFileFormat(AVRO);
    options.setInputFileSpec(PARQUET_FILE_PATH);
    options.setOutputBucket(tempDir);
    options.setSchema(SCHEMA_FILE_PATH);

    Schema schema = SchemaUtils.getAvroSchema(SCHEMA_FILE_PATH);
    GenericRecord genericRecords = new GenericData.Record(schema);
    genericRecords.put("id", "007");
    genericRecords.put("state", "CA");
    genericRecords.put("price", 26.23);

    mainPipeline.apply(
        "TestParquetToAvro",
        FileFormatConversionFactory.FileFormat.newBuilder()
            .setOptions(options)
            .setInputFileFormat(PARQUET)
            .setOutputFileFormat(AVRO)
            .build());
    mainPipeline.run();

    PCollection<GenericRecord> readAvroFile =
        readPipeline.apply(
            "ReadAvroFile",
            AvroConverters.ReadAvroFile.newBuilder()
                .withInputFileSpec(tempDir + "*")
                .withSchema(SCHEMA_FILE_PATH)
                .build());

    PAssert.that(readAvroFile).containsInAnyOrder(genericRecords);

    readPipeline.run();
  }

  /** Tests if the Csv to Avro pipeline can handle empty fields in the Csv file. */
  @Test
  public void testCsvToAvroWithEmptyField() {
    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);

    String tempDir = temporaryFolder.getRoot().getAbsolutePath() + "/";

    options.setInputFileFormat(CSV);
    options.setOutputFileFormat(AVRO);
    options.setInputFileSpec(CSV_FILE_WITH_MISSING_FIELD_PATH);
    options.setOutputBucket(tempDir);
    options.setContainsHeaders(true);
    options.setSchema(SCHEMA_FILE_TWO_PATH);

    Schema schema = SchemaUtils.getAvroSchema(SCHEMA_FILE_TWO_PATH);
    GenericRecord genericRecords = new GenericData.Record(schema);
    genericRecords.put("id", "007");
    genericRecords.put("state", "CA");
    genericRecords.put("price", null);

    mainPipeline.apply(
        "TestCsvToAvroWithEmptyField",
        FileFormatConversionFactory.FileFormat.newBuilder()
            .setOptions(options)
            .setInputFileFormat(CSV)
            .setOutputFileFormat(AVRO)
            .build());
    mainPipeline.run();

    PCollection<GenericRecord> readAvroFile =
        readPipeline.apply(
            "ReadAvroFile",
            AvroConverters.ReadAvroFile.newBuilder()
                .withInputFileSpec(tempDir + "*")
                .withSchema(SCHEMA_FILE_TWO_PATH)
                .build());

    PAssert.that(readAvroFile).containsInAnyOrder(genericRecords);

    readPipeline.run();
  }

  /** Tests if the Csv to Parquet pipeline can handle empty fields in the Csv file. */
  @Test
  public void testCsvToParquetWithEmptyField() {
    FileFormatConversionOptions options =
        PipelineOptionsFactory.create().as(FileFormatConversionOptions.class);

    String tempDir = temporaryFolder.getRoot().getAbsolutePath() + "/";

    options.setInputFileFormat(CSV);
    options.setOutputFileFormat(PARQUET);
    options.setInputFileSpec(CSV_FILE_WITH_MISSING_FIELD_PATH);
    options.setOutputBucket(tempDir);
    options.setContainsHeaders(true);
    options.setSchema(SCHEMA_FILE_TWO_PATH);

    Schema schema = SchemaUtils.getAvroSchema(SCHEMA_FILE_TWO_PATH);
    GenericRecord genericRecords = new GenericData.Record(schema);
    genericRecords.put("id", "007");
    genericRecords.put("state", "CA");
    genericRecords.put("price", null);

    mainPipeline.apply(
        "TestCsvToParquetWithEmptyField",
        FileFormatConversionFactory.FileFormat.newBuilder()
            .setOptions(options)
            .setInputFileFormat(CSV)
            .setOutputFileFormat(PARQUET)
            .build());
    mainPipeline.run();

    PCollection<GenericRecord> readParquetFile =
        readPipeline.apply(
            "ReadParquetFile",
            ParquetConverters.ReadParquetFile.newBuilder()
                .withInputFileSpec(tempDir + "*")
                .withSchema(SCHEMA_FILE_TWO_PATH)
                .build());

    PAssert.that(readParquetFile).containsInAnyOrder(genericRecords);

    readPipeline.run();
  }
}
