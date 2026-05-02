/*
 * Copyright (C) 2026 Google LLC
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

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.templates.BigtableToParquet.Options;
import com.google.protobuf.ByteOutput;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;

/**
 * Dataflow pipeline that exports data from a Cloud Bigtable table to Parquet files in GCS.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/bigtable-to-parquet/README_Cloud_Bigtable_to_GCS_Parquet.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Cloud_Bigtable_to_GCS_Parquet_Flex",
    category = TemplateCategory.BATCH,
    displayName = "Cloud Bigtable to Parquet Files on Cloud Storage (Flex)",
    description =
        "The Bigtable to Cloud Storage Parquet template is a pipeline that reads data from a Bigtable"
            + " table and writes it to a Cloud Storage bucket in Parquet format."
            + " You can use the template to move data from Bigtable to Cloud Storage.",
    optionsClass = Options.class,
    flexContainerName = "bigtable-to-parquet",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/bigtable-to-parquet",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Bigtable table must exist.",
      "The output Cloud Storage bucket must exist before running the pipeline."
    })
public class BigtableToParquet {

  /** Avro schema for Bigtable rows, matching the v1 classic template format. */
  private static final String BIGTABLE_ROW_AVSC =
      "{"
          + "\"name\":\"BigtableRow\","
          + "\"type\":\"record\","
          + "\"namespace\":\"com.google.cloud.teleport.bigtable\","
          + "\"fields\":["
          + "  {\"name\":\"key\",\"type\":\"bytes\"},"
          + "  {\"name\":\"cells\",\"type\":{\"type\":\"array\",\"items\":{"
          + "    \"name\":\"BigtableCell\",\"type\":\"record\",\"fields\":["
          + "      {\"name\":\"family\",\"type\":\"string\"},"
          + "      {\"name\":\"qualifier\",\"type\":\"bytes\"},"
          + "      {\"name\":\"timestamp\",\"type\":\"long\",\"logicalType\":\"timestamp-micros\"},"
          + "      {\"name\":\"value\",\"type\":\"bytes\"}"
          + "    ]"
          + "  }}}"
          + "]}";

  private static final Schema SCHEMA = new Schema.Parser().parse(BIGTABLE_ROW_AVSC);

  /** Options for the export pipeline. */
  public interface Options extends PipelineOptions {

    @TemplateParameter.ProjectId(
        order = 1,
        groupName = "Source",
        description = "Project ID",
        helpText =
            "The ID of the Google Cloud project that contains the Cloud Bigtable instance that"
                + " you want to read data from.")
    @Required
    String getBigtableProjectId();

    void setBigtableProjectId(String projectId);

    @TemplateParameter.Text(
        order = 2,
        groupName = "Source",
        regexes = {"[a-z][a-z0-9\\-]+[a-z0-9]"},
        description = "Instance ID",
        helpText = "The ID of the Cloud Bigtable instance that contains the table.")
    @Required
    String getBigtableInstanceId();

    void setBigtableInstanceId(String instanceId);

    @TemplateParameter.Text(
        order = 3,
        groupName = "Source",
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Table ID",
        helpText = "The ID of the Cloud Bigtable table to export.")
    @Required
    String getBigtableTableId();

    void setBigtableTableId(String tableId);

    @TemplateParameter.GcsWriteFolder(
        order = 4,
        groupName = "Target",
        description = "Output file directory in Cloud Storage",
        helpText =
            "The path and filename prefix for writing output files. Must end with a slash."
                + " For example: gs://your-bucket/your-path/.")
    @Required
    String getOutputDirectory();

    void setOutputDirectory(String outputDirectory);

    @TemplateParameter.Text(
        order = 5,
        groupName = "Target",
        description = "Parquet file prefix",
        helpText =
            "The prefix of the Parquet file name. For example, \"table1-\". Defaults to: \"part\".")
    @Default.String("part")
    String getFilenamePrefix();

    void setFilenamePrefix(String filenamePrefix);

    @TemplateParameter.Integer(
        order = 6,
        groupName = "Target",
        optional = true,
        description = "Maximum output shards",
        helpText =
            "The maximum number of output shards produced when writing. A higher number of shards"
                + " means higher throughput for writing to Cloud Storage, but potentially higher"
                + " data aggregation cost across shards when processing output Cloud Storage files."
                + " The default value is decided by Dataflow.")
    @Default.Integer(0)
    Integer getNumShards();

    void setNumShards(Integer numShards);

    @TemplateParameter.Text(
        order = 7,
        groupName = "Source",
        optional = true,
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Application profile ID",
        helpText =
            "The ID of the Bigtable application profile to use for the export. If you don't"
                + " specify an app profile, Bigtable uses the instance's default app profile:"
                + " https://cloud.google.com/bigtable/docs/app-profiles#default-app-profile.")
    @Default.String("default")
    String getBigtableAppProfileId();

    void setBigtableAppProfileId(String appProfileId);

    @TemplateParameter.Integer(
        order = 8,
        optional = true,
        description = "Minimum row count for page size check",
        helpText =
            "The minimum number of rows to buffer before checking if the page size threshold is"
                + " reached. With large rows, the default (100) can cause excessive memory use;"
                + " set a lower value (for example, 1) to flush pages more frequently."
                + " The default is 100.")
    Integer getMinRowCountForPageSizeCheck();

    void setMinRowCountForPageSizeCheck(Integer minRowCountForPageSizeCheck);
  }

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    run(options);
  }

  /**
   * Runs a pipeline to export data from a Cloud Bigtable table to Parquet file(s) in GCS.
   *
   * @param options arguments to the pipeline
   */
  public static PipelineResult run(Options options) {
    // Disable memory monitor for GC-intensive Bigtable pipelines.
    SdkHarnessOptions debugOptions = options.as(SdkHarnessOptions.class);
    debugOptions.setGCThrashingPercentagePerPeriod(100.00);

    Pipeline pipeline = Pipeline.create(debugOptions);

    BigtableIO.Read read =
        BigtableIO.read()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withAppProfileId(options.getBigtableAppProfileId())
            .withTableId(options.getBigtableTableId());

    ParquetIO.Sink parquetSink = ParquetIO.sink(SCHEMA);
    Integer minRowCount = options.getMinRowCountForPageSizeCheck();
    if (minRowCount != null) {
      parquetSink = parquetSink.withMinRowCountForPageSizeCheck(minRowCount);
    }

    FileIO.Write<Void, GenericRecord> write =
        FileIO.<GenericRecord>write()
            .via(parquetSink)
            .to(options.getOutputDirectory())
            .withPrefix(options.getFilenamePrefix())
            .withSuffix(".parquet");

    Integer numShards = options.getNumShards();
    if (numShards != null && numShards > 0) {
      write = write.withNumShards(numShards);
    }

    pipeline
        .apply("Read from Bigtable", read)
        .apply("Transform to Parquet", MapElements.via(new BigtableToParquetFn()))
        .setCoder(AvroCoder.of(GenericRecord.class, SCHEMA))
        .apply("Write to Parquet in GCS", write);

    return pipeline.run();
  }

  /** Translates Bigtable {@link Row} to Avro {@link GenericRecord}. */
  static class BigtableToParquetFn extends SimpleFunction<Row, GenericRecord> {
    @Override
    public GenericRecord apply(Row row) {
      ByteBuffer key = ByteBuffer.wrap(toByteArray(row.getKey()));
      List<GenericRecord> cells = new ArrayList<>();
      for (Family family : row.getFamiliesList()) {
        String familyName = family.getName();
        for (Column column : family.getColumnsList()) {
          ByteBuffer qualifier = ByteBuffer.wrap(toByteArray(column.getQualifier()));
          for (Cell cell : column.getCellsList()) {
            long timestamp = cell.getTimestampMicros();
            ByteBuffer value = ByteBuffer.wrap(toByteArray(cell.getValue()));
            cells.add(
                new GenericRecordBuilder(SCHEMA.getField("cells").schema().getElementType())
                    .set("family", familyName)
                    .set("qualifier", qualifier)
                    .set("timestamp", timestamp)
                    .set("value", value)
                    .build());
          }
        }
      }
      return new GenericRecordBuilder(SCHEMA).set("key", key).set("cells", cells).build();
    }
  }

  private static byte[] toByteArray(ByteString byteString) {
    try {
      ZeroCopyByteOutput byteOutput = new ZeroCopyByteOutput();
      UnsafeByteOperations.unsafeWriteTo(byteString, byteOutput);
      return byteOutput.bytes;
    } catch (IOException e) {
      return byteString.toByteArray();
    }
  }

  private static final class ZeroCopyByteOutput extends ByteOutput {
    private byte[] bytes;

    @Override
    public void writeLazy(byte[] value, int offset, int length) {
      if (offset != 0 || length != value.length) {
        throw new UnsupportedOperationException();
      }
      bytes = value;
    }

    @Override
    public void write(byte value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte[] value, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeLazy(ByteBuffer value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void write(ByteBuffer value) {
      throw new UnsupportedOperationException();
    }
  }
}
