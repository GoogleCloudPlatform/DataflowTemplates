/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.bigtable;

import static com.google.cloud.teleport.bigtable.BigtableToAvro.toByteArray;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

/**
 * Dataflow pipeline that exports data from a Cloud Bigtable table to Parquet files in GCS.
 * Currently, filtering on Cloud Bigtable table is not supported.
 */
public class BigtableToParquet {

  /** Options for the export pipeline. */
  public interface Options extends PipelineOptions {

    @Description("The project that contains the table to export.")
    ValueProvider<String> getBigtableProjectId();

    @SuppressWarnings("unused")
    void setBigtableProjectId(ValueProvider<String> projectId);

    @Description("The Bigtable instance id that contains the table to export.")
    ValueProvider<String> getBigtableInstanceId();

    @SuppressWarnings("unused")
    void setBigtableInstanceId(ValueProvider<String> instanceId);

    @Description("The Bigtable table id to export.")
    ValueProvider<String> getBigtableTableId();

    @SuppressWarnings("unused")
    void setBigtableTableId(ValueProvider<String> tableId);

    @Description("The output location to write to (e.g. gs://mybucket/somefolder/)")
    ValueProvider<String> getOutputDirectory();

    @SuppressWarnings("unused")
    void setOutputDirectory(ValueProvider<String> outputDirectory);

    @Description(
        "The prefix for each exported file in outputDirectory. Default file prefix is set to"
            + " \"output\".")
    @Default.String("output")
    ValueProvider<String> getFilenamePrefix();

    @SuppressWarnings("unused")
    void setFilenamePrefix(ValueProvider<String> filenamePrefix);

    @Description(
        "The maximum number of output shards produced when writing. Default number is runner"
            + " defined. ")
    @Default.Integer(0)
    ValueProvider<Integer> getNumShards();

    @SuppressWarnings("unused")
    void setNumShards(ValueProvider<Integer> numShards);
  }

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    PipelineResult result = run(options);

    // Wait for pipeline to finish only if it is not constructing a template.
    if (options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
      result.waitUntilFinish();
    }
  }

  /**
   * Runs a pipeline to export data from a Cloud Bigtable table to Parquet file(s) in GCS.
   *
   * @param options arguments to the pipeline
   */
  public static PipelineResult run(Options options) {
    Pipeline pipeline = Pipeline.create(PipelineUtils.tweakPipelineOptions(options));
    BigtableIO.Read read =
        BigtableIO.read()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId());

    // Do not validate input fields if it is running as a template.
    if (options.as(DataflowPipelineOptions.class).getTemplateLocation() != null) {
      read = read.withoutValidation();
    }

    /**
     * Steps:
     * 1) Read records from Bigtable.
     * 2) Convert a Bigtable Row to a GenericRecord.
     * 3) Write GenericRecord(s) to GCS in parquet format.
     */
    pipeline
        .apply("Read from Bigtable", read)
        .apply("Transform to Parquet", MapElements.via(new BigtableToParquetFn()))
        .setCoder(AvroCoder.of(GenericRecord.class, BigtableRow.getClassSchema()))
        .apply(
            "Write to Parquet in GCS",
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(BigtableRow.getClassSchema()))
                .to(options.getOutputDirectory())
                .withPrefix(options.getFilenamePrefix())
                .withSuffix(".parquet")
                .withNumShards(options.getNumShards()));

    return pipeline.run();
  }

  /**
   * Translates a {@link PCollection} of Bigtable {@link Row} to a {@link PCollection} of {@link
   * GenericRecord}.
   */
  static class BigtableToParquetFn extends SimpleFunction<Row, GenericRecord> {
    @Override
    public GenericRecord apply(Row row) {
      ByteBuffer key = ByteBuffer.wrap(toByteArray(row.getKey()));
      List<BigtableCell> cells = new ArrayList<>();
      for (Family family : row.getFamiliesList()) {
        String familyName = family.getName();
        for (Column column : family.getColumnsList()) {
          ByteBuffer qualifier = ByteBuffer.wrap(toByteArray(column.getQualifier()));
          for (Cell cell : column.getCellsList()) {
            long timestamp = cell.getTimestampMicros();
            ByteBuffer value = ByteBuffer.wrap(toByteArray(cell.getValue()));
            cells.add(new BigtableCell(familyName, qualifier, timestamp, value));
          }
        }
      }
      return new GenericRecordBuilder(BigtableRow.getClassSchema())
          .set("key", key)
          .set("cells", cells)
          .build();
    }
  }
}
