/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.bigtable;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.cloud.teleport.bigtable.BigtableToAvro.Options;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.util.DualInputNestedValueProvider;
import com.google.cloud.teleport.util.DualInputNestedValueProvider.TranslatorInput;
import com.google.protobuf.ByteOutput;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;

/**
 * Dataflow pipeline that exports data from a Cloud Bigtable table to Avro files in GCS. Currently,
 * filtering on Cloud Bigtable table is not supported.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Cloud_Bigtable_to_GCS_Avro.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Cloud_Bigtable_to_GCS_Avro",
    category = TemplateCategory.BATCH,
    displayName = "Cloud Bigtable to Avro Files in Cloud Storage",
    description =
        "The Bigtable to Cloud Storage Avro template is a pipeline that reads data from a Bigtable table and writes it to a Cloud Storage bucket in Avro format. "
            + "You can use the template to move data from Bigtable to Cloud Storage.",
    optionsClass = Options.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/bigtable-to-avro",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Bigtable table must exist.",
      "The output Cloud Storage bucket must exist before running the pipeline."
    })
public class BigtableToAvro {

  /** Options for the export pipeline. */
  public interface Options extends PipelineOptions {
    @TemplateParameter.ProjectId(
        order = 1,
        description = "Project ID",
        helpText =
            "The ID of the Google Cloud project of the Cloud Bigtable instance that you want to"
                + " read data from")
    ValueProvider<String> getBigtableProjectId();

    @SuppressWarnings("unused")
    void setBigtableProjectId(ValueProvider<String> projectId);

    @TemplateParameter.Text(
        order = 2,
        regexes = {"[a-z][a-z0-9\\-]+[a-z0-9]"},
        description = "Instance ID",
        helpText = "The ID of the Cloud Bigtable instance that contains the table")
    ValueProvider<String> getBigtableInstanceId();

    @SuppressWarnings("unused")
    void setBigtableInstanceId(ValueProvider<String> instanceId);

    @TemplateParameter.Text(
        order = 3,
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Table ID",
        helpText = "The ID of the Cloud Bigtable table to read")
    ValueProvider<String> getBigtableTableId();

    @SuppressWarnings("unused")
    void setBigtableTableId(ValueProvider<String> tableId);

    @TemplateParameter.GcsWriteFolder(
        order = 4,
        description = "Output file directory in Cloud Storage",
        helpText =
            "The path and filename prefix for writing output files. Must end with a slash. DateTime"
                + " formatting is used to parse directory path for date & time formatters.",
        example = "gs://your-bucket/your-path")
    ValueProvider<String> getOutputDirectory();

    @SuppressWarnings("unused")
    void setOutputDirectory(ValueProvider<String> outputDirectory);

    @TemplateParameter.Text(
        order = 5,
        description = "Avro file prefix",
        helpText = "The prefix of the Avro file name. For example, \"table1-\"")
    @Default.String("part")
    ValueProvider<String> getFilenamePrefix();

    @SuppressWarnings("unused")
    void setFilenamePrefix(ValueProvider<String> filenamePrefix);
  }

  /**
   * Runs a pipeline to export data from a Cloud Bigtable table to Avro files in GCS.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    PipelineResult result = run(options);

    // Wait for pipeline to finish only if it is not constructing a template.
    if (options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
      result.waitUntilFinish();
    }
  }

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

    ValueProvider<String> filePathPrefix =
        DualInputNestedValueProvider.of(
            options.getOutputDirectory(),
            options.getFilenamePrefix(),
            new SerializableFunction<TranslatorInput<String, String>, String>() {
              @Override
              public String apply(TranslatorInput<String, String> input) {
                return FileSystems.matchNewResource(input.getX(), true)
                    .resolve(input.getY(), StandardResolveOptions.RESOLVE_FILE)
                    .toString();
              }
            });

    pipeline
        .apply("Read from Bigtable", read)
        .apply("Transform to Avro", MapElements.via(new BigtableToAvroFn()))
        .apply(
            "Write to Avro in GCS",
            AvroIO.write(BigtableRow.class).to(filePathPrefix).withSuffix(".avro"));

    return pipeline.run();
  }

  /** Translates Bigtable {@link Row} to Avro {@link BigtableRow}. */
  static class BigtableToAvroFn extends SimpleFunction<Row, BigtableRow> {
    @Override
    public BigtableRow apply(Row row) {
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
      return new BigtableRow(key, cells);
    }
  }

  /**
   * Extracts the byte array from the given {@link ByteString} without copy.
   *
   * @param byteString A {@link ByteString} from which to extract the array.
   * @return an array of byte.
   */
  protected static byte[] toByteArray(final ByteString byteString) {
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
    public void write(ByteBuffer value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeLazy(ByteBuffer value) {
      throw new UnsupportedOperationException();
    }
  }
}
