/*
 * Copyright (C) 2023 Google LLC
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
import com.google.cloud.teleport.bigtable.BigtableToJson.Options;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.io.StringWriter;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow pipeline that exports data from a Cloud Bigtable table to JSON files in GCS. Currently,
 * filtering on Cloud Bigtable table is not supported.
 *
 * <p>Check out <a href=
 * "https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Cloud_Bigtable_to_GCS_JSON.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Cloud_Bigtable_to_GCS_Json",
    category = TemplateCategory.BATCH,
    displayName = "Cloud Bigtable to JSON",
    description =
        "The Bigtable to JSON template is a pipeline that reads data from a Bigtable table and writes it to a Cloud Storage bucket in JSON format",
    optionsClass = Options.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/bigtable-to-json", // TODO(rongal): create guide.
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Bigtable table must exist.",
      "The output Cloud Storage bucket must exist before running the pipeline."
    })
public class BigtableToJson {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableToJson.class);

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
        optional = true,
        description = "Cloud Storage directory for storing JSON files",
        helpText = "The Cloud Storage path where the output JSON files can be stored.",
        example = "gs://your-bucket/your-path/")
    ValueProvider<String> getOutputDirectory();

    @SuppressWarnings("unused")
    void setOutputDirectory(ValueProvider<String> outputDirectory);

    @TemplateParameter.Text(
        order = 5,
        description = "JSON file prefix",
        helpText = "The prefix of the JSON file name. For example, \"table1-\"")
    @Default.String("part")
    ValueProvider<String> getFilenamePrefix();

    @SuppressWarnings("unused")
    void setFilenamePrefix(ValueProvider<String> filenamePrefix);
  }

  /**
   * Runs a pipeline to export data from a Cloud Bigtable table to JSON files in GCS in JSON format.
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
    LOG.info("Completed pipeline setup");
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

    // Concatenating cloud storage folder with file prefix to get complete path
    ValueProvider<String> outputFilePrefix = options.getFilenamePrefix();

    ValueProvider<String> outputFilePathWithPrefix =
        ValueProvider.NestedValueProvider.of(
            options.getOutputDirectory(),
            (SerializableFunction<String, String>)
                folder -> {
                  if (!folder.endsWith("/")) {
                    // Appending the slash if not provided by user
                    folder = folder + "/";
                  }
                  return folder + outputFilePrefix.get();
                });
    pipeline
        .apply("Read from Bigtable", read)
        .apply("Transform to JSON", MapElements.via(new BigtableToJsonFn()))
        .apply("Write to storage", TextIO.write().to(outputFilePathWithPrefix).withSuffix(".json"));

    return pipeline.run();
  }

  /** Translates Bigtable {@link Row} to JSON. */
  static class BigtableToJsonFn extends SimpleFunction<Row, String> {
    @Override
    public String apply(Row row) {
      StringWriter stringWriter = new StringWriter();
      JsonWriter jsonWriter = new JsonWriter(stringWriter);
      try {
        jsonWriter.beginObject();
        jsonWriter.name(row.getKey().toStringUtf8());
        jsonWriter.beginObject();
        for (Family family : row.getFamiliesList()) {
          String familyName = family.getName();
          jsonWriter.name(familyName);
          jsonWriter.beginObject();
          for (Column column : family.getColumnsList()) {
            for (Cell cell : column.getCellsList()) {
              jsonWriter
                  .name(column.getQualifier().toStringUtf8())
                  .value(cell.getValue().toStringUtf8());
            }
          }
          jsonWriter.endObject();
        }
        jsonWriter.endObject();
        jsonWriter.endObject();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return stringWriter.toString();
    }
  }
}
