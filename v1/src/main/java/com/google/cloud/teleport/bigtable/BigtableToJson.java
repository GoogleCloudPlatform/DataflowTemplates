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
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
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
import org.apache.commons.lang3.StringUtils;
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
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/bigtable-to-json",
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
        groupName = "Source",
        description = "Project ID",
        helpText =
            "The ID for the Google Cloud project that contains the Bigtable instance that you want to read data from.")
    ValueProvider<String> getBigtableProjectId();

    @SuppressWarnings("unused")
    void setBigtableProjectId(ValueProvider<String> projectId);

    @TemplateParameter.Text(
        order = 2,
        groupName = "Source",
        regexes = {"[a-z][a-z0-9\\-]+[a-z0-9]"},
        description = "Instance ID",
        helpText = "The ID of the Bigtable instance that contains the table.")
    ValueProvider<String> getBigtableInstanceId();

    @SuppressWarnings("unused")
    void setBigtableInstanceId(ValueProvider<String> instanceId);

    @TemplateParameter.Text(
        order = 3,
        groupName = "Source",
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Table ID",
        helpText = "The ID of the Bigtable table to read from.")
    ValueProvider<String> getBigtableTableId();

    @SuppressWarnings("unused")
    void setBigtableTableId(ValueProvider<String> tableId);

    @TemplateParameter.GcsWriteFolder(
        order = 4,
        groupName = "Target",
        optional = true,
        description = "Cloud Storage directory for storing JSON files",
        helpText = "The Cloud Storage path where the output JSON files are stored.",
        example = "gs://your-bucket/your-path/")
    ValueProvider<String> getOutputDirectory();

    @SuppressWarnings("unused")
    void setOutputDirectory(ValueProvider<String> outputDirectory);

    @TemplateParameter.Text(
        order = 5,
        groupName = "Target",
        description = "JSON file prefix",
        helpText =
            "The prefix of the JSON file name. For example, `table1-`. If no value is provided, defaults to `part`.")
    @Default.String("part")
    ValueProvider<String> getFilenamePrefix();

    @SuppressWarnings("unused")
    void setFilenamePrefix(ValueProvider<String> filenamePrefix);

    @TemplateParameter.Enum(
        order = 6,
        groupName = "Target",
        optional = true,
        enumOptions = {@TemplateEnumOption("FLATTEN"), @TemplateEnumOption("NONE")},
        description = "User option",
        helpText =
            "Possible values are `FLATTEN` or `NONE`. `FLATTEN` flattens the row to the single level. `NONE` stores the whole row as a JSON string. Defaults to `NONE`.")
    @Default.String("NONE")
    String getUserOption();

    @SuppressWarnings("unused")
    void setUserOption(String userOption);

    @TemplateParameter.Text(
        order = 7,
        groupName = "Target",
        optional = true,
        parentName = "userOption",
        parentTriggerValues = {"FLATTEN"},
        description = "Columns aliases",
        helpText =
            "A comma-separated list of columns that are required for the Vertex AI Vector Search index. The"
                + " columns `id` and `embedding` are required for Vertex AI Vector Search. You can use the notation"
                + " `fromfamily:fromcolumn;to`. For example, if the columns are `rowkey` and `cf:my_embedding`, where"
                + " `rowkey` has a different name than the embedding column, specify `cf:my_embedding;embedding` and,"
                + " `rowkey;id`. Only use this option when the value for `userOption` is `FLATTEN`.")
    ValueProvider<String> getColumnsAliases();

    @SuppressWarnings("unused")
    void setColumnsAliases(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 8,
        groupName = "Source",
        optional = true,
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Application profile ID",
        helpText =
            "The ID of the Bigtable application profile to use for the export. If you don't specify an app profile, Bigtable uses the instance's default app profile: https://cloud.google.com/bigtable/docs/app-profiles#default-app-profile.")
    @Default.String("default")
    ValueProvider<String> getBigtableAppProfileId();

    @SuppressWarnings("unused")
    void setBigtableAppProfileId(ValueProvider<String> appProfileId);
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
            .withAppProfileId(options.getBigtableAppProfileId())
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
    String userOption = options.getUserOption();
    pipeline
        .apply("Read from Bigtable", read)
        .apply(
            "Transform to JSON",
            MapElements.via(
                new BigtableToJsonFn(userOption.equals("FLATTEN"), options.getColumnsAliases())))
        .apply("Write to storage", TextIO.write().to(outputFilePathWithPrefix).withSuffix(".json"));

    return pipeline.run();
  }

  /** Translates Bigtable {@link Row} to JSON. */
  static class BigtableToJsonFn extends SimpleFunction<Row, String> {
    private boolean flatten;
    private ValueProvider<String> columnsAliases;

    public BigtableToJsonFn(boolean flatten, ValueProvider<String> columnsAliases) {
      this.flatten = flatten;
      this.columnsAliases = columnsAliases;
    }

    @Override
    public String apply(Row row) {
      StringWriter stringWriter = new StringWriter();
      JsonWriter jsonWriter = new JsonWriter(stringWriter);
      try {
        if (flatten) {
          serializeFlattented(row, jsonWriter);
        } else {
          serializeUnFlattented(row, jsonWriter);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return stringWriter.toString();
    }

    private void serializeUnFlattented(Row row, JsonWriter jsonWriter) throws IOException {
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
    }

    private void serializeFlattented(Row row, JsonWriter jsonWriter) throws IOException {
      jsonWriter.beginObject();
      Map<String, String> columnsWithAliases = extractColumnsAliases();

      maybeAddToJson(jsonWriter, columnsWithAliases, "rowkey", row.getKey().toStringUtf8());
      for (Family family : row.getFamiliesList()) {
        String familyName = family.getName();
        for (Column column : family.getColumnsList()) {
          for (Cell cell : column.getCellsList()) {
            maybeAddToJson(
                jsonWriter,
                columnsWithAliases,
                familyName + ":" + column.getQualifier().toStringUtf8(),
                cell.getValue().toStringUtf8());
          }
        }
      }
      jsonWriter.endObject();
    }

    private void maybeAddToJson(
        JsonWriter jsonWriter, Map<String, String> columnsWithAliases, String key, String value)
        throws IOException {
      if (!columnsWithAliases.isEmpty() && !columnsWithAliases.containsKey(key)) {
        return;
      }
      jsonWriter.name(columnsWithAliases.getOrDefault(key, key)).value(value);
    }

    private Map<String, String> extractColumnsAliases() {
      Map<String, String> columnsWithAliases = new HashMap<>();
      if (StringUtils.isBlank(columnsAliases.get())) {
        return columnsWithAliases;
      }
      String[] columnsList = columnsAliases.get().split(",");

      for (String columnsWithAlias : columnsList) {
        String[] columnWithAlias = columnsWithAlias.split(";");
        if (columnWithAlias.length == 2) {
          columnsWithAliases.put(columnWithAlias[0], columnWithAlias[1]);
        }
      }
      return columnsWithAliases;
    }
  }
}
