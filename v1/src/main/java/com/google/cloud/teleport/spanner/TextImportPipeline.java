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
package com.google.cloud.teleport.spanner;

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateCreationParameter;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.spanner.TextImportPipeline.Options;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Text files to Cloud Spanner Import pipeline. This pipeline ingests CSV and other type of
 * delimited data from GCS and writes data to a Cloud Spanner database table. Each row from the
 * input CSV file will be applied to Cloud Spanner with an InsertOrUpdate mutation, so this can be
 * used both to populate new rows or to update columns of existing rows.
 *
 * <p>You can specify column delimiter other than comma. Also make sure to use field qualifier such
 * as double quote to escape delimiter if it is in the value.
 *
 * <p>Text file must NOT have a header.
 *
 * <p>Example Usage: Here is CSV sample data simulating an account table:
 * 1,sample_user_1,true,2018-01-01,2018-01-01T12:30:00Z
 *
 * <p>Schema file must have all column and type definition in one line. Schema file must use the
 * data type names of Cloud Spanner. We currently support the following Cloud Spanner data types: -
 * BOOL - DATE - FLOAT64 - INT64 - STRING - TIMESTAMP
 *
 * <p>Input format properties: - \\N in the source column will be considered as NULL value when
 * writing to Cloud Spanner. - If you need to escape characters, you can use the "fieldQualifier"
 * parameter to tell the pipeline. e.g. You can put all values inside double quotes like "123",
 * "john", "true" - See the implementation of parseRow() below to see what values are accepted for
 * each data type.
 *
 * <p>NOTE: BYTES, ARRAY, STRUCT types are not supported.
 *
 * <p>Example schema file for the CSV file above:
 *
 * <pre>Id:INT64,Username:STRING,Active:BOOL,CreateDate:DATE,ModifyTime:TIMESTAMP</pre>
 *
 * <p>Here is the DDL for creating Cloud Spanner table:
 *
 * <pre>CREATE TABLE example_table
 * ( Id INT64, Username STRING(MAX), Active BOOL, CreateDate DATE, ModifyTime TIMESTAMP )
 *  PRIMARY KEY(Id)
 * </pre>
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_GCS_Text_to_Cloud_Spanner.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "GCS_Text_to_Cloud_Spanner",
    category = TemplateCategory.BATCH,
    displayName = "Text Files on Cloud Storage to Cloud Spanner",
    description =
        "The Cloud Storage Text to Cloud Spanner template is a batch pipeline that reads CSV text files from "
            + "Cloud Storage and imports them to a Cloud Spanner database.",
    optionsClass = Options.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-storage-to-cloud-spanner",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The target Cloud Spanner database and table must exist.",
      "You must have read permissions for the Cloud Storage bucket and write permissions for the target Cloud Spanner database.",
      "The input Cloud Storage path containing the CSV files must exist.",
      "You must create an import manifest file containing a JSON description of the CSV files, and you must store that manifest file in Cloud Storage.",
      "If the target Cloud Spanner database already has a schema, any columns specified in the manifest file must have the same data types as their corresponding columns in the target database's schema.",
      // TODO: convey all the information
      "The manifest file, encoded in ASCII or UTF-8, must match the following format: ... TODO ..."
    })
public class TextImportPipeline {

  /** Options for {@link TextImportPipeline}. */
  public interface Options extends PipelineOptions {

    @TemplateParameter.Text(
        order = 1,
        regexes = {"^[a-z0-9\\-]+$"},
        description = "Cloud Spanner instance ID",
        helpText = "The instance ID of the Cloud Spanner database that you want to import to.")
    ValueProvider<String> getInstanceId();

    void setInstanceId(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 2,
        regexes = {"^[a-z_0-9\\-]+$"},
        description = "Cloud Spanner database ID",
        helpText =
            "The database ID of the Cloud Spanner database that you want to import into (must"
                + " already exist, and with the destination tables created).")
    ValueProvider<String> getDatabaseId();

    void setDatabaseId(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 3,
        optional = true,
        description = "Cloud Spanner Endpoint to call",
        helpText = "The Cloud Spanner endpoint to call in the template. Only used for testing.",
        example = "https://batch-spanner.googleapis.com")
    @Default.String("https://batch-spanner.googleapis.com")
    ValueProvider<String> getSpannerHost();

    void setSpannerHost(ValueProvider<String> value);

    @TemplateParameter.GcsReadFile(
        order = 4,
        description = "Text Import Manifest file",
        helpText =
            "The Cloud Storage path and filename of the text import manifest file. Text Import"
                + " Manifest file, storing a json-encoded importManifest object.",
        example = "gs://your-bucket/your-folder/your-manifest.json")
    ValueProvider<String> getImportManifest();

    void setImportManifest(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 5,
        optional = true,
        description = "Column delimiter of the data files",
        helpText = "The column delimiter of the input text files. Defaults to ','",
        example = ",")
    @Default.Character(',')
    ValueProvider<Character> getColumnDelimiter();

    void setColumnDelimiter(ValueProvider<Character> value);

    @TemplateParameter.Text(
        order = 6,
        optional = true,
        description = "Field qualifier used by the source file",
        helpText =
            "The field qualifier used by the source file. It should be used when character needs to"
                + " be escaped. Field qualifier should be used when character needs to be escaped."
                + " The default value is double quotes.")
    @Default.Character('"')
    ValueProvider<Character> getFieldQualifier();

    void setFieldQualifier(ValueProvider<Character> value);

    @TemplateParameter.Boolean(
        order = 7,
        optional = true,
        description = "If true, the lines has trailing delimiters",
        helpText =
            "The flag indicating whether or not the input lines have trailing delimiters. The"
                + " default value is true. If the text file contains trailing delimiter, then set"
                + " trailingDelimiter parameter to true during pipeline execution to import a Cloud"
                + " Spanner database from a set of text files, otherwise set it to false.")
    @Default.Boolean(true)
    ValueProvider<Boolean> getTrailingDelimiter();

    void setTrailingDelimiter(ValueProvider<Boolean> value);

    @TemplateParameter.Text(
        order = 8,
        optional = true,
        description = "Escape character",
        helpText =
            "The escape character. The default value is NULL (not using the escape character).")
    ValueProvider<Character> getEscape();

    void setEscape(ValueProvider<Character> value);

    @TemplateParameter.Text(
        order = 9,
        optional = true,
        description = "Null String",
        helpText =
            "The string that represents the NULL value. The default value is null (not using the"
                + " null string).")
    ValueProvider<String> getNullString();

    void setNullString(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 10,
        optional = true,
        description = "Date format",
        helpText =
            "The format used to parse date columns. By default, the pipeline tries to parse the"
                + " date columns as \"yyyy-MM-dd[' 00:00:00']\" (e.g., 2019-01-31, or 2019-01-31"
                + " 00:00:00). If your data format is different, please specify the format using"
                + " the java.time.format.DateTimeFormatter patterns. For more details, please refer"
                + " to https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/format/DateTimeFormatter.html")
    ValueProvider<String> getDateFormat();

    void setDateFormat(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 11,
        optional = true,
        description = "Timestamp format",
        helpText =
            "The format used to parse timestamp columns. If the timestamp is a long integer, then"
                + " it is treated as Unix epoch (the microsecond since 1970-01-01T00:00:00.000Z."
                + " Otherwise, it is parsed as a string using the"
                + " java.time.format.DateTimeFormatter.ISO_INSTANT format. For other cases, please"
                + " specify you own pattern string, e.g., \"MMM dd yyyy HH:mm:ss.SSSVV\" for"
                + " timestamp in the form of \"Jan 21 1998 01:02:03.456+08:00\". For more details,"
                + " please refer to"
                + " https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/format/DateTimeFormatter.html")
    ValueProvider<String> getTimestampFormat();

    void setTimestampFormat(ValueProvider<String> value);

    @TemplateCreationParameter(value = "false")
    @Description("If true, wait for job finish. The default value is true.")
    @Default.Boolean(true)
    boolean getWaitUntilFinish();

    void setWaitUntilFinish(boolean value);

    @TemplateParameter.ProjectId(
        order = 13,
        optional = true,
        description = "Cloud Spanner Project Id",
        helpText = "The project ID of the Cloud Spanner instance.")
    ValueProvider<String> getSpannerProjectId();

    void setSpannerProjectId(ValueProvider<String> value);

    @TemplateParameter.Enum(
        order = 14,
        enumOptions = {
          @TemplateEnumOption("LOW"),
          @TemplateEnumOption("MEDIUM"),
          @TemplateEnumOption("HIGH")
        },
        optional = true,
        description = "Priority for Spanner RPC invocations",
        helpText =
            "The request priority for Cloud Spanner calls. The value must be one of:"
                + " [HIGH,MEDIUM,LOW].")
    ValueProvider<RpcPriority> getSpannerPriority();

    void setSpannerPriority(ValueProvider<RpcPriority> value);

    @TemplateParameter.Boolean(
        order = 15,
        optional = true,
        description = "Handle new line",
        helpText =
            "If true, run the template in handleNewLine mode, which is slower but handles newline"
                + " characters inside data.")
    @Default.Boolean(false)
    ValueProvider<Boolean> getHandleNewLine();

    void setHandleNewLine(ValueProvider<Boolean> value);

    @TemplateParameter.GcsWriteFolder(
        order = 16,
        description = "Invalid rows output path",
        optional = true,
        helpText = "Cloud Storage path where to write rows that cannot be imported.",
        example = "gs://your-bucket/your-path")
    @Default.String("")
    ValueProvider<String> getInvalidOutputPath();

    void setInvalidOutputPath(ValueProvider<String> value);
  }

  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline p = Pipeline.create(options);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            // Temporary fix explicitly setting SpannerConfig.projectId to the default project
            // if spannerProjectId is not provided as a parameter. Required as of Beam 2.38,
            // which no longer accepts null label values on metrics, and SpannerIO#setup() has
            // a bug resulting in the label value being set to the original parameter value,
            // with no fallback to the default project.
            // TODO: remove NestedValueProvider when this is fixed in Beam.
            .withProjectId(
                NestedValueProvider.of(
                    options.getSpannerProjectId(),
                    (SerializableFunction<String, String>)
                        input -> input != null ? input : SpannerOptions.getDefaultProjectId()))
            .withHost(options.getSpannerHost())
            .withInstanceId(options.getInstanceId())
            .withDatabaseId(options.getDatabaseId())
            .withRpcPriority(options.getSpannerPriority());

    p.apply(
        new TextImportTransform(
            spannerConfig, options.getImportManifest(), options.getInvalidOutputPath()));

    PipelineResult result = p.run();
    if (options.getWaitUntilFinish()
        &&
        /* Only if template location is null, there is a dataflow job to wait for. Otherwise it's
         * template generation, which doesn't start a dataflow job.
         */
        options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
      result.waitUntilFinish();
    }
  }
}
