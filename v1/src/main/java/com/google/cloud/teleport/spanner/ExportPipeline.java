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
package com.google.cloud.teleport.spanner;

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateCreationParameter;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.spanner.ExportPipeline.ExportPipelineOptions;
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
 * Dataflow template that exports a Cloud Spanner database to Avro files in GCS.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Cloud_Spanner_to_GCS_Avro.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Cloud_Spanner_to_GCS_Avro",
    category = TemplateCategory.BATCH,
    displayName = "Cloud Spanner to Avro Files on Cloud Storage",
    description = {
      "The Cloud Spanner to Avro Files on Cloud Storage template is a batch pipeline that exports a whole Cloud Spanner database to Cloud Storage in Avro format. "
          + "Exporting a Cloud Spanner database creates a folder in the bucket you select. The folder contains:\n"
          + "- A `spanner-export.json` file.\n"
          + "- A `TableName-manifest.json` file for each table in the database you exported.\n"
          + "- One or more `TableName.avro-#####-of-#####` files.\n",
      "For example, exporting a database with two tables, Singers and Albums, creates the following file set:\n"
          + "- `Albums-manifest.json`\n"
          + "- `Albums.avro-00000-of-00002`\n"
          + "- `Albums.avro-00001-of-00002`\n"
          + "- `Singers-manifest.json`\n"
          + "- `Singers.avro-00000-of-00003`\n"
          + "- `Singers.avro-00001-of-00003`\n"
          + "- `Singers.avro-00002-of-00003`\n"
          + "- `spanner-export.json`"
    },
    optionsClass = ExportPipelineOptions.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-spanner-to-avro",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Cloud Spanner database must exist.",
      "The output Cloud Storage bucket must exist.",
      "In addition to the Identity and Access Management (IAM) roles necessary to run Dataflow jobs, you must also have the <a href=\"https://cloud.google.com/spanner/docs/export#iam\">appropriate IAM roles</a> for reading your Cloud Spanner data and writing to your Cloud Storage bucket."
    })
public class ExportPipeline {

  /** Options for Export pipeline. */
  public interface ExportPipelineOptions extends PipelineOptions {
    @TemplateParameter.Text(
        order = 1,
        regexes = {"[a-z][a-z0-9\\-]*[a-z0-9]"},
        description = "Cloud Spanner instance id",
        helpText = "The instance id of the Cloud Spanner database that you want to export.")
    ValueProvider<String> getInstanceId();

    void setInstanceId(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 2,
        regexes = {"[a-z][a-z0-9_\\-]*[a-z0-9]"},
        description = "Cloud Spanner database id",
        helpText = "The database id of the Cloud Spanner database that you want to export.")
    ValueProvider<String> getDatabaseId();

    void setDatabaseId(ValueProvider<String> value);

    @TemplateParameter.GcsWriteFolder(
        order = 3,
        description = "Cloud Storage output directory",
        helpText =
            "The Cloud Storage path where the Avro files should be exported to. A new directory"
                + " will be created under this path that contains the export.",
        example = "gs://your-bucket/your-path")
    ValueProvider<String> getOutputDir();

    void setOutputDir(ValueProvider<String> value);

    @TemplateParameter.GcsWriteFolder(
        order = 4,
        optional = true,
        description = "Cloud Storage temp directory for storing Avro files",
        helpText =
            "The Cloud Storage path where the temporary Avro files can be created. Ex:"
                + " gs://your-bucket/your-path")
    ValueProvider<String> getAvroTempDirectory();

    void setAvroTempDirectory(ValueProvider<String> value);

    @TemplateCreationParameter(value = "")
    @Description("Test dataflow job identifier for Beam Direct Runner")
    @Default.String(value = "")
    ValueProvider<String> getTestJobId();

    void setTestJobId(ValueProvider<String> jobId);

    @TemplateParameter.Text(
        order = 6,
        optional = true,
        description = "Cloud Spanner Endpoint to call",
        helpText = "The Cloud Spanner endpoint to call in the template. Only used for testing.",
        example = "https://batch-spanner.googleapis.com")
    @Default.String("https://batch-spanner.googleapis.com")
    ValueProvider<String> getSpannerHost();

    void setSpannerHost(ValueProvider<String> value);

    @TemplateCreationParameter(value = "false")
    @Description("If true, wait for job finish")
    @Default.Boolean(true)
    boolean getWaitUntilFinish();

    void setWaitUntilFinish(boolean value);

    @TemplateParameter.Text(
        order = 7,
        optional = true,
        regexes = {
          "^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):(([0-9]{2})(\\.[0-9]+)?)Z$"
        },
        description = "Snapshot time",
        helpText =
            "Specifies the snapshot time as RFC 3339 format in UTC time without the timezone"
                + " offset(always ends in 'Z'). Timestamp must be in the past and Maximum timestamp"
                + " staleness applies. See"
                + " https://cloud.google.com/spanner/docs/timestamp-bounds#maximum_timestamp_staleness",
        example = "1990-12-31T23:59:59Z")
    @Default.String(value = "")
    ValueProvider<String> getSnapshotTime();

    void setSnapshotTime(ValueProvider<String> value);

    @TemplateParameter.ProjectId(
        order = 8,
        optional = true,
        description = "Cloud Spanner Project Id",
        helpText = "The project id of the Cloud Spanner instance.")
    ValueProvider<String> getSpannerProjectId();

    void setSpannerProjectId(ValueProvider<String> value);

    @TemplateParameter.Boolean(
        order = 9,
        optional = true,
        description = "Export Timestamps as Timestamp-micros type",
        helpText =
            "If true, Timestamps are exported as timestamp-micros type. Timestamps are exported as"
                + " ISO8601 strings at nanosecond precision by default.")
    @Default.Boolean(false)
    ValueProvider<Boolean> getShouldExportTimestampAsLogicalType();

    void setShouldExportTimestampAsLogicalType(ValueProvider<Boolean> value);

    @TemplateParameter.Text(
        order = 10,
        optional = true,
        regexes = {"^[a-zA-Z0-9_]+(,[a-zA-Z0-9_]+)*$"},
        description = "Cloud Spanner table name(s).",
        helpText =
            "If provided, only this comma separated list of tables are exported. Ancestor tables"
                + " and tables that are referenced via foreign keys are required. If not explicitly"
                + " listed, the `shouldExportRelatedTables` flag must be set for a successful"
                + " export.")
    @Default.String(value = "")
    ValueProvider<String> getTableNames();

    void setTableNames(ValueProvider<String> value);

    @TemplateParameter.Boolean(
        order = 11,
        optional = true,
        description = "Export necessary Related Spanner tables.",
        helpText =
            "Used in conjunction with `tableNames`. If true, add related tables necessary for the"
                + " export, such as interleaved parent tables and foreign keys tables.  If"
                + " `tableNames` is specified but doesn't include related tables, this option must"
                + " be set to true for a successful export.")
    @Default.Boolean(false)
    ValueProvider<Boolean> getShouldExportRelatedTables();

    void setShouldExportRelatedTables(ValueProvider<Boolean> value);

    @TemplateParameter.Enum(
        order = 12,
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
        order = 13,
        optional = true,
        description = "Use independent compute resource (Spanner DataBoost).",
        helpText =
            "Use Spanner on-demand compute so the export job will run on independent compute"
                + " resources and have no impact to current Spanner workloads. This will incur"
                + " additional charges in Spanner.")
    @Default.Boolean(false)
    ValueProvider<Boolean> getDataBoostEnabled();

    void setDataBoostEnabled(ValueProvider<Boolean> value);
  }

  /**
   * Runs a pipeline to export a Cloud Spanner database to Avro files.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {

    ExportPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(ExportPipelineOptions.class);

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
            .withRpcPriority(options.getSpannerPriority())
            .withDataBoostEnabled(options.getDataBoostEnabled());
    p.begin()
        .apply(
            "Run Export",
            new ExportTransform(
                spannerConfig,
                options.getOutputDir(),
                options.getTestJobId(),
                options.getSnapshotTime(),
                options.getTableNames(),
                options.getShouldExportRelatedTables(),
                options.getShouldExportTimestampAsLogicalType(),
                options.getAvroTempDirectory()));
    PipelineResult result = p.run();
    if (options.getWaitUntilFinish()
        &&
        /* Only if template location is null, there is a dataflow job to wait for. Else it's
         * template generation which doesn't start a dataflow job.
         */
        options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
      result.waitUntilFinish();
    }
  }
}
