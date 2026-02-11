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

import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.SOURCE_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.SPANNER_TAG;

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.fn.SchemaMapperProviderFn;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.transforms.MatchRecordsTransform;
import com.google.cloud.teleport.v2.transforms.ReportResultsTransform;
import com.google.cloud.teleport.v2.transforms.SourceReaderTransform;
import com.google.cloud.teleport.v2.transforms.SpannerInformationSchemaProcessorTransform;
import com.google.cloud.teleport.v2.transforms.SpannerReaderTransform;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Instant;

@Template(
    name = "GCS_Spanner_DV",
    category = TemplateCategory.BATCH,
    displayName = "GCS Spanner Data Validation",
    description =
        "Batch pipeline that reads data from GCS and Spanner compares them to validate migration correctness.",
    optionsClass = GCSSpannerDV.Options.class,
    flexContainerName = "gcs-spanner-dv",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/gcs-spanner-dv",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The GCS directory for AVRO files must exist before pipeline execution.",
      "The Spanner tables must exist before pipeline execution.",
      "The Spanner tables must have a compatible schema."
    })
public class GCSSpannerDV {

  public interface Options extends PipelineOptions {

    @TemplateParameter.GcsReadFolder(
        order = 1,
        optional = true,
        description = "GCS directory for AVRO files",
        helpText = "This directory is used to read the AVRO files of the records read from source.",
        example = "gs://your-bucket/your-path")
    String getGcsInputDirectory();

    void setGcsInputDirectory(String value);

    @TemplateParameter.ProjectId(
        order = 2,
        optional = true,
        description = "Cloud Spanner Project Id.",
        helpText = "This is the name of the Cloud Spanner project.")
    String getProjectId();

    void setProjectId(String projectId);

    @TemplateParameter.Text(
        order = 3,
        optional = true,
        description = "Cloud Spanner Endpoint to call",
        helpText = "The Cloud Spanner endpoint to call in the template.",
        example = "https://batch-spanner.googleapis.com")
    @Default.String("https://batch-spanner.googleapis.com")
    String getSpannerHost();

    void setSpannerHost(String value);

    @TemplateParameter.Text(
        order = 4,
        groupName = "Target",
        description = "Cloud Spanner Instance Id.",
        helpText = "The destination Cloud Spanner instance.")
    String getInstanceId();

    void setInstanceId(String value);

    @TemplateParameter.Text(
        order = 5,
        regexes = {"^[a-z]([a-z0-9_-]{0,28})[a-z0-9]$"},
        description = "Cloud Spanner Database Id.",
        helpText = "The destination Cloud Spanner database.")
    String getDatabaseId();

    void setDatabaseId(String value);

    @TemplateParameter.Enum(
        order = 6,
        enumOptions = {
          @TemplateParameter.TemplateEnumOption("LOW"),
          @TemplateParameter.TemplateEnumOption("MEDIUM"),
          @TemplateParameter.TemplateEnumOption("HIGH")
        },
        optional = true,
        description = "Priority for Spanner RPC invocations",
        helpText =
            "The request priority for Cloud Spanner calls. The value must be one of:"
                + " [`HIGH`,`MEDIUM`,`LOW`]. Defaults to `HIGH`.")
    @Default.Enum("HIGH")
    RpcPriority getSpannerPriority();

    void setSpannerPriority(RpcPriority value);

    @TemplateParameter.GcsReadFile(
        order = 7,
        optional = true,
        description =
            "Session File Path in Cloud Storage, to provide mapping information in the form of a session file",
        helpText =
            "Session file path in Cloud Storage that contains mapping information from"
                + " Spanner Migration Tool")
    @Default.String("")
    String getSessionFilePath();

    void setSessionFilePath(String value);

    @TemplateParameter.GcsReadFile(
        order = 8,
        optional = true,
        description = "File based overrides from source to spanner",
        helpText =
            "A file which specifies the table and the column name overrides from source to spanner.")
    @Default.String("")
    String getSchemaOverridesFilePath();

    void setSchemaOverridesFilePath(String value);

    @TemplateParameter.Text(
        order = 9,
        optional = true,
        description = "Table name overrides from source to spanner",
        regexes =
            "^\\[([[:space:]]*\\{[[:graph:]]+[[:space:]]*,[[:space:]]*[[:graph:]]+[[:space:]]*\\}[[:space:]]*(,[[:space:]]*)*)*\\]$",
        example = "[{Singers, Vocalists}, {Albums, Records}]",
        helpText =
            "These are the table name overrides from source to spanner. They are written in the"
                + "following format: [{SourceTableName1, SpannerTableName1}, {SourceTableName2, SpannerTableName2}]"
                + "This example shows mapping Singers table to Vocalists and Albums table to Records.")
    @Default.String("")
    String getTableOverrides();

    void setTableOverrides(String value);

    @TemplateParameter.Text(
        order = 10,
        optional = true,
        regexes =
            "^\\[([[:space:]]*\\{[[:space:]]*[[:graph:]]+\\.[[:graph:]]+[[:space:]]*,[[:space:]]*[[:graph:]]+\\.[[:graph:]]+[[:space:]]*\\}[[:space:]]*(,[[:space:]]*)*)*\\]$",
        description = "Column name overrides from source to spanner",
        example =
            "[{Singers.SingerName, Singers.TalentName}, {Albums.AlbumName, Albums.RecordName}]",
        helpText =
            "These are the column name overrides from source to spanner. They are written in the"
                + "following format: [{SourceTableName1.SourceColumnName1, SourceTableName1.SpannerColumnName1}, {SourceTableName2.SourceColumnName1, SourceTableName2.SpannerColumnName1}]"
                + "Note that the SourceTableName should remain the same in both the source and spanner pair. To override table names, use tableOverrides."
                + "The example shows mapping SingerName to TalentName and AlbumName to RecordName in Singers and Albums table respectively.")
    @Default.String("")
    String getColumnOverrides();

    void setColumnOverrides(String value);

    @TemplateParameter.Text(
        order = 11,
        optional = false,
        regexes = {"^[^ ;]*$"},
        description = "BigQuery dataset for reporting",
        helpText = "The BigQuery dataset ID where the validation results will be stored.",
        example = "validation_report_dataset")
    String getBigQueryDataset();

    void setBigQueryDataset(String value);

    @TemplateParameter.Text(
        order = 12,
        optional = true,
        regexes = {"^[^ ;]*$"},
        description = "Run ID for the validation job",
        helpText =
            "A unique identifier for the validation run. If not provided, the Dataflow Job Name will be used.",
        example = "run_20230101_120000")
    String getRunId();

    void setRunId(String value);
  }

  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    run(options);
  }

  public static PipelineResult run(Options options) {
    Pipeline pipeline = Pipeline.create(options);

    SpannerConfig spannerConfig = createSpannerConfig(options);

    // Fetch Spanner DDL using Info schema
    final PCollectionView<Ddl> ddlView =
        pipeline.apply(
            "ReadSpannerInformationSchema",
            new SpannerInformationSchemaProcessorTransform(spannerConfig));

    // Get Schema mapper provider, we get Ddl from a side input
    // so the mapper has to be initialized lazily
    SerializableFunction<Ddl, ISchemaMapper> schemaMapperProvider =
        new SchemaMapperProviderFn(
            options.getSessionFilePath(),
            options.getSchemaOverridesFilePath(),
            options.getTableOverrides(),
            options.getColumnOverrides());

    // Get Source records hashes
    PCollection<ComparisonRecord> sourceRecords =
        pipeline.apply(
            "ReadSourceRecords",
            new SourceReaderTransform(
                options.getGcsInputDirectory(), ddlView, schemaMapperProvider));

    // Get Spanner records hashes
    PCollection<ComparisonRecord> spannerRecords =
        pipeline.apply(
            "ReadSpannerRecords",
            new SpannerReaderTransform(spannerConfig, ddlView, schemaMapperProvider));

    PCollectionTuple inputs =
        PCollectionTuple.of(SOURCE_TAG, sourceRecords).and(SPANNER_TAG, spannerRecords);

    // Match records to determine equivalence
    PCollectionTuple matchResults = inputs.apply("MatchRecords", new MatchRecordsTransform());

    // Report results of the validation
    Instant startTimestamp = Instant.now();
    String runId = options.getRunId();
    if (runId == null) {
      runId = String.format("%s_%s", options.getJobName(), startTimestamp);
    }

    matchResults.apply(
        "ReportResults",
        new ReportResultsTransform(options.getBigQueryDataset(), runId, startTimestamp));

    return pipeline.run();
  }

  @VisibleForTesting
  static SpannerConfig createSpannerConfig(Options options) {
    return SpannerConfig.create()
        .withProjectId(ValueProvider.StaticValueProvider.of(options.getProjectId()))
        .withHost(ValueProvider.StaticValueProvider.of(options.getSpannerHost()))
        .withInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
        .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()))
        .withRpcPriority(ValueProvider.StaticValueProvider.of(options.getSpannerPriority()));
  }
}
