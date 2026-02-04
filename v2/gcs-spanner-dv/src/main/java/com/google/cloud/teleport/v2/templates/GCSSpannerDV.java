/*
 * Copyright (C) 2026 Google Inc.
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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MATCHED_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MISSING_IN_SOURCE_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MISSING_IN_SPANNER_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.SOURCE_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.SPANNER_TAG;

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.transforms.MatchRecordsTransform;
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
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Template(
    name = "GCS_Spanner_DV",
    category = TemplateCategory.BATCH,
    displayName = "GCS Spanner Data Validation",
    description = "Batch pipeline that reads data from GCS and Spanner compares them to validate migration correctness.",
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
    }
)
public class GCSSpannerDV {

  private static final Logger LOG = LoggerFactory.getLogger(GCSSpannerDV.class);

  public interface Options extends PipelineOptions {

    @TemplateParameter.GcsReadFolder(
        order = 1,
        optional = true,
        description = "GCS directory for AVRO files",
        helpText = "This directory is used to read the AVRO files of the records read from source.",
        example = "gs://your-bucket/your-path"
    )
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
  }

  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    run(options);
  }

  public static PipelineResult run(Options options) {
    Pipeline pipeline = Pipeline.create(options);

    // Get Source records hashes
    PCollection<ComparisonRecord> sourceRecords = pipeline.apply("ReadSourceRecords",
        new SourceReaderTransform(options.getGcsInputDirectory())
    );

    SpannerConfig spannerConfig = createSpannerConfig(options);

    // Fetch Spanner DDL using Info schema
    final PCollectionView<Ddl> ddlView =
        pipeline.apply(
                    "ReadSpannerInformationSchema",
                    new SpannerInformationSchemaProcessorTransform(
                            spannerConfig));

    // Get Spanner records hashes
    PCollection<ComparisonRecord> spannerRecords = pipeline
        .apply("ReadSpannerRecords", new SpannerReaderTransform(spannerConfig, ddlView));

    PCollectionTuple inputs = PCollectionTuple.of(SOURCE_TAG, sourceRecords)
            .and(SPANNER_TAG, spannerRecords);

    // Match records to determine equivalence
    PCollectionTuple matchResults = inputs.apply("MatchRecords", new MatchRecordsTransform());

    matchResults.get(MATCHED_TAG)
            .apply("ExtractTableNameMatched", MapElements.into(TypeDescriptors.strings())
                    .via(ComparisonRecord::getTableName))
            .apply("CountMatched", Count.perElement())
            .apply("LogMatched", ParDo.of(new LogCountFn("Matched")));

    matchResults.get(MISSING_IN_SPANNER_TAG)
            .apply("ExtractTableNameMissingInSpanner", MapElements.into(TypeDescriptors.strings())
                    .via(ComparisonRecord::getTableName))
            .apply("CountMissingInSpanner", Count.perElement())
            .apply("LogMissingInSpanner", ParDo.of(new LogCountFn("MissingInSpanner")));

    matchResults.get(MISSING_IN_SOURCE_TAG)
            .apply("ExtractTableNameMissingInSource", MapElements.into(TypeDescriptors.strings())
                    .via(ComparisonRecord::getTableName))
            .apply("CountMissingInSource", Count.perElement())
            .apply("LogMissingInSource", ParDo.of(new LogCountFn("MissingInSource")));

    return pipeline.run();
  }

  static class LogCountFn extends DoFn<KV<String, Long>, Void> {

      private final String label;

      public LogCountFn(String label) {
          this.label = label;
      }

      @ProcessElement
      public void processElement(ProcessContext c) {
          LOG.info("{}: {} - {}", label, c.element().getKey(), c.element().getValue());
      }
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
