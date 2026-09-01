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
import com.google.cloud.teleport.v2.options.GCSSpannerDVOptions;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.fn.SchemaMapperProviderFn;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
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
    name = "GCS_Spanner_Data_Validator",
    category = TemplateCategory.BATCH,
    displayName = "GCS Spanner Data Validation",
    description =
        "Batch pipeline that reads data from GCS and Spanner compares them to validate migration"
            + " correctness.",
    optionsClass = GCSSpannerDVOptions.class,
    flexContainerName = "gcs-spanner-dv",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/gcs-spanner-dv",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The GCS directory for AVRO files must exist before pipeline execution.",
      "The Spanner tables must exist before pipeline execution.",
      "The Spanner tables must have a compatible schema (either directly or schema mapping)."
    })
public class GCSSpannerDV {

  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    GCSSpannerDVOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GCSSpannerDVOptions.class);
    run(options);
  }

  public static PipelineResult run(GCSSpannerDVOptions options) {
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

    CustomTransformation customTransformation =
        CustomTransformation.builder(
                options.getTransformationJarPath(), options.getTransformationClassName())
            .setCustomParameters(options.getTransformationCustomParameters())
            .build();

    // Get Source records hashes
    PCollection<ComparisonRecord> sourceRecords =
        pipeline.apply(
            "ReadSourceRecords",
            new SourceReaderTransform(
                options.getGcsInputDirectory(),
                ddlView,
                schemaMapperProvider,
                customTransformation));

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
  static SpannerConfig createSpannerConfig(GCSSpannerDVOptions options) {
    return SpannerConfig.create()
        .withProjectId(ValueProvider.StaticValueProvider.of(options.getProjectId()))
        .withHost(ValueProvider.StaticValueProvider.of(options.getSpannerHost()))
        .withInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
        .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()))
        .withRpcPriority(ValueProvider.StaticValueProvider.of(options.getSpannerPriority()));
  }
}
