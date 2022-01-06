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

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;

/** Dataflow template that exports a Cloud Spanner database to Avro files in GCS. */
public class ExportPipeline {

  /** Options for Export pipeline. */
  public interface ExportPipelineOptions extends PipelineOptions {
    @Description("Instance ID for Cloud Spanner")
    ValueProvider<String> getInstanceId();

    void setInstanceId(ValueProvider<String> value);

    @Description("Database ID for Cloud Spanner")
    ValueProvider<String> getDatabaseId();

    void setDatabaseId(ValueProvider<String> value);

    @Description("Output directory in GCS")
    ValueProvider<String> getOutputDir();

    void setOutputDir(ValueProvider<String> value);

    @Description("Temporary Directory to store Avro files.")
    ValueProvider<String> getAvroTempDirectory();

    void setAvroTempDirectory(ValueProvider<String> value);

    @Description("Test dataflow job identifier for Beam Direct Runner")
    @Default.String(value = "")
    ValueProvider<String> getTestJobId();

    void setTestJobId(ValueProvider<String> jobId);

    @Description("Cloud Spanner API endpoint")
    @Default.String("https://batch-spanner.googleapis.com")
    ValueProvider<String> getSpannerHost();

    void setSpannerHost(ValueProvider<String> value);

    @Description("If true, wait for job finish")
    @Default.Boolean(true)
    boolean getWaitUntilFinish();

    void setWaitUntilFinish(boolean value);

    @Description(
        "If set, specifies the time when the snapshot must be taken."
            + " String is in the RFC 3339 format. "
            + " Example - 1990-12-31T23:59:60Z"
            + " Timestamp must be in the past and Maximum timestamp staleness applies."
            + " https://cloud.google.com/spanner/docs/timestamp-bounds#maximum_timestamp_staleness")
    @Default.String(value = "")
    ValueProvider<String> getSnapshotTime();

    void setSnapshotTime(ValueProvider<String> value);

    @Description("GCP Project Id of where the Spanner table lives.")
    ValueProvider<String> getSpannerProjectId();

    void setSpannerProjectId(ValueProvider<String> value);

    @Description(
        "If true, Timestamps are exported with timestamp-micros logical type."
            + "By default, Timestamps are exported as ISO8601 strings at nanosecond precision.")
    @Default.Boolean(false)
    ValueProvider<Boolean> getShouldExportTimestampAsLogicalType();

    void setShouldExportTimestampAsLogicalType(ValueProvider<Boolean> value);

    @Description(
        "If provided, these tables (along with any necessary parent or foreign key"
            + " tables) are exported from Cloud Spanner.")
    @Default.String(value = "")
    ValueProvider<String> getTableNames();

    void setTableNames(ValueProvider<String> value);

    @Description(
        "If true, export not only the specified list of tables, but any related tables necessary"
            + " for the export, such as interleaved parent tables and foreign keys tables")
    @Default.Boolean(false)
    ValueProvider<Boolean> getShouldExportRelatedTables();

    void setShouldExportRelatedTables(ValueProvider<Boolean> value);
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
            .withProjectId(options.getSpannerProjectId())
            .withHost(options.getSpannerHost())
            .withInstanceId(options.getInstanceId())
            .withDatabaseId(options.getDatabaseId());
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
