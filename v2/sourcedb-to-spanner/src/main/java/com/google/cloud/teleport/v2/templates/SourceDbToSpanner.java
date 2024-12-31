/*
 * Copyright (C) 2024 Google LLC
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

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template that copies data from a relational database using JDBC to an existing Spanner
 * database.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/sourcedb-to-spanner/README_Sourcedb_to_Spanner_Flex.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Sourcedb_to_Spanner_Flex",
    category = TemplateCategory.BATCH,
    displayName = "Sourcedb to Spanner",
    description = {
      "The SourceDB to Spanner template is a batch pipeline that copies data from a relational"
          + " database into an existing Spanner database. This pipeline uses JDBC to connect to"
          + " the relational database. You can use this template to copy data from any relational"
          + " database with available JDBC drivers into Spanner. This currently only supports a limited set of types of MySQL",
      "For an extra layer of protection, you can also pass in a Cloud KMS key along with a"
          + " Base64-encoded username, password, and connection string parameters encrypted with"
          + " the Cloud KMS key. See the <a"
          + " href=\"https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt\">Cloud"
          + " KMS API encryption endpoint</a> for additional details on encrypting your username,"
          + " password, and connection string parameters."
    },
    optionsClass = SourceDbToSpannerOptions.class,
    flexContainerName = "source-db-to-spanner",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/sourcedb-to-spanner",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The JDBC drivers for the relational database must be available.",
      "The Spanner tables must exist before pipeline execution.",
      "The Spanner tables must have a compatible schema.",
      "The relational database must be accessible from the subnet where Dataflow runs."
    })
public class SourceDbToSpanner {

  private static final Logger LOG = LoggerFactory.getLogger(SourceDbToSpanner.class);

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link SourceDbToSpanner#run} method to start the
   * pipeline and invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    // Parse the user options passed from the command-line
    SourceDbToSpannerOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SourceDbToSpannerOptions.class);
    run(options);
  }

  /**
   * Create the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  @VisibleForTesting
  static PipelineResult run(SourceDbToSpannerOptions options) {
    // TODO - Validate if options are as expected
    Pipeline pipeline = Pipeline.create(options);

    SpannerConfig spannerConfig = createSpannerConfig(options);

    // Decide type and source of migration
    // TODO(vardhanvthigle): Move this within pipelineController.
    switch (options.getSourceDbDialect()) {
      case SourceDbToSpannerOptions.CASSANDRA_SOURCE_DIALECT:
        return PipelineController.executeCassandraMigration(options, pipeline, spannerConfig);
      default:
        /* Implementation detail, not having a default leads to failure in compile time checks enforced here */
        /* Making jdbc as default case which includes MYSQL and PG. */
        return executeJdbcMigration(options, pipeline, spannerConfig);
    }
  }

  // TODO(vardhanvthigle): Move this within pipelineController.
  private static PipelineResult executeJdbcMigration(
      SourceDbToSpannerOptions options, Pipeline pipeline, SpannerConfig spannerConfig) {
    if (options.getSourceConfigURL().startsWith("gs://")) {
      List<Shard> shards =
          new ShardFileReader(new SecretManagerAccessorImpl())
              .readForwardMigrationShardingConfig(options.getSourceConfigURL());
      return PipelineController.executeJdbcShardedMigration(
          options, pipeline, shards, spannerConfig);
    } else {
      return PipelineController.executeJdbcSingleInstanceMigration(
          options, pipeline, spannerConfig);
    }
  }

  @VisibleForTesting
  static SpannerConfig createSpannerConfig(SourceDbToSpannerOptions options) {
    return SpannerConfig.create()
        .withProjectId(ValueProvider.StaticValueProvider.of(options.getProjectId()))
        .withHost(ValueProvider.StaticValueProvider.of(options.getSpannerHost()))
        .withInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
        .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()));
  }
}
