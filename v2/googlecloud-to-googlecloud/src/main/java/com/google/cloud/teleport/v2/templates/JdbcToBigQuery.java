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
package com.google.cloud.teleport.v2.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.io.DynamicJdbcIO;
import com.google.cloud.teleport.v2.io.DynamicJdbcIO.DynamicDataSourceConfiguration;
import com.google.cloud.teleport.v2.options.JdbcToBigQueryOptions;
import com.google.cloud.teleport.v2.utils.JdbcConverters;
import com.google.cloud.teleport.v2.utils.KMSEncryptedNestedValue;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;

/**
 * A template that copies data from a relational database using JDBC to an existing BigQuery table.
 */
public class JdbcToBigQuery {

  private static KMSEncryptedNestedValue maybeDecrypt(String unencryptedValue, String kmsKey) {
    return new KMSEncryptedNestedValue(unencryptedValue, kmsKey);
  }

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link JdbcToBigQuery#run} method to start the pipeline
   * and invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    // Parse the user options passed from the command-line
    JdbcToBigQueryOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(JdbcToBigQueryOptions.class);

    run(options, writeToBQTransform(options));
  }

  /**
   * Create the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @param writeToBQ the transform that outputs {@link TableRow}s to BigQuery.
   * @return The result of the pipeline execution.
   */
  @VisibleForTesting
  static PipelineResult run(JdbcToBigQueryOptions options, Write<TableRow> writeToBQ) {
    // Validate BQ STORAGE_WRITE_API options
    if (options.getUseStorageWriteApiAtLeastOnce() && !options.getUseStorageWriteApi()) {
      // Technically this is a no-op, since useStorageWriteApiAtLeastOnce is only checked by
      // BigQueryIO when useStorageWriteApi is true, but it might be confusing to a user why
      // useStorageWriteApiAtLeastOnce doesn't take effect.
      throw new IllegalArgumentException(
          "When at-least-once semantics (useStorageWriteApiAtLeastOnce) are enabled Storage Write"
              + " API (useStorageWriteApi) must also be enabled.");
    }

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    /*
     * Steps: 1) Read records via JDBC and convert to TableRow via RowMapper
     *        2) Append TableRow to BigQuery via BigQueryIO
     */
    pipeline
        /*
         * Step 1: Read records via JDBC and convert to TableRow
         *         via {@link org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper}
         */
        .apply(
            "Read from JdbcIO",
            DynamicJdbcIO.<TableRow>read()
                .withDataSourceConfiguration(
                    DynamicDataSourceConfiguration.create(
                            options.getDriverClassName(),
                            maybeDecrypt(options.getConnectionURL(), options.getKMSEncryptionKey()))
                        .withUsername(
                            maybeDecrypt(options.getUsername(), options.getKMSEncryptionKey()))
                        .withPassword(
                            maybeDecrypt(options.getPassword(), options.getKMSEncryptionKey()))
                        .withDriverJars(options.getDriverJars())
                        .withConnectionProperties(options.getConnectionProperties()))
                .withQuery(options.getQuery())
                .withCoder(TableRowJsonCoder.of())
                .withRowMapper(JdbcConverters.getResultSetToTableRow(options.getUseColumnAlias())))
        /*
         * Step 2: Append TableRow to an existing BigQuery table
         */
        .apply("Write to BigQuery", writeToBQ);

    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  /** Create the {@link Write} transform that outputs the collection to BigQuery. */
  @VisibleForTesting
  static Write<TableRow> writeToBQTransform(JdbcToBigQueryOptions options) {
    return BigQueryIO.writeTableRows()
        .withoutValidation()
        .withCreateDisposition(Write.CreateDisposition.CREATE_NEVER)
        .withWriteDisposition(Write.WriteDisposition.WRITE_APPEND)
        .withCustomGcsTempLocation(
            StaticValueProvider.of(options.getBigQueryLoadingTemporaryDirectory()))
        .to(options.getOutputTable());
  }
}
