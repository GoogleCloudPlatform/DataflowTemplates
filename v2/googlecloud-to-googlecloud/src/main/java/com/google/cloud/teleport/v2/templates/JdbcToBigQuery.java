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

import static com.google.cloud.teleport.v2.utils.KMSUtils.maybeDecrypt;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.JdbcToBigQueryOptions;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import com.google.cloud.teleport.v2.utils.JdbcConverters;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;

/**
 * A template that copies data from a relational database using JDBC to an existing BigQuery table.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-googlecloud/README_Jdbc_to_BigQuery_Flex.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Jdbc_to_BigQuery_Flex",
    category = TemplateCategory.BATCH,
    displayName = "JDBC to BigQuery with BigQuery Storage API support",
    description =
        "A pipeline that reads from a JDBC source and writes to a BigQuery table. JDBC connection"
            + " string, user name and password can be passed in directly as plaintext or encrypted"
            + " using the Google Cloud KMS API.  If the parameter KMSEncryptionKey is specified,"
            + " connectionURL, username, and password should be all in encrypted format.",
    additionalHelp =
        "A sample curl command for the KMS API encrypt endpoint: curl -s -X POST"
            + " \"https://cloudkms.googleapis.com/v1/projects/your-project/locations/your-path/keyRings/your-keyring/cryptoKeys/your-key:encrypt\""
            + "  -d \"{\\\"plaintext\\\":\\\"PasteBase64EncodedString\\\"}\" -H \"Authorization:"
            + " Bearer $(gcloud auth application-default print-access-token)\" -H \"Content-Type:"
            + " application/json\"",
    optionsClass = JdbcToBigQueryOptions.class,
    flexContainerName = "jdbc-to-bigquery",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/jdbc-to-bigquery",
    contactInformation = "https://cloud.google.com/support")
public class JdbcToBigQuery {

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link JdbcToBigQuery#run} method to start the pipeline
   * and invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

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
    BigQueryIOUtils.validateBQStorageApiOptionsBatch(options);

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    /*
     * Steps: 1) Read records via JDBC and convert to TableRow via RowMapper
     *        2) Append TableRow to BigQuery via BigQueryIO
     */
    JdbcIO.DataSourceConfiguration dataSourceConfiguration =
        JdbcIO.DataSourceConfiguration.create(
                options.getDriverClassName(),
                maybeDecrypt(options.getConnectionURL(), options.getKMSEncryptionKey()))
            .withUsername(maybeDecrypt(options.getUsername(), options.getKMSEncryptionKey()))
            .withPassword(maybeDecrypt(options.getPassword(), options.getKMSEncryptionKey()))
            .withDriverJars(options.getDriverJars());

    if (options.getConnectionProperties() != null) {
      dataSourceConfiguration =
          dataSourceConfiguration.withConnectionProperties(options.getConnectionProperties());
    }
    pipeline
        /*
         * Step 1: Read records via JDBC and convert to TableRow
         *         via {@link org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper}
         */
        .apply(
            "Read from JdbcIO",
            JdbcIO.<TableRow>read()
                .withDataSourceConfiguration(dataSourceConfiguration)
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

  /**
   * Create the {@link Write} transform that outputs the collection to BigQuery as per input option.
   */
  @VisibleForTesting
  static Write<TableRow> writeToBQTransform(JdbcToBigQueryOptions options) {
    return BigQueryIO.writeTableRows()
        .withoutValidation()
        .withCreateDisposition(Write.CreateDisposition.CREATE_NEVER)
        .withWriteDisposition(
            options.getIsTruncate()
                ? Write.WriteDisposition.WRITE_TRUNCATE
                : Write.WriteDisposition.WRITE_APPEND)
        .withCustomGcsTempLocation(
            StaticValueProvider.of(options.getBigQueryLoadingTemporaryDirectory()))
        .to(options.getOutputTable());
  }
}
