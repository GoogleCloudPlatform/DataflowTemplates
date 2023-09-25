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
import org.apache.beam.sdk.values.PCollection;

/**
 * A template that copies data from a relational database using JDBC to an existing BigQuery table.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/jdbc-to-googlecloud/README_Jdbc_to_BigQuery_Flex.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Jdbc_to_BigQuery_Flex",
    category = TemplateCategory.BATCH,
    displayName = "JDBC to BigQuery with BigQuery Storage API support",
    description = {
      "The JDBC to BigQuery template is a batch pipeline that copies data from a relational database table into an existing BigQuery table. "
          + "This pipeline uses JDBC to connect to the relational database. You can use this template to copy data from any relational database with available JDBC drivers into BigQuery.",
      "For an extra layer of protection, you can also pass in a Cloud KMS key along with a Base64-encoded username, password, and connection string parameters encrypted with the Cloud KMS key. "
          + "See the <a href=\"https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt\">Cloud KMS API encryption endpoint</a> for additional details on encrypting your username, password, and connection string parameters."
    },
    optionsClass = JdbcToBigQueryOptions.class,
    flexContainerName = "jdbc-to-bigquery",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/jdbc-to-bigquery",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The JDBC drivers for the relational database must be available.",
      "The BigQuery table must exist before pipeline execution.",
      "The BigQuery table must have a compatible schema.",
      "The relational database must be accessible from the subnet where Dataflow runs."
    })
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
                StaticValueProvider.of(options.getDriverClassName()),
                maybeDecrypt(options.getConnectionURL(), options.getKMSEncryptionKey()))
            .withUsername(maybeDecrypt(options.getUsername(), options.getKMSEncryptionKey()))
            .withPassword(maybeDecrypt(options.getPassword(), options.getKMSEncryptionKey()));

    if (options.getDriverJars() != null) {
      dataSourceConfiguration = dataSourceConfiguration.withDriverJars(options.getDriverJars());
    }

    if (options.getConnectionProperties() != null) {
      dataSourceConfiguration =
          dataSourceConfiguration.withConnectionProperties(options.getConnectionProperties());
    }

    /*
     * Step 1: Read records via JDBC and convert to TableRow
     *         via {@link org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper}
     */
    PCollection<TableRow> rows;
    if (options.getPartitionColumn() != null && options.getTable() != null) {
      // Read with Partitions
      // TODO(pranavbhandari): Support readWithPartitions for other data types.
      JdbcIO.ReadWithPartitions<TableRow, Long> readIO =
          JdbcIO.<TableRow>readWithPartitions()
              .withDataSourceConfiguration(dataSourceConfiguration)
              .withTable(options.getTable())
              .withPartitionColumn(options.getPartitionColumn())
              .withRowMapper(JdbcConverters.getResultSetToTableRow(options.getUseColumnAlias()));
      if (options.getNumPartitions() != null) {
        readIO = readIO.withNumPartitions(options.getNumPartitions());
      }
      if (options.getLowerBound() != null && options.getUpperBound() != null) {
        readIO =
            readIO.withLowerBound(options.getLowerBound()).withUpperBound(options.getUpperBound());
      }
      rows = pipeline.apply("Read from JDBC with Partitions", readIO);
    } else {
      rows =
          pipeline.apply(
              "Read from JdbcIO",
              JdbcIO.<TableRow>read()
                  .withDataSourceConfiguration(dataSourceConfiguration)
                  .withQuery(options.getQuery())
                  .withCoder(TableRowJsonCoder.of())
                  .withRowMapper(
                      JdbcConverters.getResultSetToTableRow(options.getUseColumnAlias())));
    }

    /*
     * Step 2: Append TableRow to an existing BigQuery table
     */
    rows.apply("Write to BigQuery", writeToBQ);

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
