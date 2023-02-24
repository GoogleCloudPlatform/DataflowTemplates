/*
 * Copyright (C) 2021 Google LLC
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

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.templates.BigQueryToBigtable.BigQueryToBigtableOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.AvroToMutation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Dataflow template which reads BigQuery data and writes it to Bigtable. The source data can be
 * either a BigQuery table or an SQL query.
 */
@Template(
    name = "BigQuery_to_Bigtable",
    category = TemplateCategory.BATCH,
    displayName = "BigQuery to Bigtable",
    description = "A pipeline to export a BigQuery table into Bigtable.",
    optionsClass = BigQueryToBigtableOptions.class,
    flexContainerName = "bigquery-to-bigtable",
    contactInformation = "https://cloud.google.com/support")
public class BigQueryToBigtable {

  /**
   * The {@link BigQueryToBigtableOptions} class provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface BigQueryToBigtableOptions extends PipelineOptions {

    @TemplateParameter.Text(
        order = 1,
        description = "Input SQL query",
        helpText = "SQL query in standard SQL to pull data from BigQuery")
    String getReadQuery();

    void setReadQuery(String value);

    @TemplateParameter.Text(
        order = 2,
        regexes = {"[A-Za-z_][A-Za-z_0-9]*"},
        description = "Unique identifier column",
        helpText = "Name of the BigQuery column storing the unique identifier of the row")
    String getReadIdColumn();

    void setReadIdColumn(String value);

    @TemplateParameter.ProjectId(
        order = 3,
        description = "Project ID",
        helpText =
            "The ID of the Google Cloud project of the Cloud Bigtable instance that you want to"
                + " write data to")
    @Required
    String getBigtableWriteProjectId();

    void setBigtableWriteProjectId(String value);

    @TemplateParameter.Text(
        order = 4,
        regexes = {"[a-z][a-z0-9\\-]+[a-z0-9]"},
        description = "Instance ID",
        helpText = "The ID of the Cloud Bigtable instance that contains the table")
    @Required
    String getBigtableWriteInstanceId();

    void setBigtableWriteInstanceId(String value);

    @TemplateParameter.Text(
        order = 5,
        regexes = {"[a-z][a-z0-9\\-]+[a-z0-9]"},
        description = "Bigtable App Profile",
        helpText = "Bigtable App Profile to use for the export")
    @Default.String("default")
    String getBigtableWriteAppProfile();

    void setBigtableWriteAppProfile(String value);

    @TemplateParameter.Text(
        order = 6,
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Table ID",
        helpText = "The ID of the Cloud Bigtable table to write")
    @Required
    String getBigtableWriteTableId();

    void setBigtableWriteTableId(String value);

    @TemplateParameter.Text(
        order = 8,
        optional = true,
        regexes = {"[-_.a-zA-Z0-9]+"},
        description = "The Bigtable Column Family",
        helpText = "This specifies the column family to write data into")
    @Required
    String getBigtableWriteColumnFamily();

    void setBigtableWriteColumnFamily(String value);

    @TemplateParameter.Integer(
        order = 9,
        optional = true,
        description = "Bigtable's latency target in milliseconds for latency-based throttling",
        helpText = "This enables latency-based throttling and specifies the target latency")
    Integer getBigtableBulkWriteLatencyTargetMs();

    void setBigtableBulkWriteLatencyTargetMs(Integer value);

    @TemplateParameter.Integer(
        order = 10,
        optional = true,
        description = "The max number of row keys in a Bigtable batch write operation",
        helpText = "This sets the max number of row keys in a Bigtable batch write operation")
    Integer getBigtableBulkWriteMaxRowKeyCount();

    void setBigtableBulkWriteMaxRowKeyCount(Integer value);

    @TemplateParameter.Integer(
        order = 11,
        optional = true,
        description = "The max amount of bytes in a Bigtable batch write operation",
        helpText = "This sets the max amount of bytes in a Bigtable batch write operation")
    Integer getBigtableBulkWriteMaxRequestSizeBytes();

    void setBigtableBulkWriteMaxRequestSizeBytes(Integer value);

    @TemplateParameter.Integer(
        order = 12,
        optional = true,
        description = "The timeout for an RPC attempt in milliseconds",
        helpText = "This sets the timeout for an RPC attempt in milliseconds")
    Integer getBigtableRpcAttemptTimeoutMs();

    void setBigtableRpcAttemptTimeoutMs(Integer value);

    @TemplateParameter.Integer(
        order = 13,
        optional = true,
        description = "The total timeout for an RPC operation in milliseconds",
        helpText = "This sets the total timeout for an RPC operation in milliseconds")
    Integer getBigtableRpcTimeoutMs();

    void setBigtableRpcTimeoutMs(Integer value);

    @TemplateParameter.Text(
        order = 14,
        optional = true,
        description = "The additional retry codes",
        helpText = "This sets the additional retry codes, separated by ','",
        example = "RESOURCE_EXHAUSTED,DEADLINE_EXCEEDED")
    String getBigtableAdditionalRetryCodes();

    void setBigtableAdditionalRetryCodes(String value);
  }

  /**
   * Runs a pipeline which reads data from BigQuery and writes it to Bigtable.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    BigQueryToBigtableOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryToBigtableOptions.class);
    CloudBigtableTableConfiguration.Builder builderBigtableTableConfig =
        new CloudBigtableTableConfiguration.Builder()
            .withProjectId(options.getBigtableWriteProjectId())
            .withInstanceId(options.getBigtableWriteInstanceId())
            .withTableId(options.getBigtableWriteTableId())
            .withAppProfileId(options.getBigtableWriteAppProfile())
            .withConfiguration(BigtableOptionsFactory.INITIAL_ELAPSED_BACKOFF_MILLIS_KEY, "100")
            .withConfiguration(BigtableOptionsFactory.MAX_ELAPSED_BACKOFF_MILLIS_KEY, "600000");

    if (options.getBigtableBulkWriteLatencyTargetMs() != null) {
      builderBigtableTableConfig
          .withConfiguration(
              BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING, "true")
          .withConfiguration(
              BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_THROTTLING_THRESHOLD_MILLIS,
              String.valueOf(options.getBigtableBulkWriteLatencyTargetMs()));
    }
    if (options.getBigtableBulkWriteMaxRowKeyCount() != null) {
      builderBigtableTableConfig.withConfiguration(
          BigtableOptionsFactory.BIGTABLE_BULK_MAX_ROW_KEY_COUNT,
          String.valueOf(options.getBigtableBulkWriteMaxRowKeyCount()));
    }
    if (options.getBigtableBulkWriteMaxRequestSizeBytes() != null) {
      builderBigtableTableConfig.withConfiguration(
          BigtableOptionsFactory.BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES,
          String.valueOf(options.getBigtableBulkWriteMaxRequestSizeBytes()));
    }
    if (options.getBigtableRpcTimeoutMs() != null) {
      builderBigtableTableConfig.withConfiguration(
          BigtableOptionsFactory.BIGTABLE_RPC_TIMEOUT_MS_KEY,
          String.valueOf(options.getBigtableRpcTimeoutMs()));
    }
    if (options.getBigtableRpcAttemptTimeoutMs() != null) {
      builderBigtableTableConfig.withConfiguration(
          BigtableOptionsFactory.BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS_KEY,
          String.valueOf(options.getBigtableRpcAttemptTimeoutMs()));
    }

    if (options.getBigtableAdditionalRetryCodes() != null) {
      builderBigtableTableConfig.withConfiguration(
          BigtableOptionsFactory.ADDITIONAL_RETRY_CODES, options.getBigtableAdditionalRetryCodes());
    }

    CloudBigtableTableConfiguration bigtableTableConfig = builderBigtableTableConfig.build();

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(
            "AvroToMutation",
            BigQueryIO.read(
                    AvroToMutation.newBuilder()
                        .setColumnFamily(options.getBigtableWriteColumnFamily())
                        .setRowkey(options.getReadIdColumn())
                        .build())
                .fromQuery(options.getReadQuery())
                .withoutValidation()
                .withTemplateCompatibility()
                .usingStandardSql())
        .apply("WriteToTable", CloudBigtableIO.writeToTable(bigtableTableConfig));

    pipeline.run();
  }
}
