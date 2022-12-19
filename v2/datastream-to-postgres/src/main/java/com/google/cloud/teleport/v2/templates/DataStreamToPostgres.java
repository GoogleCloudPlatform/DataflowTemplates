/*
 * Copyright (C) 2020 Google LLC
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

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.cdc.sources.DataStreamIO;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.io.CdcJdbcIO;
import com.google.cloud.teleport.v2.transforms.CreateDml;
import com.google.cloud.teleport.v2.transforms.ProcessDml;
import com.google.cloud.teleport.v2.values.DmlInfo;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.sql.SQLException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests DataStream data from GCS. The data is then cleaned and validated against a
 * BigQuery Table. If new columns or tables appear, they are automatically added to BigQuery. The
 * data is then inserted into BigQuery staging tables and Merged into a final replica table.
 *
 * <p>NOTE: Future version will support: Pub/Sub, GCS, or Kafka as per DataStream
 *
 * <p>Example Usage: TODO: FIX EXAMPLE USAGE
 *
 * <pre>
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.${PIPELINE_NAME} \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
 * --tempLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
 * --templateLocation=gs://$BUCKET_NAME/templates/${PIPELINE_NAME}.json \
 * --inputFilePattern=${GCS_AVRO_FILE_PATTERN} \
 * --outputProjectId=${OUTPUT_PROJECT_ID} \
 * --outputStagingDatasetTemplate=${BQ_STAGING_DATASET_TEMPLATE} \
 * --outputStagingTableNameTemplate=${BQ_STAGING_TABLE_NAME_TEMPLATE} \
 * --outputDatasetTemplate=${BQ_DATASET_TEMPLATE} \
 * --outputTableNameTemplate=${BQ_TABLE_NAME_TEMPLATE} \
 * --deadLetterQueueDirectory=${DLQ_GCS_PATH} \
 * --runner=(DirectRunner|DataflowRunner)"
 *
 * OPTIONAL Dataflow Params:
 * --autoscalingAlgorithm=THROUGHPUT_BASED \
 * --numWorkers=2 \
 * --maxNumWorkers=10 \
 * --workerMachineType=n1-highcpu-4 \
 * </pre>
 */
public class DataStreamToPostgres {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToPostgres.class);
  private static final String AVRO_SUFFIX = "avro";
  private static final String JSON_SUFFIX = "json";

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, StreamingOptions {
    @TemplateParameter.GcsReadFile(
        order = 1,
        description = "Cloud Storage Input File(s)",
        helpText = "Path of the file pattern glob to read from.",
        example = "gs://your-bucket/path/*.avro")
    String getInputFilePattern();

    void setInputFilePattern(String value);

    @Description(
        "The Pub/Sub subscription with DataStream file notifications."
            + "The name should be in the format of "
            + "projects/<project-id>/subscriptions/<subscription-name>.")
    String getGcsPubSubSubscription();

    void setGcsPubSubSubscription(String value);

    @Description("The GCS output format avro/json")
    @Default.String("avro")
    String getInputFileFormat();

    void setInputFileFormat(String value);

    @Description("The DataStream Stream to Reference.")
    String getStreamName();

    void setStreamName(String value);

    @TemplateParameter.DateTime(
        order = 2,
        optional = true,
        description =
            "The starting DateTime used to fetch from Cloud Storage "
                + "(https://tools.ietf.org/html/rfc3339).",
        helpText =
            "The starting DateTime used to fetch from Cloud Storage "
                + "(https://tools.ietf.org/html/rfc3339).")
    @Default.String("1970-01-01T00:00:00.00Z")
    String getRfcStartDateTime();

    void setRfcStartDateTime(String value);

    @Description("DataStream API Root Url (only used for testing)")
    @Default.String("https://datastream.googleapis.com/")
    String getDataStreamRootUrl();

    void setDataStreamRootUrl(String value);

    // Postgres Connection Parameters
    @Description("Postgres Database Host")
    String getDatabaseHost();

    void setDatabaseHost(String value);

    @Description("Postgres Database Port")
    @Default.String("5432")
    String getDatabasePort();

    void setDatabasePort(String value);

    @Description("Database User")
    @Default.String("postgres")
    String getDatabaseUser();

    void setDatabaseUser(String value);

    @Description("Database Password")
    @Default.String("postgres")
    String getDatabasePassword();

    void setDatabasePassword(String value);

    @Description("Database Name")
    @Default.String("postgres")
    String getDatabaseName();

    void setDatabaseName(String value);
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    LOG.info("Starting Avro Python to BigQuery");

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    options.setStreaming(true);
    run(options);
  }

  /**
   * Validate the options supplied match expected values. We will also validate that connectivity is
   * working correctly for Postgres.
   *
   * @param options The execution parameters to the pipeline.
   * @param dataSourceConfiguration The Postgres datasource configuration.
   */
  public static void validateOptions(
      Options options, CdcJdbcIO.DataSourceConfiguration dataSourceConfiguration) {
    try {
      if (options.getDatabaseHost() != null) {
        dataSourceConfiguration.buildDatasource().getConnection().close();
      }
    } catch (SQLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {
    /*
     * Stages:
     *   1) Ingest and Normalize Data to FailsafeElement with JSON Strings
     *   2) Write JSON Strings to Postgres DML Objects
     *   3) Filter stale rows using stateful PK transform
     *   4) Write DML statements to Postgres
     */

    Pipeline pipeline = Pipeline.create(options);

    String jdbcDriverConnectionString =
        String.format(
            "jdbc:postgresql://%s:%s/%s",
            options.getDatabaseHost(), options.getDatabasePort(), options.getDatabaseName());

    CdcJdbcIO.DataSourceConfiguration dataSourceConfiguration =
        CdcJdbcIO.DataSourceConfiguration.create(
                "org.postgresql.Driver", jdbcDriverConnectionString)
            .withUsername(options.getDatabaseUser())
            .withPassword(options.getDatabasePassword())
            .withMaxIdleConnections(new Integer(0));

    validateOptions(options, dataSourceConfiguration);

    /*
     * Stage 1: Ingest and Normalize Data to FailsafeElement with JSON Strings
     *   a) Read DataStream data from GCS into JSON String FailsafeElements (datastreamJsonRecords)
     */
    PCollection<FailsafeElement<String, String>> datastreamJsonRecords =
        pipeline.apply(
            new DataStreamIO(
                    options.getStreamName(),
                    options.getInputFilePattern(),
                    options.getInputFileFormat(),
                    options.getGcsPubSubSubscription(),
                    options.getRfcStartDateTime())
                .withLowercaseSourceColumns()
                .withRenameColumnValue("_metadata_row_id", "rowid")
                .withHashRowId());

    /*
     * Stage 2: Write JSON Strings to Postgres Insert Strings
     *   a) Convert JSON String FailsafeElements to TableRow's (tableRowRecords)
     * Stage 3) Filter stale rows using stateful PK transform
     */
    PCollection<DmlInfo> dmlStatements =
        datastreamJsonRecords
            .apply("Format to Postgres DML", CreateDml.createDmlObjects(dataSourceConfiguration))
            .apply("DML Stateful Processing", ProcessDml.statefulOrderByPK());

    /*
     * Stage 4: Write Inserts to CloudSQL
     */
    dmlStatements.apply(
        "Write to Postgres",
        CdcJdbcIO.<DmlInfo>write()
            .withDataSourceConfiguration(dataSourceConfiguration)
            .withStatementFormatter(
                new CdcJdbcIO.StatementFormatter<DmlInfo>() {
                  public String formatStatement(DmlInfo element) {
                    return element.getDmlSql();
                  }
                }));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}
