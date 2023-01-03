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

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.cdc.sources.DataStreamIO;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.io.CdcJdbcIO;
import com.google.cloud.teleport.v2.templates.DataStreamToSQL.Options;
import com.google.cloud.teleport.v2.transforms.CreateDml;
import com.google.cloud.teleport.v2.transforms.ProcessDml;
import com.google.cloud.teleport.v2.values.DmlInfo;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Splitter;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests DataStream data from GCS. The data is then cleaned and converted from JSON
 * objects into DML statements. The DML is applied to the desired target database, which can be one
 * of MySQL or POstgreSQL. Replication maintains a 1:1 match between source and target by default.
 * No DDL is supported in the current version of this pipeline.
 *
 * <p>NOTE: Future versiosn will support: Pub/Sub, GCS, or Kafka as per DataStream
 *
 * <p>Example Usage: TODO: FIX EXMAPLE USAGE
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
 * --gcsPubSubSubscription="projects/<project-id>/subscriptions/<subscription-name>" \
 * --inputFilePattern=${GCS_AVRO_FILE_PATTERN} \
 * --outputProjectId=${OUTPUT_PROJECT_ID} \
 * --runner=(DirectRunner|DataflowRunner)"
 *
 * OPTIONAL Dataflow Params:
 * --autoscalingAlgorithm=THROUGHPUT_BASED \
 * --numWorkers=2 \
 * --maxNumWorkers=10 \
 * --workerMachineType=n1-highcpu-4 \
 * </pre>
 */
@Template(
    name = "Cloud_Datastream_to_SQL",
    category = TemplateCategory.STREAMING,
    displayName = "Datastream to SQL",
    description =
        "Streaming pipeline. Ingests messages from a stream in Cloud Datastream, transforms them,"
            + " and writes them to a set of pre-defined Postgres tables.",
    optionsClass = Options.class,
    flexContainerName = "datastream-to-sql",
    contactInformation = "https://cloud.google.com/support")
public class DataStreamToSQL {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToSQL.class);
  private static final String AVRO_SUFFIX = "avro";
  private static final String JSON_SUFFIX = "json";

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, StreamingOptions {
    @TemplateParameter.Text(
        order = 1,
        description = "File location for Datastream file input in Cloud Storage.",
        helpText =
            "This is the file location for Datastream file input in Cloud Storage. Normally, this"
                + " will be gs://${BUCKET}/${ROOT_PATH}/.")
    String getInputFilePattern();

    void setInputFilePattern(String value);

    @TemplateParameter.PubsubSubscription(
        order = 2,
        optional = true,
        description = "The Pub/Sub subscription being used in a Cloud Storage notification policy.",
        helpText =
            "The Pub/Sub subscription being used in a Cloud Storage notification policy. The name"
                + " should be in the format of"
                + " projects/<project-id>/subscriptions/<subscription-name>.")
    String getGcsPubSubSubscription();

    void setGcsPubSubSubscription(String value);

    @TemplateParameter.Enum(
        order = 3,
        enumOptions = {"avro", "json"},
        optional = true,
        description = "Datastream output file format (avro/json).",
        helpText =
            "This is the format of the output file produced by Datastream. by default this will be"
                + " avro.")
    @Default.String("avro")
    String getInputFileFormat();

    void setInputFileFormat(String value);

    @TemplateParameter.Text(
        order = 4,
        optional = true,
        description = "Name or template for the stream to poll for schema information.",
        helpText =
            "This is the name or template for the stream to poll for schema information. Default is"
                + " {_metadata_stream}. The default value is enough under most conditions.")
    String getStreamName();

    void setStreamName(String value);

    @TemplateParameter.DateTime(
        order = 5,
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

    // DataStream API Root Url (only used for testing)
    @TemplateParameter.Text(
        order = 6,
        optional = true,
        description = "Datastream API Root URL (only required for testing)",
        helpText = "Datastream API Root URL")
    @Default.String("https://datastream.googleapis.com/")
    String getDataStreamRootUrl();

    void setDataStreamRootUrl(String value);

    // SQL Connection Parameters
    @TemplateParameter.Enum(
        order = 7,
        optional = true,
        enumOptions = {"postgres", "mysql"},
        description = "SQL Database Type (postgres or mysql).",
        helpText = "The database type to write to (for example, Postgres).")
    @Default.String("postgres")
    String getDatabaseType();

    void setDatabaseType(String value);

    @TemplateParameter.Text(
        order = 8,
        description = "Database Host to connect on.",
        helpText = "Database Host to connect on.")
    String getDatabaseHost();

    void setDatabaseHost(String value);

    @TemplateParameter.Text(
        order = 9,
        optional = true,
        description = "Database Port to connect on.",
        helpText = "Database Port to connect on (default 5432).")
    @Default.String("5432")
    String getDatabasePort();

    void setDatabasePort(String value);

    @TemplateParameter.Text(
        order = 10,
        description = "Database User to connect with.",
        helpText = "Database User to connect with.")
    String getDatabaseUser();

    void setDatabaseUser(String value);

    @TemplateParameter.Password(
        order = 11,
        description = "Database Password for given user.",
        helpText = "Database Password for given user.")
    String getDatabasePassword();

    void setDatabasePassword(String value);

    @TemplateParameter.Text(
        order = 12,
        optional = true,
        description = "SQL Database Name.",
        helpText = "The database name to connect to.")
    @Default.String("postgres")
    String getDatabaseName();

    void setDatabaseName(String value);

    @TemplateParameter.Text(
        order = 13,
        optional = true,
        description = "A map of key/values used to dictate schema name changes",
        helpText =
            "A map of key/values used to dictate schema name changes (ie."
                + " old_name:new_name,CaseError:case_error)")
    @Default.String("")
    String getSchemaMap();

    void setSchemaMap(String value);

    @TemplateParameter.Text(
        order = 14,
        optional = true,
        description = "Custom connection string.",
        helpText =
            "Optional connection string which will be used instead of the default database string.")
    @Default.String("")
    String getCustomConnectionString();

    void setCustomConnectionString(String value);
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    LOG.info("Starting Datastream to SQL");

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    options.setStreaming(true);
    run(options);
  }

  /**
   * Build the DataSourceConfiguration for the target SQL database. Using the pipeline options,
   * determine the database type and create the correct jdbc connection for the requested DB.
   *
   * @param options The execution parameters to the pipeline.
   */
  public static CdcJdbcIO.DataSourceConfiguration getDataSourceConfiguration(Options options) {
    String jdbcDriverName;
    String jdbcDriverConnectionString;

    switch (options.getDatabaseType()) {
      case "postgres":
        jdbcDriverName = "org.postgresql.Driver";
        jdbcDriverConnectionString =
            String.format(
                "jdbc:postgresql://%s:%s/%s",
                options.getDatabaseHost(), options.getDatabasePort(), options.getDatabaseName());
        break;
      case "mysql":
        jdbcDriverName = "com.mysql.cj.jdbc.Driver";
        jdbcDriverConnectionString =
            String.format(
                "jdbc:mysql://%s:%s/%s",
                options.getDatabaseHost(), options.getDatabasePort(), options.getDatabaseName());
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Database Type %s is not supported.", options.getDatabaseType()));
    }
    if (!options.getCustomConnectionString().isEmpty()) {
      jdbcDriverConnectionString = options.getCustomConnectionString();
    }

    CdcJdbcIO.DataSourceConfiguration dataSourceConfiguration =
        CdcJdbcIO.DataSourceConfiguration.create(jdbcDriverName, jdbcDriverConnectionString)
            .withUsername(options.getDatabaseUser())
            .withPassword(options.getDatabasePassword())
            .withMaxIdleConnections(new Integer(0));

    return dataSourceConfiguration;
  }

  /**
   * Validate the options supplied match expected values. We will also validate that connectivity is
   * working correctly for the target SQL database.
   *
   * @param options The execution parameters to the pipeline.
   * @param dataSourceConfiguration The JDBC datasource configuration.
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

  /** Parse the SchemaMap config which allows key:value pairs of column naming configs. */
  public static Map<String, String> parseSchemaMap(String schemaMapString) {
    if (schemaMapString == null || schemaMapString.equals("")) {
      return new HashMap<>();
    }

    return Splitter.on(",").withKeyValueSeparator(":").split(schemaMapString);
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
     *   2) Write JSON Strings to SQL DML Objects
     *   3) Filter stale rows using stateful PK transform
     *   4) Write DML statements to SQL Database via jdbc
     */

    Pipeline pipeline = Pipeline.create(options);

    CdcJdbcIO.DataSourceConfiguration dataSourceConfiguration = getDataSourceConfiguration(options);
    validateOptions(options, dataSourceConfiguration);
    Map<String, String> schemaMap = parseSchemaMap(options.getSchemaMap());

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
     * Stage 2: Write JSON Strings to SQL Insert Strings
     *   a) Convert JSON String FailsafeElements to TableRow's (tableRowRecords)
     * Stage 3) Filter stale rows using stateful PK transform
     */
    PCollection<KV<String, DmlInfo>> dmlStatements =
        datastreamJsonRecords
            .apply("Format to DML", CreateDml.of(dataSourceConfiguration).withSchemaMap(schemaMap))
            .apply("DML Stateful Processing", ProcessDml.statefulOrderByPK());

    /*
     * Stage 4: Write Inserts to CloudSQL
     */
    dmlStatements.apply(
        "Write to SQL",
        CdcJdbcIO.<KV<String, DmlInfo>>write()
            .withDataSourceConfiguration(dataSourceConfiguration)
            .withStatementFormatter(
                new CdcJdbcIO.StatementFormatter<KV<String, DmlInfo>>() {
                  public String formatStatement(KV<String, DmlInfo> element) {
                    LOG.debug("Executing SQL: {}", element.getValue().getDmlSql());
                    return element.getValue().getDmlSql();
                  }
                }));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}
