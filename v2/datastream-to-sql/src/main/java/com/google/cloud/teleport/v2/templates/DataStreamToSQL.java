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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.datastream.io.CdcJdbcIO;
import com.google.cloud.teleport.v2.datastream.sources.DataStreamIO;
import com.google.cloud.teleport.v2.datastream.values.DmlInfo;
import com.google.cloud.teleport.v2.templates.DataStreamToSQL.Options;
import com.google.cloud.teleport.v2.transforms.CreateDml;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import com.google.cloud.teleport.v2.transforms.ProcessDml;
import com.google.cloud.teleport.v2.utils.DatastreamToDML;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Splitter;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests DataStream data from GCS. The data is then cleaned and converted from JSON
 * objects into DML statements. The DML is applied to the desired target database, which can be one
 * of MySQL or PostgreSQL. Replication maintains a 1:1 match between source and target by default.
 * No DDL is supported in the current version of this pipeline.
 *
 * <p>Failures during SQL execution are captured and written to a Dead Letter Queue (DLQ) in GCS.
 * The pipeline also reconsumes failed records from the DLQ for reprocessing.
 *
 * <p>NOTE: Future versions will support: Pub/Sub, GCS, or Kafka as per DataStream
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/datastream-to-sql/README_Cloud_Datastream_to_SQL.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Cloud_Datastream_to_SQL",
    category = TemplateCategory.STREAMING,
    displayName = "Datastream to SQL",
    description = {
      "The Datastream to SQL template is a streaming pipeline that reads <a href=\"https://cloud.google.com/datastream/docs\">Datastream</a> data and replicates it into any MySQL or PostgreSQL database. "
          + "The template reads data from Cloud Storage using Pub/Sub notifications and replicates this data into SQL replica tables.\n",
      "The template does not support data definition language (DDL) and expects that all tables already exist in the database. "
          + "Replication uses Dataflow stateful transforms to filter stale data and ensure consistency in out of order data. "
          + "For example, if a more recent version of a row has already passed through, a late arriving version of that row is ignored. "
          + "The data manipulation language (DML) that executes is a best attempt to perfectly replicate source to target data. The DML statements executed follow the following rules:\n",
      "If a primary key exists, insert and update operations use upsert syntax (ie. <code>INSERT INTO table VALUES (...) ON CONFLICT (...) DO UPDATE</code>).\n"
          + "If primary keys exist, deletes are replicated as a delete DML.\n"
          + "If no primary key exists, both insert and update operations are inserted into the table.\n"
          + "If no primary keys exist, deletes are ignored.\n"
          + "If you are using the Oracle to Postgres utilities, add <code>ROWID</code> in SQL as the primary key when none exists."
    },
    optionsClass = Options.class,
    flexContainerName = "datastream-to-sql",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/datastream-to-sql",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "A Datastream stream that is ready to or already replicating data.",
      "<a href=\"https://cloud.google.com/storage/docs/reporting-changes\">Cloud Storage Pub/Sub notifications</a> are enabled for the Datastream data.",
      "A PostgreSQL database was seeded with the required schema.",
      "Network access between Dataflow workers and PostgreSQL is set up."
    },
    streaming = true,
    supportsAtLeastOnce = true)
public class DataStreamToSQL {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToSQL.class);
  private static final String AVRO_SUFFIX = "avro";
  private static final String JSON_SUFFIX = "json";

  /** String/String Coder for FailsafeElement. */
  public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, StreamingOptions {
    @TemplateParameter.GcsReadFile(
        order = 1,
        groupName = "Source",
        description = "File location for Datastream file input in Cloud Storage.",
        helpText =
            "The file location for the Datastream files in Cloud Storage to replicate. This file location is typically the root path for the stream.")
    String getInputFilePattern();

    void setInputFilePattern(String value);

    @TemplateParameter.PubsubSubscription(
        order = 2,
        optional = true,
        description = "The Pub/Sub subscription being used in a Cloud Storage notification policy.",
        helpText =
            "The Pub/Sub subscription with Datastream file notifications."
                + " For example, `projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_ID>`.")
    String getGcsPubSubSubscription();

    void setGcsPubSubSubscription(String value);

    @TemplateParameter.Enum(
        order = 3,
        enumOptions = {@TemplateEnumOption("avro"), @TemplateEnumOption("json")},
        optional = true,
        description = "Datastream output file format (avro/json).",
        helpText =
            "The format of the output file produced by Datastream. For example, `avro` or `json`. Defaults to `avro`.")
    @Default.String("avro")
    String getInputFileFormat();

    void setInputFileFormat(String value);

    @TemplateParameter.Text(
        order = 4,
        groupName = "Source",
        optional = true,
        description = "Name or template for the stream to poll for schema information.",
        helpText =
            "The name or template for the stream to poll for schema information. The default value is `{_metadata_stream}`.")
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
        enumOptions = {@TemplateEnumOption("postgres"), @TemplateEnumOption("mysql")},
        description = "SQL Database Type (postgres or mysql).",
        helpText = "The database type to write to (for example, Postgres).")
    @Default.String("postgres")
    String getDatabaseType();

    void setDatabaseType(String value);

    @TemplateParameter.Text(
        order = 8,
        groupName = "Target",
        description = "Database Host to connect on.",
        helpText = "The SQL host to connect on.")
    String getDatabaseHost();

    void setDatabaseHost(String value);

    @TemplateParameter.Text(
        order = 9,
        groupName = "Target",
        optional = true,
        description = "Database Port to connect on.",
        helpText = "The SQL database port to connect to. The default value is `5432`.")
    @Default.String("5432")
    String getDatabasePort();

    void setDatabasePort(String value);

    @TemplateParameter.Text(
        order = 10,
        description = "Database User to connect with.",
        helpText =
            "The SQL user with all required permissions to write to all tables in replication.")
    String getDatabaseUser();

    void setDatabaseUser(String value);

    @TemplateParameter.Password(
        order = 11,
        description = "Database Password for given user.",
        helpText = "The password for the SQL user.")
    String getDatabasePassword();

    void setDatabasePassword(String value);

    @TemplateParameter.Text(
        order = 12,
        groupName = "Target",
        optional = true,
        description = "SQL Database Name.",
        helpText = "The name of the SQL database to connect to. The default value is `postgres`.")
    @Default.String("postgres")
    String getDatabaseName();

    void setDatabaseName(String value);

    @TemplateParameter.Enum(
        order = 13,
        optional = true,
        enumOptions = {
          @TemplateEnumOption("LOWERCASE"),
          @TemplateEnumOption("UPPERCASE"),
          @TemplateEnumOption("CAMEL"),
          @TemplateEnumOption("SNAKE")
        },
        description = "Toggle for Table Casing",
        helpText =
            "A Toggle for table casing behavior. For example,(ie."
                + "LOWERCASE = mytable -> mytable, UPPERCASE = mytable -> MYTABLE"
                + "CAMEL = my_table -> myTable, SNAKE = myTable -> my_table")
    @Default.String("LOWERCASE")
    String getDefaultCasing();

    void setDefaultCasing(String value);

    @TemplateParameter.Enum(
        order = 14,
        optional = true,
        enumOptions = {
          @TemplateEnumOption("LOWERCASE"),
          @TemplateEnumOption("UPPERCASE"),
          @TemplateEnumOption("CAMEL"),
          @TemplateEnumOption("SNAKE")
        },
        description = "Toggle for Column Casing",
        helpText =
            "A toggle for target column name casing. "
                + "LOWERCASE (default): my_column -> my_column. "
                + "UPPERCASE: my_column -> MY_COLUMN. "
                + "CAMEL: my_column -> myColumn. "
                + "SNAKE: myColumn -> my_column.")
    @Default.String("LOWERCASE")
    String getColumnCasing();

    void setColumnCasing(String value);

    @TemplateParameter.Text(
        order = 15,
        optional = true,
        description = "A map of key/values used to dictate schema name changes",
        helpText =
            "A map of key/values used to dictate schema name changes (ie."
                + " old_name:new_name,CaseError:case_error)")
    @Default.String("")
    String getSchemaMap();

    void setSchemaMap(String value);

    @TemplateParameter.Text(
        order = 16,
        groupName = "Target",
        optional = true,
        description = "Custom connection string.",
        helpText =
            "Optional connection string which will be used instead of the default database string.")
    @Default.String("")
    String getCustomConnectionString();

    void setCustomConnectionString(String value);

    @TemplateParameter.Integer(
        order = 17,
        optional = true,
        description = "Number of threads to use for Format to DML step.",
        helpText =
            "Determines key parallelism of Format to DML step, specifically, the value is passed into Reshuffle.withNumBuckets.")
    @Default.Integer(100)
    int getNumThreads();

    void setNumThreads(int value);

    @TemplateParameter.Integer(
        order = 18,
        groupName = "Target",
        optional = true,
        description = "Database login timeout in seconds.",
        helpText =
            "The timeout in seconds for database login attempts. This helps prevent connection hangs when multiple workers try to connect simultaneously.")
    Integer getDatabaseLoginTimeout();

    void setDatabaseLoginTimeout(Integer value);

    @TemplateParameter.Boolean(
        order = 19,
        optional = true,
        description =
            "Order by configurations for data should include prioritizing data which is not deleted.",
        helpText =
            "Order by configurations for data should include prioritizing data which is not deleted.")
    @Default.Boolean(false)
    Boolean getOrderByIncludesIsDeleted();

    void setOrderByIncludesIsDeleted(Boolean value);

    @TemplateParameter.Text(
        order = 20,
        optional = true,
        description = "Datastream source type override",
        helpText =
            "Override the source type detection for Datastream CDC data. When specified, this value will be used instead of deriving the source type from the read_method field. Valid values include 'mysql', 'postgresql', 'oracle', etc. This parameter is useful when the read_method field contains 'cdc' and the actual source type cannot be determined automatically.")
    String getDatastreamSourceType();

    void setDatastreamSourceType(String value);

    @TemplateParameter.Text(
        order = 21,
        optional = true,
        description = "Dead letter queue directory.",
        helpText =
            "The path that Dataflow uses to write the dead-letter queue output. This path must not be in the same path as the Datastream file output. Defaults to `empty`.")
    @Default.String("")
    String getDeadLetterQueueDirectory();

    void setDeadLetterQueueDirectory(String value);

    @TemplateParameter.Integer(
        order = 22,
        optional = true,
        description = "The number of minutes between DLQ Retries.",
        helpText = "The number of minutes between DLQ Retries. Defaults to `10`.")
    @Default.Integer(10)
    Integer getDlqRetryMinutes();

    void setDlqRetryMinutes(Integer value);
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

    if (options.getDatabaseLoginTimeout() != null) {
      dataSourceConfiguration =
          dataSourceConfiguration.withLoginTimeout(options.getDatabaseLoginTimeout());
    }

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

  /**
   * Parses a single map string and resolves it into schema and table mappings, intelligently
   * inferring a schema-to-schema mapping if only table-specific rules are provided.
   *
   * @param mappingString A comma-separated string of mapping rules.
   */
  public static Map<String, Map<String, String>> parseMappings(String mappingString) {
    Map<String, String> schemaMappings = new HashMap<>();
    Map<String, String> tableMappings = new HashMap<>();

    if (mappingString != null && !mappingString.isEmpty()) {
      Map<String, String> allMappings =
          Splitter.on("|").withKeyValueSeparator(":").split(mappingString);

      // Strictly separate rules based on the presence of a dot.
      for (Map.Entry<String, String> entry : allMappings.entrySet()) {
        if (entry.getKey().contains(".")) {
          tableMappings.put(entry.getKey(), entry.getValue());
        } else {
          schemaMappings.put(entry.getKey(), entry.getValue());
        }
      }
    }
    Map<String, Map<String, String>> mappings = new HashMap<>();
    mappings.put("schemas", schemaMappings);
    mappings.put("tables", tableMappings);
    return mappings;
  }

  public static class DmlInfoDlqJsonFormatter
      implements CdcJdbcIO.DlqJsonFormatter<KV<String, DmlInfo>>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String apply(KV<String, DmlInfo> record) {
      try {
        ObjectNode jsonWrapper = MAPPER.createObjectNode();
        // FIX: Parse nested object
        JsonNode messageNode = MAPPER.readTree(record.getValue().getOriginalPayload());
        jsonWrapper.set("message", messageNode);

        jsonWrapper.put("error_message", "Failed DML execution");
        jsonWrapper.put("timestamp", Instant.now().toString());
        return MAPPER.writeValueAsString(jsonWrapper);
      } catch (Exception e) {
        return "{\"message\": \"SERIALIZATION_FAILED\"}";
      }
    }
  }

  public static class FailsafeDlqJsonFormatter
      extends DoFn<FailsafeElement<String, String>, String> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @ProcessElement
    public void processElement(ProcessContext context) {
      try {
        FailsafeElement<String, String> element = context.element();
        ObjectNode jsonWrapper = MAPPER.createObjectNode();
        // FIX: Parse nested object
        JsonNode messageNode = MAPPER.readTree(element.getOriginalPayload());
        jsonWrapper.set("message", messageNode);

        jsonWrapper.put("error_message", element.getErrorMessage());
        jsonWrapper.put("stacktrace", element.getStacktrace());
        jsonWrapper.put("timestamp", Instant.now().toString());
        context.output(MAPPER.writeValueAsString(jsonWrapper));
      } catch (Exception e) {
        LOG.error("Failed to format failsafe DLQ record", e);
      }
    }
  }

  private static DeadLetterQueueManager buildDlqManager(Options options) {
    String tempLocation =
        options.as(DataflowPipelineOptions.class).getTempLocation().endsWith("/")
            ? options.as(DataflowPipelineOptions.class).getTempLocation()
            : options.as(DataflowPipelineOptions.class).getTempLocation() + "/";

    String dlqDirectory =
        options.getDeadLetterQueueDirectory().isEmpty()
            ? tempLocation + "dlq/"
            : options.getDeadLetterQueueDirectory();

    LOG.info("Dead-letter queue directory: {}", dlqDirectory);
    return DeadLetterQueueManager.create(dlqDirectory);
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
     * 1) Ingest and Normalize Data to FailsafeElement with JSON Strings
     * 2) Write JSON Strings to SQL DML Objects
     * 3) Filter stale rows using stateful PK transform
     * 4) Write DML statements to SQL Database via jdbc
     * 5) Write Failures to GCS Dead Letter Queue
     */

    Pipeline pipeline = Pipeline.create(options);

    CdcJdbcIO.DataSourceConfiguration dataSourceConfiguration = getDataSourceConfiguration(options);
    validateOptions(options, dataSourceConfiguration);

    Map<String, Map<String, String>> mappings = parseMappings(options.getSchemaMap());
    Map<String, String> schemaMap = mappings.get("schemas");
    Map<String, String> tableNameMap = mappings.get("tables");

    LOG.info("Parsed schema map: {}", schemaMap);
    LOG.info("Parsed table name map: {}", tableNameMap);

    DeadLetterQueueManager dlqManager = buildDlqManager(options);
    String dlqDirectory = dlqManager.getRetryDlqDirectoryWithDateTime();
    String tempDlqDir = dlqManager.getRetryDlqDirectory() + "tmp/";

    /*
     * Stage 1: Ingest and Normalize Data to FailsafeElement with JSON Strings
     * a) Read DataStream data from GCS into JSON String FailsafeElements (datastreamJsonRecords)
     * b) Reconsume Dead Letter Queue data from GCS into JSON String FailsafeElements
     * (dlqJsonRecords)
     * c) Flatten DataStream and DLQ Streams (allJsonRecords)
     */
    PCollection<FailsafeElement<String, String>> datastreamJsonRecords =
        pipeline.apply(
            new DataStreamIO(
                    options.getStreamName(),
                    options.getInputFilePattern(),
                    options.getInputFileFormat(),
                    options.getGcsPubSubSubscription(),
                    options.getRfcStartDateTime())
                .withRenameColumnValue("_metadata_row_id", "rowid")
                .withHashRowId()
                .withDatastreamSourceType(options.getDatastreamSourceType()));

    // Elements sent to the Dead Letter Queue are to be reconsumed.
    PCollection<FailsafeElement<String, String>> dlqJsonRecords =
        pipeline
            .apply("DLQ Consumer/reader", dlqManager.dlqReconsumer(options.getDlqRetryMinutes()))
            .apply(
                "DLQ Consumer/cleaner",
                ParDo.of(
                    new DoFn<String, FailsafeElement<String, String>>() {
                      private final ObjectMapper mapper = new ObjectMapper();

                      @ProcessElement
                      public void process(
                          @Element String input,
                          OutputReceiver<FailsafeElement<String, String>> receiver) {
                        try {
                          DatastreamToDML.clearCaches();
                          JsonNode wrapper = mapper.readTree(input);
                          if (wrapper.has("message")) {
                            // FIX: Use .toString() to convert the nested JSON Object back to a
                            // String.
                            // .asText() would return null for a JSON Object node.
                            String payload = wrapper.get("message").toString();
                            receiver.output(FailsafeElement.of(payload, payload));
                          } else {
                            receiver.output(FailsafeElement.of(input, input));
                          }
                        } catch (Exception e) {
                          LOG.warn("Could not parse DLQ wrapper, trying raw: {}", e.getMessage());
                          receiver.output(FailsafeElement.of(input, input));
                        }
                      }
                    }))
            .setCoder(FAILSAFE_ELEMENT_CODER);

    PCollection<FailsafeElement<String, String>> allJsonRecords =
        PCollectionList.of(datastreamJsonRecords)
            .and(dlqJsonRecords)
            .apply("Merge Datastream & DLQ", Flatten.pCollections());

    /*
     * Stage 2: Write JSON Strings to SQL Insert Strings
     * a) Convert JSON String FailsafeElements to TableRow's (tableRowRecords)
     */
    PCollectionTuple dmlResults =
        allJsonRecords.apply(
            "Format to DML",
            CreateDml.of(dataSourceConfiguration)
                .withDefaultCasing(options.getDefaultCasing())
                .withSchemaMap(schemaMap)
                .withTableNameMap(tableNameMap)
                .withColumnCasing(options.getColumnCasing())
                .withOrderByIncludesIsDeleted(options.getOrderByIncludesIsDeleted())
                .withNumThreads(options.getNumThreads()));

    PCollection<KV<String, DmlInfo>> dmlStatements =
        dmlResults
            .get(CreateDml.DML_MAIN_TAG)
            /*
             * Stage 3) Filter stale rows using stateful PK transform
             */
            .apply("DML Stateful Processing", ProcessDml.statefulOrderByPK());

    PCollection<String> dmlConversionErrors =
        dmlResults
            .get(DatastreamToDML.ERROR_TAG)
            .apply("Format DML Errors", ParDo.of(new FailsafeDlqJsonFormatter()));

    /*
     * Stage 4: Write Inserts to CloudSQL
     */
    CdcJdbcIO.WriteResult writeResult =
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
                    })
                .withDlqJsonFormatter(new DmlInfoDlqJsonFormatter()));

    /*
     * Stage 5: Write Failures to GCS Dead Letter Queue
     */
    PCollection<String> sqlWriteFailures = writeResult.getFailedInserts();

    // FIX: Re-window SQL failures to GlobalWindows to match the upstream dmlConversionErrors
    // before flattening.
    PCollection<String> allErrors =
        PCollectionList.of(
                sqlWriteFailures.apply(
                    "GlobalWindow SQL Failures", Window.into(new GlobalWindows())))
            .and(dmlConversionErrors)
            .apply("Flatten Errors", Flatten.pCollections());

    allErrors.apply(
        "Write To DLQ/Writer",
        DLQWriteTransform.WriteDLQ.newBuilder()
            .withDlqDirectory(dlqDirectory)
            .withTmpDirectory(tempDlqDir)
            .setIncludePaneInfo(true)
            .build());

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}
