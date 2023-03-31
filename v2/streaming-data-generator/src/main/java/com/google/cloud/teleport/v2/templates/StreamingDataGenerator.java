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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.github.vincentrussell.json.datagenerator.JsonDataGenerator;
import com.github.vincentrussell.json.datagenerator.JsonDataGeneratorException;
import com.github.vincentrussell.json.datagenerator.impl.JsonDataGeneratorImpl;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator.StreamingDataGeneratorOptions;
import com.google.cloud.teleport.v2.transforms.StreamingDataGeneratorWriteToBigQuery;
import com.google.cloud.teleport.v2.transforms.StreamingDataGeneratorWriteToGcs;
import com.google.cloud.teleport.v2.transforms.StreamingDataGeneratorWriteToJdbc;
import com.google.cloud.teleport.v2.transforms.StreamingDataGeneratorWriteToPubSub;
import com.google.cloud.teleport.v2.transforms.StreamingDataGeneratorWriteToSpanner;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.utils.GCSUtils;
import com.google.cloud.teleport.v2.utils.MetadataValidator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link StreamingDataGenerator} is a streaming pipeline which generates messages at a
 * specified rate to either Pub/Sub, BigQuery, GCS, JDBC, or Spanner. The messages are generated
 * according to a schema template which instructs the pipeline how to populate the messages with
 * fake data compliant to constraints.
 *
 * <p>The number of workers executing the pipeline must be large enough to support the supplied QPS.
 * Use a general rule of 2,500 QPS per core in the worker pool.
 *
 * <p>See <a href="https://github.com/vincentrussell/json-data-generator">json-data-generator</a>
 * for instructions on how to construct the schema file.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/streaming-data-generator/README_Streaming_Data_Generator.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Streaming_Data_Generator",
    category = TemplateCategory.UTILITIES,
    displayName = "Streaming Data Generator",
    description =
        "A pipeline to publish messages at specified QPS.This template can be used to benchmark"
            + " performance of streaming pipelines.",
    optionsClass = StreamingDataGeneratorOptions.class,
    flexContainerName = "streaming-data-generator",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/streaming-data-generator",
    contactInformation = "https://cloud.google.com/support")
public class StreamingDataGenerator {

  private static final Logger logger = LoggerFactory.getLogger(StreamingDataGenerator.class);

  /**
   * The {@link StreamingDataGeneratorOptions} class provides the custom execution options passed by
   * the executor at the command-line.
   */
  public interface StreamingDataGeneratorOptions extends PipelineOptions {
    @TemplateParameter.Text(
        order = 1,
        regexes = {"^[1-9][0-9]*$"},
        description = "Required output rate",
        helpText = "Indicates rate of messages per second to be published to Pub/Sub")
    @Required
    Long getQps();

    void setQps(Long value);

    @TemplateParameter.Enum(
        order = 2,
        enumOptions = {"GAME_EVENT"},
        optional = true,
        description = "Schema template to generate fake data",
        helpText = "Pre-existing schema template to use. The value must be one of: [GAME_EVENT]")
    SchemaTemplate getSchemaTemplate();

    void setSchemaTemplate(SchemaTemplate value);

    @TemplateParameter.GcsReadFile(
        order = 3,
        optional = true,
        description = "Location of Schema file to generate fake data",
        helpText = "Cloud Storage path of schema location.",
        example = "gs://<bucket-name>/prefix")
    String getSchemaLocation();

    void setSchemaLocation(String value);

    @TemplateParameter.PubsubTopic(
        order = 4,
        optional = true,
        description = "Output Pub/Sub topic",
        helpText = "The name of the topic to which the pipeline should publish data.",
        example = "projects/<project-id>/topics/<topic-name>")
    String getTopic();

    void setTopic(String value);

    @TemplateParameter.Long(
        order = 5,
        optional = true,
        description = "Maximum number of output Messages",
        helpText =
            "Indicates maximum number of output messages to be generated. 0 means unlimited.")
    @Default.Long(0L)
    Long getMessagesLimit();

    void setMessagesLimit(Long value);

    @TemplateParameter.Enum(
        order = 6,
        enumOptions = {"AVRO", "JSON", "PARQUET"},
        optional = true,
        description = "Output Encoding Type",
        helpText = "The message Output type. Default is JSON.")
    @Default.Enum("JSON")
    OutputType getOutputType();

    void setOutputType(OutputType value);

    @TemplateParameter.GcsReadFile(
        order = 7,
        optional = true,
        description = "Location of Avro Schema file",
        helpText =
            "Cloud Storage path of Avro schema location. Mandatory when output type is AVRO or"
                + " PARQUET.",
        example = "gs://your-bucket/your-path/schema.avsc")
    String getAvroSchemaLocation();

    void setAvroSchemaLocation(String value);

    @TemplateParameter.Enum(
        order = 8,
        enumOptions = {"BIGQUERY", "GCS", "PUBSUB", "JDBC", "SPANNER"},
        optional = true,
        description = "Output Sink Type",
        helpText = "The message Sink type. Default is PUBSUB")
    @Default.Enum("PUBSUB")
    SinkType getSinkType();

    void setSinkType(SinkType value);

    @TemplateParameter.BigQueryTable(
        order = 9,
        optional = true,
        description = "Output BigQuery table",
        helpText = "Output BigQuery table. Mandatory when sinkType is BIGQUERY",
        example = "<project>:<dataset>.<table_name>")
    String getOutputTableSpec();

    void setOutputTableSpec(String value);

    @TemplateParameter.Enum(
        order = 10,
        enumOptions = {"WRITE_APPEND", "WRITE_EMPTY", "WRITE_TRUNCATE"},
        optional = true,
        description = "Write Disposition to use for BigQuery",
        helpText =
            "BigQuery WriteDisposition. For example, WRITE_APPEND, WRITE_EMPTY or WRITE_TRUNCATE.")
    @Default.String("WRITE_APPEND")
    String getWriteDisposition();

    void setWriteDisposition(String writeDisposition);

    @TemplateParameter.BigQueryTable(
        order = 11,
        optional = true,
        description = "The dead-letter table name to output failed messages to BigQuery",
        helpText =
            "Messages failed to reach the output table for all kind of reasons (e.g., mismatched"
                + " schema, malformed json) are written to this table. If it doesn't exist, it will"
                + " be created during pipeline execution.",
        example = "your-project-id:your-dataset.your-table-name")
    String getOutputDeadletterTable();

    void setOutputDeadletterTable(String outputDeadletterTable);

    @TemplateParameter.Duration(
        order = 12,
        optional = true,
        description = "Window duration",
        helpText =
            "The window duration/size in which data will be written to Cloud Storage. Allowed"
                + " formats are: Ns (for seconds, example: 5s), Nm (for minutes, example: 12m), Nh"
                + " (for hours, example: 2h).",
        example = "1m")
    @Default.String("1m")
    String getWindowDuration();

    void setWindowDuration(String windowDuration);

    @TemplateParameter.GcsWriteFolder(
        order = 13,
        optional = true,
        description = "Output file directory in Cloud Storage",
        helpText =
            "The path and filename prefix for writing output files. Must end with a slash. DateTime"
                + " formatting is used to parse directory path for date & time formatters.",
        example = "gs://your-bucket/your-path/")
    String getOutputDirectory();

    void setOutputDirectory(String outputDirectory);

    @TemplateParameter.Text(
        order = 14,
        optional = true,
        description = "Output filename prefix of the files to write",
        helpText = "The prefix to place on each windowed file.",
        example = "output-")
    @Default.String("output-")
    String getOutputFilenamePrefix();

    void setOutputFilenamePrefix(String outputFilenamePrefix);

    @TemplateParameter.Integer(
        order = 15,
        optional = true,
        description = "Maximum output shards",
        helpText =
            "The maximum number of output shards produced when writing. A higher number of shards"
                + " means higher throughput for writing to Cloud Storage, but potentially higher"
                + " data aggregation cost across shards when processing output Cloud Storage files."
                + " Default value is decided by the runner.")
    @Default.Integer(0)
    Integer getNumShards();

    void setNumShards(Integer numShards);

    @TemplateParameter.Text(
        order = 16,
        optional = true,
        regexes = {"^.+$"},
        description = "JDBC driver class name.",
        helpText = "JDBC driver class name to use.",
        example = "com.mysql.jdbc.Driver")
    String getDriverClassName();

    void setDriverClassName(String driverClassName);

    @TemplateParameter.Text(
        order = 17,
        optional = true,
        regexes = {
          "(^jdbc:[a-zA-Z0-9/:@.?_+!*=&-;]+$)|(^([A-Za-z0-9+/]{4}){1,}([A-Za-z0-9+/]{0,3})={0,3})"
        },
        description = "JDBC connection URL string.",
        helpText = "Url connection string to connect to the JDBC source.",
        example = "jdbc:mysql://some-host:3306/sampledb")
    String getConnectionUrl();

    void setConnectionUrl(String connectionUrl);

    @TemplateParameter.Text(
        order = 18,
        optional = true,
        regexes = {"^.+$"},
        description = "JDBC connection username.",
        helpText = "User name to be used for the JDBC connection.")
    String getUsername();

    void setUsername(String username);

    @TemplateParameter.Password(
        order = 19,
        optional = true,
        description = "JDBC connection password.",
        helpText = "Password to be used for the JDBC connection.")
    String getPassword();

    void setPassword(String password);

    @TemplateParameter.Text(
        order = 20,
        optional = true,
        regexes = {"^[a-zA-Z0-9_;!*&=@#-:\\/]+$"},
        description = "JDBC connection property string.",
        helpText =
            "Properties string to use for the JDBC connection. Format of the string must be"
                + " [propertyName=property;]*.",
        example = "unicode=true;characterEncoding=UTF-8")
    String getConnectionProperties();

    void setConnectionProperties(String connectionProperties);

    @TemplateParameter.Text(
        order = 21,
        optional = true,
        regexes = {"^.+$"},
        description = "Statement which will be executed against the database.",
        helpText =
            "SQL statement which will be executed to write to the database. The statement must"
                + " specify the column names of the table in any order. Only the values of the"
                + " specified column names will be read from the json and added to the statement.",
        example = "INSERT INTO tableName (column1, column2) VALUES (?,?)")
    String getStatement();

    void setStatement(String statement);

    @TemplateParameter.Text(
        order = 22,
        optional = true,
        regexes = {"^.+$"},
        description = "GCP Project Id of where the Spanner table lives.",
        helpText = "GCP Project Id of where the Spanner table lives.")
    String getProjectId();

    void setProjectId(String projectId);

    @TemplateParameter.Text(
        order = 23,
        optional = true,
        regexes = {"^.+$"},
        description = "Cloud Spanner instance name.",
        helpText = "Cloud Spanner instance name.")
    String getSpannerInstanceName();

    void setSpannerInstanceName(String spannerInstanceName);

    @TemplateParameter.Text(
        order = 24,
        optional = true,
        regexes = {"^.+$"},
        description = "Cloud Spanner database name.",
        helpText = "Cloud Spanner database name.")
    String getSpannerDatabaseName();

    void setSpannerDatabaseName(String spannerDBName);

    @TemplateParameter.Text(
        order = 25,
        optional = true,
        regexes = {"^.+$"},
        description = "Cloud Spanner table name.",
        helpText = "Cloud Spanner table name.")
    String getSpannerTableName();

    void setSpannerTableName(String spannerTableName);
  }

  /** Allowed list of existing schema templates. */
  public enum SchemaTemplate {
    GAME_EVENT(
        "{\n"
            + "  \"eventId\": \"{{uuid()}}\",\n"
            + "  \"eventTimestamp\": {{timestamp()}},\n"
            + "  \"ipv4\": \"{{ipv4()}}\",\n"
            + "  \"ipv6\": \"{{ipv6()}}\",\n"
            + "  \"country\": \"{{country()}}\",\n"
            + "  \"username\": \"{{username()}}\",\n"
            + "  \"quest\": \"{{random(\"A Break In the Ice\", \"Ghosts of Perdition\", \"Survive"
            + " the Low Road\")}}\",\n"
            + "  \"score\": {{integer(100, 10000)}},\n"
            + "  \"completed\": {{bool()}}\n"
            + "}"),
    LOG_ENTRY(
        "{\n"
            + "  \"logName\": \"{{alpha(10,20)}}\",\n"
            + "  \"resource\": {\n"
            + "    \"type\": \"{{alpha(5,10)}}\"\n"
            + "  },\n"
            + "  \"timestamp\": {{timestamp()}},\n"
            + "  \"receiveTimestamp\": {{timestamp()}},\n"
            + "  \"severity\": \"{{random(\"DEFAULT\", \"DEBUG\", \"INFO\", \"NOTICE\","
            + " \"WARNING\", \"ERROR\", \"CRITICAL\", \"ERROR\")}}\",\n"
            + "  \"insertId\": \"{{uuid()}}\",\n"
            + "  \"trace\": \"{{uuid()}}\",\n"
            + "  \"spanId\": \"{{uuid()}}\",\n"
            + "  \"jsonPayload\": {\n"
            + "    \"bytes_sent\": {{integer(1000,20000)}},\n"
            + "    \"connection\": {\n"
            + "      \"dest_ip\": \"{{ipv4()}}\",\n"
            + "      \"dest_port\": {{integer(0,65000)}},\n"
            + "      \"protocol\": {{integer(0,6)}},\n"
            + "      \"src_ip\": \"{{ipv4()}}\",\n"
            + "      \"src_port\": {{integer(0,65000)}}\n"
            + "    },\n"
            + "    \"dest_instance\": {\n"
            + "      \"project_id\": \"{{concat(\"PROJECT\", integer(0,3))}}\",\n"
            + "      \"region\": \"{{country()}}\",\n"
            + "      \"vm_name\": \"{{username()}}\",\n"
            + "      \"zone\": \"{{state()}}\"\n"
            + "    },\n"
            + "    \"end_time\": {{timestamp()}},\n"
            + "    \"packets_sent\": {{integer(100,400)}},\n"
            + "    \"reporter\": \"{{random(\"SRC\", \"DEST\")}}\",\n"
            + "    \"rtt_msec\": {{integer(0,20)}},\n"
            + "    \"start_time\": {{timestamp()}}\n"
            + "  }\n"
            + "}");

    private final String schema;

    SchemaTemplate(String schema) {
      this.schema = schema;
    }

    public String getSchema() {
      return schema;
    }
  }

  /** Allowed list of message encoding types. */
  public enum OutputType {
    JSON(".json"),
    AVRO(".avro"),
    PARQUET(".parquet");

    private final String fileExtension;

    /** Sets file extension associated with output type. */
    OutputType(String fileExtension) {
      this.fileExtension = fileExtension;
    }

    /** Returns file extension associated with output type. */
    public String getFileExtension() {
      return fileExtension;
    }
  }

  /** Allowed list of sink types. */
  public enum SinkType {
    PUBSUB,
    BIGQUERY,
    GCS,
    JDBC,
    SPANNER
  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * StreamingDataGenerator#run(StreamingDataGeneratorOptions)} method to start the pipeline and
   * invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args command-line args passed by the executor.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    StreamingDataGeneratorOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(StreamingDataGeneratorOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options. This method does not wait until the
   * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
   * object to block until the pipeline is finished running if blocking programmatic execution is
   * required.
   *
   * @param options the execution options.
   * @return the pipeline result.
   */
  public static PipelineResult run(@Nonnull StreamingDataGeneratorOptions options) {
    checkNotNull(options, "options argument to run method cannot be null.");
    MetadataValidator.validate(options);

    // FileSystems does not set the default configuration in workers till Pipeline.run
    // Explicitly registering standard file systems.
    FileSystems.setDefaultPipelineOptions(options);
    String schema = getSchema(options.getSchemaTemplate(), options.getSchemaLocation());

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    /*
     * Steps:
     *  1) Trigger at the supplied QPS
     *  2) Generate messages containing fake data
     *  3) Write messages to appropriate Sink
     */
    PCollection<byte[]> generatedMessages =
        pipeline
            .apply("Trigger", createTrigger(options))
            .apply("Generate Fake Messages", ParDo.of(new MessageGeneratorFn(schema)));

    if (options.getSinkType().equals(SinkType.GCS)) {
      generatedMessages =
          generatedMessages.apply(
              options.getWindowDuration() + " Window",
              Window.into(
                  FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))));
    }

    generatedMessages.apply(
        "Write To " + options.getSinkType().name(), createSink(options, schema));

    return pipeline.run();
  }

  /**
   * Creates either Bounded or UnBounded Source based on messageLimit pipeline option.
   *
   * @param options the pipeline options.
   */
  private static GenerateSequence createTrigger(@Nonnull StreamingDataGeneratorOptions options) {
    checkNotNull(options, "options argument to createTrigger method cannot be null.");
    GenerateSequence generateSequence =
        GenerateSequence.from(0L)
            .withRate(options.getQps(), /* periodLength= */ Duration.standardSeconds(1L));

    return options.getMessagesLimit() > 0
        ? generateSequence.to(options.getMessagesLimit())
        : generateSequence;
  }

  /**
   * The {@link MessageGeneratorFn} class generates fake messages based on supplied schema
   *
   * <p>See <a href="https://github.com/vincentrussell/json-data-generator">json-data-generator</a>
   * for instructions on how to construct the schema file.
   */
  @VisibleForTesting
  static class MessageGeneratorFn extends DoFn<Long, byte[]> {

    // Not initialized inline or constructor because {@link JsonDataGenerator} is not serializable.
    private transient JsonDataGenerator dataGenerator;
    private final String schema;

    MessageGeneratorFn(String schema) {
      this.schema = schema;
    }

    @Setup
    public void setup() {
      dataGenerator = new JsonDataGeneratorImpl();
    }

    @ProcessElement
    public void processElement(
        @Element Long element,
        @Timestamp Instant timestamp,
        OutputReceiver<byte[]> receiver,
        ProcessContext context)
        throws IOException, JsonDataGeneratorException {

      byte[] payload;

      // Generate the fake JSON according to the schema.
      try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
        dataGenerator.generateTestDataJson(schema, byteArrayOutputStream);
        payload = byteArrayOutputStream.toByteArray();
      }

      receiver.output(payload);
    }
  }

  /**
   * Creates appropriate sink based on sinkType pipeline option.
   *
   * @param options the pipeline options.
   */
  @VisibleForTesting
  static PTransform<PCollection<byte[]>, PDone> createSink(
      @Nonnull StreamingDataGeneratorOptions options, @Nonnull String schema) {
    checkNotNull(options, "options argument to createSink method cannot be null.");
    checkNotNull(schema, "schema argument to createSink method cannot be null.");

    switch (options.getSinkType()) {
      case PUBSUB:
        checkArgument(
            options.getTopic() != null,
            String.format(
                "Missing required value --topic for %s sink type", options.getSinkType().name()));
        return StreamingDataGeneratorWriteToPubSub.Writer.builder(options, schema).build();
      case BIGQUERY:
        checkArgument(
            options.getOutputTableSpec() != null,
            String.format(
                "Missing required value --outputTableSpec in format"
                    + " <project>:<dataset>.<table_name> for %s sink type",
                options.getSinkType().name()));
        return StreamingDataGeneratorWriteToBigQuery.builder(options).build();
      case GCS:
        checkArgument(
            options.getOutputDirectory() != null,
            String.format(
                "Missing required value --outputDirectory in format gs:// for %s sink type",
                options.getSinkType().name()));
        return StreamingDataGeneratorWriteToGcs.builder(options).build();
      case JDBC:
        checkArgument(
            options.getDriverClassName() != null,
            String.format(
                "Missing required value --driverClassName for %s sink type",
                options.getSinkType().name()));
        checkArgument(
            options.getConnectionUrl() != null,
            String.format(
                "Missing required value --connectionUrl for %s sink type",
                options.getSinkType().name()));
        checkArgument(
            options.getStatement() != null,
            String.format(
                "Missing required value --statement for %s sink type",
                options.getSinkType().name()));
        return StreamingDataGeneratorWriteToJdbc.builder(options).build();
      case SPANNER:
        checkArgument(
            options.getProjectId() != null,
            String.format(
                "Missing required value --projectId for %s sink type",
                options.getSinkType().name()));
        checkArgument(
            options.getSpannerInstanceName() != null,
            String.format(
                "Missing required value --spannerInstanceName for %s sink type",
                options.getSinkType().name()));
        checkArgument(
            options.getSpannerDatabaseName() != null,
            String.format(
                "Missing required value --spannerDatabaseName for %s sink type",
                options.getSinkType().name()));
        checkArgument(
            options.getSpannerTableName() != null,
            String.format(
                "Missing required value --spannerTableName for %s sink type",
                options.getSinkType().name()));
        return StreamingDataGeneratorWriteToSpanner.builder(options).build();
      default:
        throw new IllegalArgumentException("Unsupported Sink.");
    }
  }

  private static String getSchema(SchemaTemplate schemaTemplate, String schemaLocation) {
    checkArgument(
        schemaTemplate != null || schemaLocation != null,
        "Either schemaTemplate or schemaLocation argument of MessageGeneratorFn class must be"
            + " provided.");
    if (schemaLocation != null) {
      return GCSUtils.getGcsFileAsString(schemaLocation);
    } else {
      return schemaTemplate.getSchema();
    }
  }
}
