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
import com.google.cloud.teleport.v2.transforms.StreamingDataGeneratorWriteToPubSub;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.utils.GCSUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
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
 * specified rate to either Pub/Sub topic or BigQuery/GCS. The messages are generated according to a
 * schema template which instructs the pipeline how to populate the messages with fake data
 * compliant to constraints.
 *
 * <p>The number of workers executing the pipeline must be large enough to support the supplied QPS.
 * Use a general rule of 2,500 QPS per core in the worker pool.
 *
 * <p>See <a href="https://github.com/vincentrussell/json-data-generator">json-data-generator</a>
 * for instructions on how to construct the schema file.
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT=my-project
 * BUCKET_NAME=my-bucket
 * SCHEMA_LOCATION=gs://<bucket>/<path>/<to>/game-event-schema.json
 * PUBSUB_TOPIC=projects/<project-id>/topics/<topic-id>
 * QPS=2500
 *
 * # Set containerization vars
 * IMAGE_NAME=my-image-name
 * TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
 * BASE_CONTAINER_IMAGE=my-base-container-image
 * BASE_CONTAINER_IMAGE_VERSION=my-base-container-image-version
 * APP_ROOT=/path/to/app-root
 * COMMAND_SPEC=/path/to/command-spec
 *
 * # Build and upload image
 * mvn clean package \
 * -Dimage=${TARGET_GCR_IMAGE} \
 * -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
 * -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
 * -Dapp-root=${APP_ROOT} \
 * -Dcommand-spec=${COMMAND_SPEC}
 *
 * # Create a template spec containing the details of image location and metadata in GCS
 *   as specified in README.md file
 *
 * # Execute template:
 * JOB_NAME=<job-name>
 * PROJECT=<project-id>
 * TEMPLATE_SPEC_GCSPATH=gs://path/to/template-spec
 * SCHEMA_LOCATION=gs://path/to/schema.json
 * PUBSUB_TOPIC=projects/$PROJECT/topics/<topic-name>
 * QPS=1
 *
 * gcloud beta dataflow flex-template run $JOB_NAME \
 *         --project=$PROJECT --region=us-central1 --flex-template  \
 *         --template-file-gcs-location=$TEMPLATE_SPEC_GCSPATH \
 *         --parameters autoscalingAlgorithm="THROUGHPUT_BASED",schemaLocation=$SCHEMA_LOCATION,topic=$PUBSUB_TOPIC,qps=$QPS,maxNumWorkers=3
 *
 * </pre>
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

    @TemplateParameter.GcsReadFile(
        order = 2,
        description = "Location of Schema file",
        helpText = "Cloud Storage path of schema location.",
        example = "gs://<bucket-name>/prefix")
    @Required
    String getSchemaLocation();

    void setSchemaLocation(String value);

    @TemplateParameter.PubsubTopic(
        order = 3,
        optional = true,
        description = "Output Pub/Sub topic",
        helpText = "The name of the topic to which the pipeline should publish data.",
        example = "projects/<project-id>/topics/<topic-name>")
    String getTopic();

    void setTopic(String value);

    @TemplateParameter.Long(
        order = 4,
        optional = true,
        description = "Maximum number of output Messages",
        helpText =
            "Indicates maximum number of output messages to be generated. 0 means unlimited.")
    @Default.Long(0L)
    Long getMessagesLimit();

    void setMessagesLimit(Long value);

    @TemplateParameter.Enum(
        order = 5,
        enumOptions = {"AVRO", "JSON", "PARQUET"},
        optional = true,
        description = "Output Encoding Type",
        helpText = "The message Output type. Default is JSON.")
    @Default.Enum("JSON")
    OutputType getOutputType();

    void setOutputType(OutputType value);

    @TemplateParameter.GcsReadFile(
        order = 6,
        optional = true,
        description = "Location of Avro Schema file",
        helpText =
            "Cloud Storage path of Avro schema location. Mandatory when output type is AVRO or"
                + " PARQUET.",
        example = "gs://your-bucket/your-path/schema.avsc")
    String getAvroSchemaLocation();

    void setAvroSchemaLocation(String value);

    @TemplateParameter.Enum(
        order = 7,
        enumOptions = {"BIGQUERY", "GCS", "PUBSUB"},
        optional = true,
        description = "Output Sink Type",
        helpText = "The message Sink type. Default is PUBSUB")
    @Default.Enum("PUBSUB")
    SinkType getSinkType();

    void setSinkType(SinkType value);

    @TemplateParameter.BigQueryTable(
        order = 8,
        optional = true,
        description = "Output BigQuery table",
        helpText = "Output BigQuery table. Mandatory when sinkType is BIGQUERY",
        example = "<project>:<dataset>.<table_name>")
    String getOutputTableSpec();

    void setOutputTableSpec(String value);

    @TemplateParameter.Enum(
        order = 9,
        enumOptions = {"WRITE_APPEND", "WRITE_EMPTY", "WRITE_TRUNCATE"},
        optional = true,
        description = "Write Disposition to use for BigQuery",
        helpText =
            "BigQuery WriteDisposition. For example, WRITE_APPEND, WRITE_EMPTY or WRITE_TRUNCATE.")
    @Default.String("WRITE_APPEND")
    String getWriteDisposition();

    void setWriteDisposition(String writeDisposition);

    @TemplateParameter.BigQueryTable(
        order = 10,
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
        order = 11,
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
        order = 12,
        description = "Output file directory in Cloud Storage",
        helpText =
            "The path and filename prefix for writing output files. Must end with a slash. DateTime"
                + " formatting is used to parse directory path for date & time formatters.",
        example = "gs://your-bucket/your-path/")
    String getOutputDirectory();

    void setOutputDirectory(String outputDirectory);

    @TemplateParameter.Text(
        order = 13,
        optional = true,
        description = "Output filename prefix of the files to write",
        helpText = "The prefix to place on each windowed file.",
        example = "output-")
    @Default.String("output-")
    String getOutputFilenamePrefix();

    void setOutputFilenamePrefix(String outputFilenamePrefix);

    @TemplateParameter.Integer(
        order = 14,
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
    GCS
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

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    /*
     * Steps:
     *  1) Trigger at the supplied QPS
     *  2) Generate messages containing fake data
     *  3) Write messages to appropriate Sink
     */
    PCollection<byte[]> fakeMessages =
        pipeline
            .apply("Trigger", createTrigger(options))
            .apply(
                "Generate Fake Messages",
                ParDo.of(new MessageGeneratorFn(options.getSchemaLocation())));

    if (options.getSinkType().equals(SinkType.GCS)) {
      fakeMessages =
          fakeMessages.apply(
              options.getWindowDuration() + " Window",
              Window.into(
                  FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))));
    }

    fakeMessages.apply("Write To " + options.getSinkType().name(), createSink(options));

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
            .withRate(options.getQps(), /* periodLength = */ Duration.standardSeconds(1L));

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
    private final String schemaLocation;
    private String schema;

    MessageGeneratorFn(@Nonnull String schemaLocation) {
      checkNotNull(
          schemaLocation, "schemaLocation argument of MessageGeneratorFn class cannot be null.");
      this.schemaLocation = schemaLocation;
    }

    @Setup
    public void setup() throws IOException {
      dataGenerator = new JsonDataGeneratorImpl();
      schema = GCSUtils.getGcsFileAsString(schemaLocation);
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
      @Nonnull StreamingDataGeneratorOptions options) {
    checkNotNull(options, "options argument to createSink method cannot be null.");

    switch (options.getSinkType()) {
      case PUBSUB:
        checkArgument(
            options.getTopic() != null,
            String.format(
                "Missing required value --topic for %s sink type", options.getSinkType().name()));
        return StreamingDataGeneratorWriteToPubSub.Writer.builder(options).build();
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
      default:
        throw new IllegalArgumentException("Unsupported Sink.");
    }
  }
}
