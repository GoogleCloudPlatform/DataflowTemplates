/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.cloud.teleport.v2.transforms.StreamingDataGeneratorWriteToBigQuery;
import com.google.cloud.teleport.v2.transforms.StreamingDataGeneratorWriteToGcs;
import com.google.cloud.teleport.v2.transforms.StreamingDataGeneratorWriteToPubSub;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
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
public class StreamingDataGenerator {

  private static final Logger logger = LoggerFactory.getLogger(StreamingDataGenerator.class);

  /**
   * The {@link StreamingDataGeneratorOptions} class provides the custom execution options passed by
   * the executor at the command-line.
   */
  public interface StreamingDataGeneratorOptions extends PipelineOptions {
    @Description("Indicates rate of messages per second to be published to Pub/Sub.")
    @Required
    Long getQps();

    void setQps(Long value);

    @Description("The path to the schema to generate.")
    @Required
    String getSchemaLocation();

    void setSchemaLocation(String value);

    @Description("The Pub/Sub topic to write to.")
    String getTopic();

    void setTopic(String value);

    @Description(
        "Indicates maximum number of messages to be generated. Default is 0 indicating unlimited.")
    @Default.Long(0L)
    Long getMessagesLimit();

    void setMessagesLimit(Long value);

    @Description("The message Output type. --outputType must be one of:[JSON,AVRO,PARQUET]")
    @Default.Enum("JSON")
    OutputType getOutputType();

    void setOutputType(OutputType value);

    @Description("The path to Avro schema for encoding message into AVRO output type.")
    String getAvroSchemaLocation();

    void setAvroSchemaLocation(String value);

    @Description("The message sink type. Must be one of:[PUBSUB,BIGQUERY,GCS]")
    @Default.Enum("PUBSUB")
    SinkType getSinkType();

    void setSinkType(SinkType value);

    @Description(
        "Output BigQuery table spec. "
            + "The name should be in the format: "
            + "<project>:<dataset>.<table_name>.")
    String getOutputTableSpec();

    void setOutputTableSpec(String value);

    @Description("Write disposition to use for BigQuery. Default: WRITE_APPEND")
    @Default.String("WRITE_APPEND")
    String getWriteDisposition();

    void setWriteDisposition(String writeDisposition);

    @Description(
        "The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
            + "format. If it doesn't exist, it will be created during pipeline execution.")
    String getOutputDeadletterTable();

    void setOutputDeadletterTable(String outputDeadletterTable);

    @Description(
        "The window duration in which data will be written. Defaults to 5m."
            + "Allowed formats are: "
            + "Ns (for seconds, example: 5s), "
            + "Nm (for minutes, example: 12m), "
            + "Nh (for hours, example: 2h).")
    @Default.String("1m")
    String getWindowDuration();

    void setWindowDuration(String windowDuration);

    @Description("The directory to write output files. Must end with a slash. ")
    String getOutputDirectory();

    void setOutputDirectory(String outputDirectory);

    @Description(
        "The filename prefix of the files to write to. Default file prefix is set to \"output-\". ")
    @Default.String("output-")
    String getOutputFilenamePrefix();

    void setOutputFilenamePrefix(String outputFilenamePrefix);

    @Description(
        "The maximum number of output shards produced while writing to FileSystem. Default number"
            + " is runner defined.")
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
      checkNotNull(schemaLocation,
          "schemaLocation argument of MessageGeneratorFn class cannot be null.");
      this.schemaLocation = schemaLocation;
    }

    @Setup
    public void setup() throws IOException {
      dataGenerator = new JsonDataGeneratorImpl();
      schema = SchemaUtils.getGcsFileAsString(schemaLocation);
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
