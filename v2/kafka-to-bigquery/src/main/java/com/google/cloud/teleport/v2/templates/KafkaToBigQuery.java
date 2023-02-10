/*
 * Copyright (C) 2019 Google LLC
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

import static com.google.cloud.teleport.v2.kafka.transforms.KafkaTransform.readFromKafka;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.kafka.options.KafkaReadOptions;
import com.google.cloud.teleport.v2.options.BigQueryStorageApiStreamingOptions;
import com.google.cloud.teleport.v2.templates.KafkaToBigQuery.KafkaToBQOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.transforms.ErrorConverters.WriteKafkaMessageErrors;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import com.google.cloud.teleport.v2.utils.MetadataValidator;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link KafkaToBigQuery} pipeline is a streaming pipeline which ingests text data from Kafka,
 * executes a UDF, and outputs the resulting records to BigQuery. Any errors which occur in the
 * transformation of the data, execution of the UDF, or inserting into the output table will be
 * inserted into a separate errors table in BigQuery. The errors table will be created if it does
 * not exist prior to execution. Both output and error tables are specified by the user as
 * parameters.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The Kafka topic exists and the message is encoded in a valid JSON format.
 *   <li>The BigQuery output table exists.
 *   <li>The Kafka brokers are reachable from the Dataflow worker machines.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 *
 * # Set some environment variables
 * PROJECT=my-project
 * TEMP_BUCKET=my-temp-bucket
 * OUTPUT_TABLE=${PROJECT}:my_dataset.my_table
 * TOPICS=my-topics
 * JS_PATH=my-js-path-on-gcs
 * JS_FUNC_NAME=my-js-func-name
 * BOOTSTRAP=my-comma-separated-bootstrap-servers
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
 * # Create an image spec in GCS that contains the path to the image
 * {
 *    "docker_template_spec": {
 *       "docker_image": $TARGET_GCR_IMAGE
 *     }
 *  }
 *
 * # Execute template:
 * API_ROOT_URL="https://dataflow.googleapis.com"
 * TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/templates:launch"
 * JOB_NAME="kafka-to-bigquery`date +%Y%m%d-%H%M%S-%N`"
 *
 * time curl -X POST -H "Content-Type: application/json"     \
 *     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
 *     "${TEMPLATES_LAUNCH_API}"`
 *     `"?validateOnly=false"`
 *     `"&dynamicTemplate.gcsPath=${TEMP_BUCKET}/path/to/image-spec"`
 *     `"&dynamicTemplate.stagingLocation=${TEMP_BUCKET}/staging" \
 *     -d '
 *      {
 *       "jobName":"'$JOB_NAME'",
 *       "parameters": {
 *           "outputTableSpec":"'$OUTPUT_TABLE'",
 *           "inputTopics":"'$TOPICS'",
 *           "javascriptTextTransformGcsPath":"'$JS_PATH'",
 *           "javascriptTextTransformFunctionName":"'$JS_FUNC_NAME'",
 *           "bootstrapServers":"'$BOOTSTRAP'"
 *        }
 *       }
 *      '
 * </pre>
 */
@Template(
    name = "Kafka_to_BigQuery",
    category = TemplateCategory.STREAMING,
    displayName = "Kafka to BigQuery",
    description =
        "A streaming pipeline which ingests data in JSON format from Kafka, performs a transform"
            + " via a user defined JavaScript function, and writes to a pre-existing BigQuery"
            + " table.",
    optionsClass = KafkaToBQOptions.class,
    flexContainerName = "kafka-to-bigquery",
    contactInformation = "https://cloud.google.com/support")
public class KafkaToBigQuery {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(KafkaToBigQuery.class);

  /** The tag for the main output for the UDF. */
  private static final TupleTag<FailsafeElement<KV<String, String>, String>> UDF_OUT =
      new TupleTag<FailsafeElement<KV<String, String>, String>>() {};

  /** The tag for the main output of the json transformation. */
  static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<TableRow>() {};

  /** The tag for the dead-letter output of the udf. */
  static final TupleTag<FailsafeElement<KV<String, String>, String>> UDF_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<KV<String, String>, String>>() {};

  /** The tag for the dead-letter output of the json to table row transform. */
  static final TupleTag<FailsafeElement<KV<String, String>, String>> TRANSFORM_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<KV<String, String>, String>>() {};

  /** The default suffix for error tables if dead letter table is not specified. */
  private static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = "_error_records";

  /** String/String Coder for FailsafeElement. */
  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

  /**
   * The {@link KafkaToBQOptions} class provides the custom execution options passed by the executor
   * at the command-line.
   */
  public interface KafkaToBQOptions
      extends KafkaReadOptions,
          JavascriptTextTransformerOptions,
          BigQueryStorageApiStreamingOptions {

    @TemplateParameter.BigQueryTable(
        order = 1,
        description = "BigQuery output table",
        helpText =
            "BigQuery table location to write the output to. The name should be in the format "
                + "<project>:<dataset>.<table_name>. The table's schema must match input objects.")
    @Required
    String getOutputTableSpec();

    void setOutputTableSpec(String outputTableSpec);

    /**
     * Get bootstrap server across releases.
     *
     * @deprecated This method is no longer acceptable to get bootstrap servers.
     *     <p>Use {@link KafkaToBQOptions#getReadBootstrapServers()} instead.
     */
    @TemplateParameter.Text(
        order = 2,
        optional = true,
        regexes = {"[,:a-zA-Z0-9._-]+"},
        description = "Kafka Bootstrap Server list",
        helpText = "Kafka Bootstrap Server list, separated by commas.",
        example = "localhost:9092,127.0.0.1:9093")
    @Deprecated
    String getBootstrapServers();

    /**
     * Get bootstrap server across releases.
     *
     * @deprecated This method is no longer acceptable to set bootstrap servers.
     *     <p>Use {@link KafkaToBQOptions#setReadBootstrapServers()} instead.
     */
    @Deprecated
    void setBootstrapServers(String bootstrapServers);

    /**
     * Get bootstrap server across releases.
     *
     * @deprecated This method is no longer acceptable to get Input topics.
     *     <p>Use {@link KafkaToBQOptions#getKafkaReadTopics()} instead.
     */
    @Deprecated
    @TemplateParameter.Text(
        order = 3,
        regexes = {"[,a-zA-Z0-9._-]+"},
        description = "Kafka topic(s) to read the input from",
        helpText = "Kafka topic(s) to read the input from.",
        example = "topic1,topic2")
    String getInputTopics();

    /**
     * Get bootstrap server across releases.
     *
     * @deprecated This method is no longer acceptable to set Input topics.
     *     <p>Use {@link KafkaToBQOptions#getKafkaReadTopics()} instead.
     */
    @Deprecated
    void setInputTopics(String inputTopics);

    @TemplateParameter.BigQueryTable(
        order = 4,
        optional = true,
        description = "The dead-letter table name to output failed messages to BigQuery",
        helpText =
            "Messages failed to reach the output table for all kind of reasons (e.g., mismatched"
                + " schema, malformed json) are written to this table. If it doesn't exist, it will"
                + " be created during pipeline execution.",
        example = "your-project-id:your-dataset.your-table-name")
    String getOutputDeadletterTable();

    void setOutputDeadletterTable(String outputDeadletterTable);
  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * KafkaToBigQuery#run(KafkaToBQOptions)} method to start the pipeline and invoke {@code
   * result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    KafkaToBQOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaToBQOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options. This method does not wait until the
   * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
   * object to block until the pipeline is finished running if blocking programmatic execution is
   * required.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(KafkaToBQOptions options) {

    // Validate BQ STORAGE_WRITE_API options
    BigQueryIOUtils.validateBQStorageApiOptionsStreaming(options);
    MetadataValidator.validate(options);

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // Register the coder for pipeline
    FailsafeElementCoder<KV<String, String>, String> coder =
        FailsafeElementCoder.of(
            KvCoder.of(
                NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of())),
            NullableCoder.of(StringUtf8Coder.of()));

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    List<String> topicsList;
    if (options.getKafkaReadTopics() != null) {
      topicsList = new ArrayList<>(Arrays.asList(options.getKafkaReadTopics().split(",")));
    } else if (options.getInputTopics() != null) {
      topicsList = new ArrayList<>(Arrays.asList(options.getInputTopics().split(",")));
    } else {
      throw new IllegalArgumentException("Please Provide --kafkaReadTopic");
    }
    String bootstrapServers;
    if (options.getReadBootstrapServers() != null) {
      bootstrapServers = options.getReadBootstrapServers();
    } else if (options.getBootstrapServers() != null) {
      bootstrapServers = options.getBootstrapServers();
    } else {
      throw new IllegalArgumentException("Please Provide --bootstrapServers");
    }
    /*
     * Steps:
     *  1) Read messages in from Kafka
     *  2) Transform the messages into TableRows
     *     - Transform message payload via UDF
     *     - Convert UDF result to TableRow objects
     *  3) Write successful records out to BigQuery
     *  4) Write failed records out to BigQuery
     */

    PCollectionTuple convertedTableRows =
        pipeline
            /*
             * Step #1: Read messages in from Kafka
             */
            .apply(
                "ReadFromKafka",
                readFromKafka(
                    bootstrapServers,
                    topicsList,
                    ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                    null))

            /*
             * Step #2: Transform the Kafka Messages into TableRows
             */
            .apply("ConvertMessageToTableRow", new MessageToTableRow(options));

    /*
     * Step #3: Write the successful records out to BigQuery
     */
    WriteResult writeResult =
        convertedTableRows
            .get(TRANSFORM_OUT)
            .apply(
                "WriteSuccessfulRecords",
                BigQueryIO.writeTableRows()
                    .withoutValidation()
                    .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                    .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                    .withExtendedErrorInfo()
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                    .to(options.getOutputTableSpec()));

    /*
     * Step 3 Contd.
     * Elements that failed inserts into BigQuery are extracted and converted to FailsafeElement
     */
    PCollection<FailsafeElement<String, String>> failedInserts =
        BigQueryIOUtils.writeResultToBigQueryInsertErrors(writeResult, options)
            .apply(
                "WrapInsertionErrors",
                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                    .via(KafkaToBigQuery::wrapBigQueryInsertError))
            .setCoder(FAILSAFE_ELEMENT_CODER);

    /*
     * Step #4: Write failed records out to BigQuery
     */
    PCollectionList.of(convertedTableRows.get(UDF_DEADLETTER_OUT))
        .and(convertedTableRows.get(TRANSFORM_DEADLETTER_OUT))
        .apply("Flatten", Flatten.pCollections())
        .apply(
            "WriteTransformationFailedRecords",
            WriteKafkaMessageErrors.newBuilder()
                .setErrorRecordsTable(
                    ObjectUtils.firstNonNull(
                        options.getOutputDeadletterTable(),
                        options.getOutputTableSpec() + DEFAULT_DEADLETTER_TABLE_SUFFIX))
                .setErrorRecordsTableSchema(SchemaUtils.DEADLETTER_SCHEMA)
                .build());

    /*
     * Step #5: Insert records that failed BigQuery inserts into a deadletter table.
     */
    failedInserts.apply(
        "WriteInsertionFailedRecords",
        ErrorConverters.WriteStringMessageErrors.newBuilder()
            .setErrorRecordsTable(
                ObjectUtils.firstNonNull(
                    options.getOutputDeadletterTable(),
                    options.getOutputTableSpec() + DEFAULT_DEADLETTER_TABLE_SUFFIX))
            .setErrorRecordsTableSchema(SchemaUtils.DEADLETTER_SCHEMA)
            .build());

    return pipeline.run();
  }

  /**
   * The {@link MessageToTableRow} class is a {@link PTransform} which transforms incoming Kafka
   * Message objects into {@link TableRow} objects for insertion into BigQuery while applying a UDF
   * to the input. The executions of the UDF and transformation to {@link TableRow} objects is done
   * in a fail-safe way by wrapping the element with it's original payload inside the {@link
   * FailsafeElement} class. The {@link MessageToTableRow} transform will output a {@link
   * PCollectionTuple} which contains all output and dead-letter {@link PCollection}.
   *
   * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
   *
   * <ul>
   *   <li>{@link KafkaToBigQuery#UDF_OUT} - Contains all {@link FailsafeElement} records
   *       successfully processed by the UDF.
   *   <li>{@link KafkaToBigQuery#UDF_DEADLETTER_OUT} - Contains all {@link FailsafeElement} records
   *       which failed processing during the UDF execution.
   *   <li>{@link KafkaToBigQuery#TRANSFORM_OUT} - Contains all records successfully converted from
   *       JSON to {@link TableRow} objects.
   *   <li>{@link KafkaToBigQuery#TRANSFORM_DEADLETTER_OUT} - Contains all {@link FailsafeElement}
   *       records which couldn't be converted to table rows.
   * </ul>
   */
  static class MessageToTableRow
      extends PTransform<PCollection<KV<String, String>>, PCollectionTuple> {

    private final KafkaToBQOptions options;

    MessageToTableRow(KafkaToBQOptions options) {
      this.options = options;
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<String, String>> input) {

      PCollectionTuple udfOut =
          input
              // Map the incoming messages into FailsafeElements so we can recover from failures
              // across multiple transforms.
              .apply("MapToRecord", ParDo.of(new MessageToFailsafeElementFn()))
              .apply(
                  "InvokeUDF",
                  FailsafeJavascriptUdf.<KV<String, String>>newBuilder()
                      .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                      .setFunctionName(options.getJavascriptTextTransformFunctionName())
                      .setSuccessTag(UDF_OUT)
                      .setFailureTag(UDF_DEADLETTER_OUT)
                      .build());

      // Convert the records which were successfully processed by the UDF into TableRow objects.
      PCollectionTuple jsonToTableRowOut =
          udfOut
              .get(UDF_OUT)
              .apply(
                  "JsonToTableRow",
                  FailsafeJsonToTableRow.<KV<String, String>>newBuilder()
                      .setSuccessTag(TRANSFORM_OUT)
                      .setFailureTag(TRANSFORM_DEADLETTER_OUT)
                      .build());

      // Re-wrap the PCollections so we can return a single PCollectionTuple
      return PCollectionTuple.of(UDF_OUT, udfOut.get(UDF_OUT))
          .and(UDF_DEADLETTER_OUT, udfOut.get(UDF_DEADLETTER_OUT))
          .and(TRANSFORM_OUT, jsonToTableRowOut.get(TRANSFORM_OUT))
          .and(TRANSFORM_DEADLETTER_OUT, jsonToTableRowOut.get(TRANSFORM_DEADLETTER_OUT));
    }
  }

  /**
   * The {@link MessageToFailsafeElementFn} wraps an Kafka Message with the {@link FailsafeElement}
   * class so errors can be recovered from and the original message can be output to a error records
   * table.
   */
  static class MessageToFailsafeElementFn
      extends DoFn<KV<String, String>, FailsafeElement<KV<String, String>, String>> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      KV<String, String> message = context.element();
      context.output(FailsafeElement.of(message, message.getValue()));
    }
  }

  /**
   * Method to wrap a {@link BigQueryInsertError} into a {@link FailsafeElement}.
   *
   * @param insertError BigQueryInsert error.
   * @return FailsafeElement object.
   */
  protected static FailsafeElement<String, String> wrapBigQueryInsertError(
      BigQueryInsertError insertError) {

    FailsafeElement<String, String> failsafeElement;
    try {

      failsafeElement =
          FailsafeElement.of(
              insertError.getRow().toPrettyString(), insertError.getRow().toPrettyString());
      failsafeElement.setErrorMessage(insertError.getError().toPrettyString());

    } catch (IOException e) {
      LOG.error("Failed to wrap BigQuery insert error.");
      throw new RuntimeException(e);
    }
    return failsafeElement;
  }
}
