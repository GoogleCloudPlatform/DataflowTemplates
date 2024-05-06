/*
 * Copyright (C) 2024 Google LLC
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

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.kafka.transforms.KafkaTransform;
import com.google.cloud.teleport.v2.options.KafkaToBigQueryFlexOptions;
import com.google.cloud.teleport.v2.transforms.AvroDynamicTransform;
import com.google.cloud.teleport.v2.transforms.AvroTransform;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.transforms.ErrorConverters.WriteKafkaMessageErrors;
import com.google.cloud.teleport.v2.transforms.StringMessageToTableRow;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import com.google.cloud.teleport.v2.utils.MetadataValidator;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link KafkaToBigQueryFlex} pipeline is a streaming pipeline which ingests text data from
 * Kafka, executes a UDF, and outputs the resulting records to BigQuery. Any errors which occur in
 * the transformation of the data, execution of the UDF, or inserting into the output table will be
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
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/kafka-to-bigquery/README_Kafka_to_BigQuery.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Kafka_to_BigQuery_Flex",
    category = TemplateCategory.STREAMING,
    displayName = "Kafka to BigQuery",
    description =
        "The Apache Kafka to BigQuery template is a streaming pipeline which ingests text data from Apache Kafka, executes a user-defined function (UDF), and outputs the resulting records to BigQuery. "
            + "Any errors which occur in the transformation of the data, execution of the UDF, or inserting into the output table are inserted into a separate errors table in BigQuery. "
            + "If the errors table does not exist prior to execution, then it is created.",
    optionsClass = KafkaToBigQueryFlexOptions.class,
    flexContainerName = "kafka-to-bigquery-flex",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/kafka-to-bigquery",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The output BigQuery table must exist.",
      "The Apache Kafka broker server must be running and be reachable from the Dataflow worker machines.",
      "The Apache Kafka topics must exist and the messages must be encoded in a valid JSON format."
    },
    streaming = true,
    hidden = true)
public class KafkaToBigQueryFlex {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(KafkaToBigQueryFlex.class);

  /** The tag for the main output of the json transformation. */
  public static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<TableRow>() {};

  /** The tag for the dead-letter output of the json to table row transform. */
  public static final TupleTag<FailsafeElement<KV<String, String>, String>>
      TRANSFORM_DEADLETTER_OUT = new TupleTag<FailsafeElement<KV<String, String>, String>>() {};

  /** The default suffix for error tables if dead letter table is not specified. */
  private static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = "_error_records";

  /** String/String Coder for FailsafeElement. */
  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * KafkaToBigQueryFlex#run(KafkaToBQFlexOptions)} method to start the pipeline and invoke {@code
   * result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) throws IOException, RestClientException {
    UncaughtExceptionLogger.register();

    KafkaToBigQueryFlexOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaToBigQueryFlexOptions.class);

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
  public static PipelineResult run(KafkaToBigQueryFlexOptions options)
      throws IOException, RestClientException {

    // Validate BQ STORAGE_WRITE_API options
    BigQueryIOUtils.validateBQStorageApiOptionsStreaming(options);
    MetadataValidator.validate(options);

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    List<String> topicsList;
    if (options.getKafkaReadTopics() != null) {
      topicsList = new ArrayList<>(Arrays.asList(options.getKafkaReadTopics().split(",")));
    } else {
      throw new IllegalArgumentException("Please Provide --kafkaReadTopic");
    }
    String bootstrapServers;
    if (options.getReadBootstrapServers() != null) {
      bootstrapServers = options.getReadBootstrapServers();
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

    ImmutableMap<String, Object> kafkaConfig =
        ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, options.getKafkaReadOffset());

    if (options.getKafkaReadUsernameSecretId() != null
        && options.getKafkaReadPasswordSecretId() != null) {

      String username = SecretManagerUtils.getSecret(options.getKafkaReadUsernameSecretId());
      String password = SecretManagerUtils.getSecret(options.getKafkaReadPasswordSecretId());

      kafkaConfig =
          ImmutableMap.<String, Object>builder()
              .putAll(kafkaConfig)
              .putAll(setClientAuthConfig(username, password))
              .build();
    }

    if (options.getMessageFormat() == null || options.getMessageFormat().equals("JSON")) {

      return runJsonPipeline(pipeline, options, topicsList, bootstrapServers, kafkaConfig);

    } else if (options.getMessageFormat() == null || options.getMessageFormat().equals("AVRO")) {

      return runAvroPipeline(pipeline, options, topicsList, bootstrapServers, kafkaConfig);

    } else {
      throw new IllegalArgumentException("Invalid format specified: " + options.getMessageFormat());
    }
  }

  public static PipelineResult runAvroPipeline(
      Pipeline pipeline,
      KafkaToBigQueryFlexOptions options,
      List<String> topicsList,
      String bootstrapServers,
      Map<String, Object> kafkaConfig)
      throws IOException, RestClientException {

    if (options.getAvroFormat().equals("NON_WIRE_FORMAT") && options.getAvroSchemaPath() == null) {
      throw new IllegalArgumentException(
          "Avro schema is needed in order to read non confluent wire format messages.");
    }
    if (options.getAvroFormat().equals("CONFLUENT_WIRE_FORMAT")
        && options.getSchemaRegistryConnectionUrl() == null
        && options.getAvroSchemaPath() == null) {
      throw new IllegalArgumentException(
          "Schema Registry Connection URL OR Avro schema is needed in order to read confluent wire format messages.");
    }

    PCollection<KafkaRecord<byte[], byte[]>> kafkaRecords;

    kafkaRecords =
        pipeline
            /*
             * Step #1: Read messages in from Kafka and convert to GenericRecords wrap in FailsafeElement
             */
            .apply(
                "ReadBytesFromKafka",
                KafkaTransform.readBytesFromKafka(bootstrapServers, topicsList, kafkaConfig, null))
            .setCoder(
                KafkaRecordCoder.of(NullableCoder.of(ByteArrayCoder.of()), ByteArrayCoder.of()));

    WriteResult writeResult = null;

    if (options.getAvroFormat().equals("NON_WIRE_FORMAT") && options.getAvroSchemaPath() != null) {

      throw new UnsupportedOperationException("Only Confluent Wire Format is supported");
      // writeResult = kafkaRecords.apply(AvroTransform.of(options));

    } else {

      if (options.getAvroSchemaPath() != null && options.getOutputTableSpec() == null) {
        throw new IllegalArgumentException("An output BigQuery table is required.");
      }

      if (options.getAvroSchemaPath() != null && options.getOutputTableSpec() != null) {
        writeResult = kafkaRecords.apply(AvroTransform.of(options));
      }

      if (options.getSchemaRegistryConnectionUrl() != null && options.getOutputDataset() == null) {
        throw new IllegalArgumentException(
            "An output BigQuery dataset is required. It will be used to create tables per schema.");
      }

      if (options.getSchemaRegistryConnectionUrl() != null && options.getOutputDataset() != null) {
        writeResult = kafkaRecords.apply(AvroDynamicTransform.of(options));
      }
    }

    /*
     * Step #2: Elements that failed inserts into BigQuery
     * are extracted and converted to FailsafeElement.
     */
    PCollection<FailsafeElement<String, String>> failedInserts =
        BigQueryIOUtils.writeResultToBigQueryInsertErrors(writeResult, options)
            .apply(
                "WrapInsertionErrors",
                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                    .via(KafkaToBigQueryFlex::wrapBigQueryInsertError))
            .setCoder(FAILSAFE_ELEMENT_CODER);

    if (options.getOutputDeadletterTable() != null) {
      /*
       * Step #3: Insert records that failed BigQuery inserts into a deadletter table.
       */
      failedInserts.apply(
          "WriteInsertionFailedRecords",
          ErrorConverters.WriteStringMessageErrors.newBuilder()
              .setErrorRecordsTable(ObjectUtils.firstNonNull(options.getOutputDeadletterTable()))
              .setErrorRecordsTableSchema(SchemaUtils.DEADLETTER_SCHEMA)
              .build());
    } else {
      failedInserts.apply("PrintInsertionFailedRecords", ParDo.of(new ThrowErrorFn()));
    }

    return pipeline.run();
  }

  public static PipelineResult runJsonPipeline(
      Pipeline pipeline,
      KafkaToBigQueryFlexOptions options,
      List<String> topicsList,
      String bootstrapServers,
      Map<String, Object> kafkaConfig) {

    PCollectionTuple convertedTableRows;
    convertedTableRows =
        pipeline
            /*
             * Step #1: Read messages in from Kafka
             */
            .apply(
                "ReadFromKafka",
                KafkaTransform.readStringFromKafka(bootstrapServers, topicsList, kafkaConfig, null))

            /*
             * Step #2: Transform the Kafka Messages into TableRows
             */
            .apply("ConvertMessageToTableRow", new StringMessageToTableRow());
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
                    .via(KafkaToBigQueryFlex::wrapBigQueryInsertError))
            .setCoder(FAILSAFE_ELEMENT_CODER);

    if (options.getOutputDeadletterTable() != null) {
      /*
       * Step #4: Write failed records out to BigQuery
       */
      PCollectionList.of(convertedTableRows.get(TRANSFORM_DEADLETTER_OUT))
          .apply("Flatten", Flatten.pCollections())
          .apply(
              "WriteTransformationFailedRecords",
              WriteKafkaMessageErrors.newBuilder()
                  .setErrorRecordsTable(
                      ObjectUtils.firstNonNull(options.getOutputDeadletterTable()))
                  .setErrorRecordsTableSchema(SchemaUtils.DEADLETTER_SCHEMA)
                  .build());
    } else {
      PCollectionList.of(convertedTableRows.get(TRANSFORM_DEADLETTER_OUT))
          .apply("Flatten", Flatten.pCollections())
          .apply(
              "PrintInsertionFailedRecords",
              ParDo.of(new ThrowErrorFn<KV<String, String>, String>()));
    }

    if (options.getOutputDeadletterTable() != null) {
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
    } else {
      failedInserts.apply(
          "PrintInsertionFailedRecords", ParDo.of(new ThrowErrorFn<String, String>()));
    }

    return pipeline.run();
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

  /**
   * Method to create Kafka Client Authentication Config with the given username and password.
   *
   * @param username Kafka username.
   * @param password Kafka password.
   * @return ImmutableMap object.
   */
  static ImmutableMap<String, Object> setClientAuthConfig(String username, String password) {
    ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
    properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    properties.put(
        SaslConfigs.SASL_JAAS_CONFIG,
        "org.apache.kafka.common.security.plain.PlainLoginModule required"
            + " username=\'"
            + username
            + "\'"
            + " password=\'"
            + password
            + "\';");
    return properties.buildOrThrow();
  }

  static class ThrowErrorFn<T, W> extends DoFn<FailsafeElement<T, W>, FailsafeElement<T, W>> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      FailsafeElement<T, W> element = context.element();
      // TODO: Logging every exception might overload Google Cloud Logging API. Find a better way to
      // log these errors.
      LOG.error(element.toString() + element.getErrorMessage() + element.getStacktrace());
      context.output(element);
    }
  }
}
