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
import com.google.cloud.teleport.v2.kafka.dlq.BigQueryDeadLetterQueue;
import com.google.cloud.teleport.v2.kafka.transforms.AvroDynamicTransform;
import com.google.cloud.teleport.v2.kafka.transforms.AvroTransform;
import com.google.cloud.teleport.v2.kafka.transforms.KafkaTransform;
import com.google.cloud.teleport.v2.kafka.utils.KafkaConfig;
import com.google.cloud.teleport.v2.kafka.utils.KafkaTopicUtils;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters.MessageFormatConstants;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters.SchemaFormat;
import com.google.cloud.teleport.v2.options.KafkaToBigQueryFlexOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryWriteUtils;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.transforms.ErrorConverters.WriteKafkaMessageErrors;
import com.google.cloud.teleport.v2.transforms.StringMessageToTableRow;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import com.google.cloud.teleport.v2.utils.MetadataValidator;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.ByteArrayCoder;
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
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link KafkaToBigQueryFlex} pipeline is a streaming pipeline which ingests text data from
 * Kafka, and outputs the resulting records to BigQuery. Any errors which occur in the
 * transformation of the data, or inserting into the output table will be inserted into a separate
 * errors table in BigQuery. Both output and error tables are specified by the user as parameters.
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
        "The Apache Kafka to BigQuery template is a streaming pipeline which ingests text data from Apache Kafka, and outputs the resulting records to BigQuery. "
            + "Any errors which occur in the transformation of the data, or inserting into the output table are inserted into a separate errors table in BigQuery. "
            + "For any errors which occur in the transformation of the data, the original records can be inserted into a separate Kafka topic. The template supports "
            + "reading a Kafka topic which contains single/multiple schema(s). It can write to a single or multiple BigQuery tables, depending on the schema of records. ",
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
    skipOptions = {"useStorageWriteApi"})
public class KafkaToBigQueryFlex {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(KafkaToBigQueryFlex.class);

  /** The tag for the main output of the json transformation. */
  public static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<>() {};

  /** The tag for the dead-letter output of the json to table row transform. */
  public static final TupleTag<FailsafeElement<KV<String, String>, String>>
      TRANSFORM_DEADLETTER_OUT = new TupleTag<>() {};

  /** String/String Coder for FailsafeElement. */
  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

  private static ErrorHandler<BadRecord, ?> errorHandler = new ErrorHandler.DefaultErrorHandler<>();
  private static BadRecordRouter badRecordRouter = BadRecordRouter.THROWING_ROUTER;

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for its execution to finish. If blocking execution is required, use the {@link
   * KafkaToBigQueryFlex#run(KafkaToBigQueryFlexOptions)} method to start the pipeline and invoke
   * {@code result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) throws Exception {
    UncaughtExceptionLogger.register();

    KafkaToBigQueryFlexOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaToBigQueryFlexOptions.class);

    run(options);
  }

  public static Boolean useErrorHandler(KafkaToBigQueryFlexOptions options) {
    return options.getUseBigQueryDLQ();
  }

  public static WriteResult processKafkaRecords(
      PCollection<KafkaRecord<byte[], byte[]>> kafkaRecords, KafkaToBigQueryFlexOptions options) {

    // Validate the pipeline options for MessageFormat and SchemaFormat.
    if (options.getMessageFormat().equals(MessageFormatConstants.AVRO_BINARY_ENCODING)
        && (options.getBinaryAvroSchemaPath() != null
            && options.getBinaryAvroSchemaPath().isBlank())) {
      throw new IllegalArgumentException(
          "Binary Avro Schema Path cannot be empty for AVRO_BINARY_ENCODING.");
    }

    if (options.getMessageFormat().equals(MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT)) {

      if ((options.getSchemaRegistryConnectionUrl() != null
              && options.getSchemaRegistryConnectionUrl().isBlank())
          && (options.getConfluentAvroSchemaPath() != null
              && options.getConfluentAvroSchemaPath().isBlank())) {
        throw new IllegalArgumentException(
            "Either Schema Registry Connection URL or Confluent Avro Schema Path must be provided for AVRO_CONFLUENT_WIRE_FORMAT.");
      }

      if (options.getSchemaFormat().equals(SchemaFormat.SINGLE_SCHEMA_FILE)) {
        if (!options.getConfluentAvroSchemaPath().isBlank()
            && (options.getOutputTableSpec() != null && options.getOutputTableSpec().isBlank())) {
          throw new IllegalArgumentException(
              "The outputTableSpec parameter is required when using the SINGLE_SCHEMA_FILE schema format.");
        }
      } else if (options.getSchemaFormat().equals(SchemaFormat.SCHEMA_REGISTRY)) {
        if (options.getSchemaRegistryConnectionUrl() != null
            && (options.getOutputDataset() != null && options.getOutputDataset().isBlank())) {
          throw new IllegalArgumentException(
              "An output BigQuery dataset is required. It will be used to create tables per schema.");
        }
      } else {
        throw new IllegalArgumentException(
            "Unsupported schemaFormat parameter value: " + options.getSchemaFormat());
      }
    }

    if (options.getMessageFormat().equals(MessageFormatConstants.AVRO_BINARY_ENCODING)
        && (!options.getBinaryAvroSchemaPath().isBlank())) {
      return handleAvroBinaryEncoding(kafkaRecords, options);
    } else if (options.getMessageFormat().equals(MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT)
        && (!options.getSchemaRegistryConnectionUrl().isBlank()
            || !options.getConfluentAvroSchemaPath().isBlank())) {
      return handleAvroConfluentWireFormat(kafkaRecords, options);
    } else {
      throw new IllegalArgumentException(
          "Message format " + options.getMessageFormat() + " is unsupported.");
    }
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
  public static PipelineResult run(KafkaToBigQueryFlexOptions options) throws Exception {

    // Enable Streaming Engine
    options.setEnableStreamingEngine(true);

    List<String> dataflowServiceOptions = options.getDataflowServiceOptions();
    if (dataflowServiceOptions == null) {
      dataflowServiceOptions = new ArrayList<>();
    }
    dataflowServiceOptions.add("enable_streaming_engine_resource_based_billing");
    options.setDataflowServiceOptions(dataflowServiceOptions);

    // Validate BQ STORAGE_WRITE_API options
    options.setUseStorageWriteApi(true);
    if (options.getStorageWriteApiTriggeringFrequencySec() == null) {
      options.setStorageWriteApiTriggeringFrequencySec(5);
    }
    BigQueryIOUtils.validateBQStorageApiOptionsStreaming(options);
    MetadataValidator.validate(options);

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    List<String> topicsList;
    String bootstrapServers;
    if (options.getReadBootstrapServerAndTopic() != null) {
      List<String> bootstrapServerAndTopicList =
          KafkaTopicUtils.getBootstrapServerAndTopic(
              options.getReadBootstrapServerAndTopic(), options.getProject());
      topicsList = List.of(bootstrapServerAndTopicList.get(1));
      bootstrapServers = bootstrapServerAndTopicList.get(0);
    } else {
      throw new IllegalArgumentException(
          "Please provide a valid bootstrap server which matches `[,:a-zA-Z0-9._-]+` and a topic which matches `[,a-zA-Z0-9._-]+`");
    }

    // Configure error handler for Dead letter queue
    if (options.getUseBigQueryDLQ()) {
      if (options.getOutputDeadletterTable() == null
          || options.getOutputDeadletterTable().isBlank()) {
        throw new IllegalArgumentException(
            "Please provide a valid BigQuery full qualified table name when using BigQuery"
                + "Dead letter queue");
      }
      badRecordRouter = BadRecordRouter.RECORDING_ROUTER;
      errorHandler =
          pipeline.registerBadRecordErrorHandler(
              BigQueryDeadLetterQueue.newBuilder()
                  .setTableName(options.getOutputDeadletterTable())
                  .build());
    }

    // Get the Kafka config
    Map<String, Object> kafkaConfig = KafkaConfig.fromReadOptions(options);

    /*
     * Steps:
     *  1) Read messages in from Kafka
     *  2) Transform the messages into TableRows
     *     - Transform message payload via UDF
     *     - Convert UDF result to TableRow objects
     *  3) Write successful records out to BigQuery
     *  4) Write failed records out to BigQuery
     */

    if (options.getMessageFormat() == null
        || options.getMessageFormat().equals(MessageFormatConstants.JSON)) {

      pipeline = runJsonPipeline(pipeline, options, topicsList, bootstrapServers, kafkaConfig);

    } else if (options.getMessageFormat().equals(MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT)
        || options.getMessageFormat().equals(MessageFormatConstants.AVRO_BINARY_ENCODING)) {
      pipeline = runAvroPipeline(pipeline, options, topicsList, bootstrapServers, kafkaConfig);

    } else {
      throw new IllegalArgumentException("Invalid format specified: " + options.getMessageFormat());
    }
    if (useErrorHandler(options)) {
      errorHandler.close();
    }
    return pipeline.run();
  }

  private static WriteResult handleAvroBinaryEncoding(
      PCollection<KafkaRecord<byte[], byte[]>> kafkaRecords, KafkaToBigQueryFlexOptions options) {
    WriteResult writeResult;
    BigQueryWriteUtils.BigQueryWrite bigQueryWrite;
    if (useErrorHandler(options)) {
      // BigQueryIO sets the BadRecordRouter to RecordingRouter even when the errorHandler is
      // DefaultErrorHandler(which is a no op). In this case, when the BadRecordRouter is
      // ThrowingRouter,
      // don't pass errorHandler to BigQueryIO.
      bigQueryWrite =
          BigQueryWriteUtils.BigQueryWrite.of(
              SchemaUtils.getAvroSchema(options.getBinaryAvroSchemaPath()),
              options.getOutputTableSpec(),
              options.getWriteDisposition(),
              options.getCreateDisposition(),
              options.getNumStorageWriteApiStreams(),
              options.getStorageWriteApiTriggeringFrequencySec(),
              options.getPersistKafkaKey(),
              options.getUseAutoSharding(),
              errorHandler);
    } else {
      bigQueryWrite =
          BigQueryWriteUtils.BigQueryWrite.of(
              SchemaUtils.getAvroSchema(options.getBinaryAvroSchemaPath()),
              options.getOutputTableSpec(),
              options.getWriteDisposition(),
              options.getCreateDisposition(),
              options.getNumStorageWriteApiStreams(),
              options.getStorageWriteApiTriggeringFrequencySec(),
              options.getPersistKafkaKey(),
              options.getUseAutoSharding());
    }
    writeResult =
        kafkaRecords
            .apply(
                AvroTransform.of(
                    options.getMessageFormat(),
                    options.getBinaryAvroSchemaPath(),
                    errorHandler,
                    badRecordRouter))
            .apply(bigQueryWrite);
    return writeResult;
  }

  private static WriteResult handleAvroConfluentWireFormat(
      PCollection<KafkaRecord<byte[], byte[]>> kafkaRecords, KafkaToBigQueryFlexOptions options) {
    if (options.getSchemaFormat().equals(SchemaFormat.SINGLE_SCHEMA_FILE)) {
      return handleSingleSchemaFileFormat(kafkaRecords, options);
    } else if (options.getSchemaFormat().equals(SchemaFormat.SCHEMA_REGISTRY)) {
      return handleSchemaRegistryFormat(kafkaRecords, options);
    } else {
      // throw error
      throw new IllegalArgumentException(
          "Message format " + options.getSchemaFormat() + " is unsupported.");
    }
  }

  private static WriteResult handleSingleSchemaFileFormat(
      PCollection<KafkaRecord<byte[], byte[]>> kafkaRecords, KafkaToBigQueryFlexOptions options) {
    if (!(options.getConfluentAvroSchemaPath() != null && options.getOutputTableSpec() != null)) {
      // TODO: Add error.
      throw new RuntimeException("");
    }
    WriteResult writeResult;
    BigQueryWriteUtils.BigQueryWrite bigQueryWrite;
    if (useErrorHandler(options)) {
      bigQueryWrite =
          BigQueryWriteUtils.BigQueryWrite.of(
              SchemaUtils.getAvroSchema(options.getConfluentAvroSchemaPath()),
              options.getOutputTableSpec(),
              options.getWriteDisposition(),
              options.getCreateDisposition(),
              options.getNumStorageWriteApiStreams(),
              options.getStorageWriteApiTriggeringFrequencySec(),
              options.getPersistKafkaKey(),
              options.getUseAutoSharding(),
              errorHandler);
    } else {
      bigQueryWrite =
          BigQueryWriteUtils.BigQueryWrite.of(
              SchemaUtils.getAvroSchema(options.getConfluentAvroSchemaPath()),
              options.getOutputTableSpec(),
              options.getWriteDisposition(),
              options.getCreateDisposition(),
              options.getNumStorageWriteApiStreams(),
              options.getStorageWriteApiTriggeringFrequencySec(),
              options.getPersistKafkaKey(),
              options.getUseAutoSharding());
    }
    writeResult =
        kafkaRecords
            .apply(
                AvroTransform.of(
                    options.getMessageFormat(),
                    options.getConfluentAvroSchemaPath(),
                    errorHandler,
                    badRecordRouter))
            .apply(bigQueryWrite);
    return writeResult;
  }

  private static WriteResult handleSchemaRegistryFormat(
      PCollection<KafkaRecord<byte[], byte[]>> kafkaRecords, KafkaToBigQueryFlexOptions options) {
    if (!(options.getSchemaRegistryConnectionUrl() != null && options.getOutputDataset() != null)) {
      throw new RuntimeException(
          "Missing required parameters: Schema Registry URL and/or Output Dataset");
    }
    WriteResult writeResult;
    BigQueryWriteUtils.BigQueryDynamicWrite bigQueryWrite;
    if (useErrorHandler(options)) {
      bigQueryWrite =
          BigQueryWriteUtils.BigQueryDynamicWrite.of(
              options.getOutputProject(),
              options.getOutputDataset(),
              options.getBqTableNamePrefix(),
              options.getWriteDisposition(),
              options.getCreateDisposition(),
              options.getNumStorageWriteApiStreams(),
              options.getStorageWriteApiTriggeringFrequencySec(),
              options.getPersistKafkaKey(),
              options.getUseAutoSharding(),
              errorHandler);
    } else {
      bigQueryWrite =
          BigQueryWriteUtils.BigQueryDynamicWrite.of(
              options.getOutputProject(),
              options.getOutputDataset(),
              options.getBqTableNamePrefix(),
              options.getWriteDisposition(),
              options.getCreateDisposition(),
              options.getNumStorageWriteApiStreams(),
              options.getStorageWriteApiTriggeringFrequencySec(),
              options.getPersistKafkaKey(),
              options.getUseAutoSharding());
    }
    writeResult =
        kafkaRecords
            .apply(
                AvroDynamicTransform.of(
                    options.getSchemaRegistryConnectionUrl(),
                    KafkaConfig.fromSchemaRegistryOptions(options),
                    errorHandler,
                    badRecordRouter))
            .apply(bigQueryWrite);
    return writeResult;
  }

  public static Pipeline runAvroPipeline(
      Pipeline pipeline,
      KafkaToBigQueryFlexOptions options,
      List<String> topicsList,
      String bootstrapServers,
      Map<String, Object> kafkaConfig)
      throws Exception {

    if (options.getMessageFormat().equals(MessageFormatConstants.AVRO_BINARY_ENCODING)
        && options.getBinaryAvroSchemaPath() == null) {
      throw new IllegalArgumentException(
          "Avro schema is needed in order to read non confluent wire format messages.");
    }
    if (options.getMessageFormat().equals(MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT)
        && options.getSchemaRegistryConnectionUrl() == null
        && options.getConfluentAvroSchemaPath() == null) {
      throw new IllegalArgumentException(
          "Schema Registry Connection URL or Avro schema is needed in order to read confluent wire format messages.");
    }

    PCollection<KafkaRecord<byte[], byte[]>> kafkaRecords;

    kafkaRecords =
        pipeline
            /*
             * Step #1: Read messages in from Kafka and convert to GenericRecords wrap in FailsafeElement
             */
            .apply(
                "ReadBytesFromKafka",
                KafkaTransform.readBytesFromKafka(
                    bootstrapServers, topicsList, kafkaConfig, options.getEnableCommitOffsets()))
            .setCoder(
                KafkaRecordCoder.of(NullableCoder.of(ByteArrayCoder.of()), ByteArrayCoder.of()));

    WriteResult writeResult = null;
    writeResult = processKafkaRecords(kafkaRecords, options);
    return pipeline;
  }

  public static Pipeline runJsonPipeline(
      Pipeline pipeline,
      KafkaToBigQueryFlexOptions options,
      List<String> topicsList,
      String bootstrapServers,
      Map<String, Object> kafkaConfig) {

    // Register the coder for pipeline
    FailsafeElementCoder<KV<String, String>, String> coder =
        FailsafeElementCoder.of(
            KvCoder.of(
                NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of())),
            NullableCoder.of(StringUtf8Coder.of()));

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    PCollectionTuple convertedTableRows;
    convertedTableRows =
        pipeline
            /*
             * Step #1: Read messages in from Kafka
             */
            .apply(
                "ReadFromKafka",
                KafkaTransform.readStringFromKafka(
                    bootstrapServers,
                    topicsList,
                    kafkaConfig,
                    null,
                    options.getEnableCommitOffsets()))

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
          .apply("PrintInsertionFailedRecords", ParDo.of(new ThrowErrorFn<>()));
    }

    if (options.getOutputDeadletterTable() != null) {
      /*
       * Step #5: Insert records that failed BigQuery inserts into a dead-letter table.
       */
      failedInserts.apply(
          "WriteInsertionFailedRecords",
          ErrorConverters.WriteStringMessageErrors.newBuilder()
              .setErrorRecordsTable(ObjectUtils.firstNonNull(options.getOutputDeadletterTable()))
              .setErrorRecordsTableSchema(SchemaUtils.DEADLETTER_SCHEMA)
              .build());
    } else {
      failedInserts.apply(
          "PrintInsertionFailedRecords", ParDo.of(new ThrowErrorFn<String, String>()));
    }

    return pipeline;
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
