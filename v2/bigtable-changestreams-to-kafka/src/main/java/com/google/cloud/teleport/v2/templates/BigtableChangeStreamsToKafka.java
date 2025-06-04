/*
 * Copyright (C) 2025 Google LLC
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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.Timestamp;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions;
import com.google.cloud.teleport.v2.bigtable.utils.UnsupportedEntryException;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.cdc.dlq.StringDeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.kafka.options.KafkaWriteOptions;
import com.google.cloud.teleport.v2.kafka.options.SchemaRegistryOptions;
import com.google.cloud.teleport.v2.kafka.transforms.BinaryAvroSerializer;
import com.google.cloud.teleport.v2.kafka.transforms.JsonAvroSerializer;
import com.google.cloud.teleport.v2.kafka.utils.FileAwareFactoryFn;
import com.google.cloud.teleport.v2.kafka.utils.KafkaConfig;
import com.google.cloud.teleport.v2.kafka.utils.KafkaTopicUtils;
import com.google.cloud.teleport.v2.kafka.values.KafkaAuthenticationMethod;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters;
import com.google.cloud.teleport.v2.templates.model.BigtableSource;
import com.google.cloud.teleport.v2.templates.model.Mod;
import com.google.cloud.teleport.v2.templates.options.BigtableChangeStreamsToKafkaOptions;
import com.google.cloud.teleport.v2.templates.schemautils.KafkaUtils;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Throwables;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline ingests {@link ChangeStreamMutation} from Bigtable change stream. The {@link
 * ChangeStreamMutation} is then broken into {@link Mod}, which converted into ProducerRecord and
 * inserted into Kafka topic.
 */
@Template(
    name = "Bigtable_Change_Streams_to_Kafka",
    category = TemplateCategory.STREAMING,
    displayName = "Cloud Bigtable Change Streams to Apache Kafka",
    description =
        "Streaming pipeline. Streams Bigtable data change records and writes them into Kafka using Dataflow runner V2.",
    optionsClass = BigtableChangeStreamsToKafkaOptions.class,
    optionsOrder = {
      BigtableChangeStreamsToKafkaOptions.class,
      BigtableCommonOptions.ReadChangeStreamOptions.class,
      BigtableCommonOptions.ReadOptions.class,
      KafkaWriteOptions.class,
      SchemaRegistryOptions.class,
    },
    skipOptions = {
      "bigtableReadAppProfile",
      "bigtableAdditionalRetryCodes",
      "bigtableRpcAttemptTimeoutMs",
      "bigtableRpcTimeoutMs",
    },
    flexContainerName = "bigtable-changestreams-to-kafka",
    contactInformation = "https://cloud.google.com/support",
    streaming = true,
    supportsAtLeastOnce = true)
public final class BigtableChangeStreamsToKafka {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableChangeStreamsToKafka.class);

  public static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();
  public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(STRING_CODER, STRING_CODER);

  private static final String USE_RUNNER_V2_EXPERIMENT = "use_runner_v2";

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    LOG.info("Starting to replicate change records from Cloud Bigtable change streams to Kafka");

    BigtableChangeStreamsToKafkaOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BigtableChangeStreamsToKafkaOptions.class);
    run(options);
  }

  private static void validateOptions(BigtableChangeStreamsToKafkaOptions options) {
    // DLQ
    if (options.getDlqRetryMinutes() <= 0) {
      throw new IllegalArgumentException("dlqRetryMinutes must be positive.");
    }
    if (options.getDlqMaxRetries() < 0) {
      throw new IllegalArgumentException("dlqMaxRetries cannot be negative.");
    }

    // Kafka non-auth
    if (StringUtils.isEmpty(options.getWriteBootstrapServerAndTopic())) {
      throw new IllegalArgumentException("WriteBootstrapServerAndTopic must be set.");
    }
    // Kafka auth
    if (options.getKafkaWriteAuthenticationMethod().equals(KafkaAuthenticationMethod.SASL_PLAIN)) {
      checkArgument(
          options.getKafkaWriteUsernameSecretId().trim().length() > 0,
          "KafkaWriteUsernameSecretId required to access username for destination Kafka");
      checkArgument(
          options.getKafkaWritePasswordSecretId().trim().length() > 0,
          "KafkaWritePasswordSecretId required to access password for destination Kafka");
    } else if (options.getKafkaWriteAuthenticationMethod().equals(KafkaAuthenticationMethod.TLS)) {
      checkArgument(
          options.getKafkaWriteTruststoreLocation().trim().length() > 0,
          "KafkaWriteTruststoreLocation for trust store certificate required for ssl authentication");
      checkArgument(
          options.getKafkaWriteTruststorePasswordSecretId().trim().length() > 0,
          "KafkaWriteTruststorePasswordSecretId for trust store password required for accessing truststore");
      checkArgument(
          options.getKafkaWriteKeystoreLocation().trim().length() > 0,
          "KafkaWriteKeystoreLocation for key store location required for ssl authentication");
      checkArgument(
          options.getKafkaWriteKeystorePasswordSecretId().trim().length() > 0,
          "KafkaWriteKeystorePasswordSecretId for key store password required to access key store");
      checkArgument(
          options.getKafkaWriteKeyPasswordSecretId().trim().length() > 0,
          "KafkaWriteKeyPasswordSecretId for key password secret id version required for SSL authentication");
    } else if (options.getKafkaWriteAuthenticationMethod().equals(KafkaAuthenticationMethod.NONE)
        || options
            .getKafkaWriteAuthenticationMethod()
            .equals(KafkaAuthenticationMethod.APPLICATION_DEFAULT_CREDENTIALS)) {
      // No additional validation is required for these auth mechanisms since they don't depend on
      // any specific pipeline options.
    } else {
      throw new UnsupportedOperationException(
          "Kafka authentication method not supported: "
              + options.getKafkaWriteAuthenticationMethod());
    }

    if (options
        .getMessageFormat()
        .equals(KafkaTemplateParameters.MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT)) {
      // Schema Registry non-auth
      if (StringUtils.isEmpty(options.getSchemaRegistryConnectionUrl())) {
        throw new IllegalArgumentException(
            "SchemaRegistryConnectionUrl must be set when MessageFormat is set to '"
                + KafkaTemplateParameters.MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT
                + "'");
      }
      if (!options.getSchemaFormat().equals(KafkaTemplateParameters.SchemaFormat.SCHEMA_REGISTRY)) {
        throw new IllegalArgumentException(
            "SchemaFormat must be set to '"
                + KafkaTemplateParameters.SchemaFormat.SCHEMA_REGISTRY
                + "' when MessageFormat is set to '"
                + KafkaTemplateParameters.MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT
                + "'");
      }
      // Schema Registry auth
      if (options.getSchemaRegistryAuthenticationMode().equals(KafkaAuthenticationMethod.TLS)) {
        checkArgument(
            options.getSchemaRegistryTruststoreLocation().trim().length() > 0,
            "SchemaRegistryTruststoreLocation for trust store certificate required for ssl authentication");
        checkArgument(
            options.getSchemaRegistryTruststorePasswordSecretId().trim().length() > 0,
            "SchemaRegistryTruststorePasswordSecretId for trust store password required for accessing truststore");
        checkArgument(
            options.getSchemaRegistryKeystoreLocation().trim().length() > 0,
            "SchemaRegistryKeystoreLocation for key store location required for ssl authentication");
        checkArgument(
            options.getSchemaRegistryKeystorePasswordSecretId().trim().length() > 0,
            "SchemaRegistryKeystorePasswordSecretId for key store password required to access key store");
        checkArgument(
            options.getSchemaRegistryKeyPasswordSecretId().trim().length() > 0,
            "SchemaRegistryKeyPasswordSecretId for source key password secret id version required for SSL authentication");
      } else if (options
          .getSchemaRegistryAuthenticationMode()
          .equals(KafkaAuthenticationMethod.OAUTH)) {
        checkArgument(
            options.getSchemaRegistryOauthTokenEndpointUrl().trim().length() > 0,
            "SchemaRegistryOauthTokenEndpointUrl for OAuth token endpoint URL required for oauth authentication");
        checkArgument(
            options.getSchemaRegistryOauthClientId().trim().length() > 0,
            "SchemaRegistryOauthClientId for OAuth client ID required for oauth authentication");
        checkArgument(
            options.getSchemaRegistryOauthClientSecretId().trim().length() > 0,
            "SchemaRegistryOauthClientSecretId for OAuth client secret ID required for oauth authentication");
      } else if (options
          .getSchemaRegistryAuthenticationMode()
          .equals(KafkaAuthenticationMethod.NONE)) {
        // No additional validation is required since it doesn't depend on any specific pipeline
        // options.
      } else {
        throw new UnsupportedOperationException(
            "Schema Registry authentication method not supported: "
                + options.getSchemaRegistryAuthenticationMode());
      }
    }
  }

  private static void setOptions(BigtableChangeStreamsToKafkaOptions options) {
    options.setStreaming(true);
    options.setEnableStreamingEngine(true);

    // Add use_runner_v2 to the experiments option, since change streams connector is only supported
    // on Dataflow runner v2.
    List<String> experiments = options.getExperiments();
    if (experiments == null) {
      experiments = new ArrayList<>();
    }
    boolean hasUseRunnerV2 = false;
    for (String experiment : experiments) {
      if (experiment.equalsIgnoreCase(USE_RUNNER_V2_EXPERIMENT)) {
        hasUseRunnerV2 = true;
        break;
      }
    }
    if (!hasUseRunnerV2) {
      experiments.add(USE_RUNNER_V2_EXPERIMENT);
    }
    options.setExperiments(experiments);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(BigtableChangeStreamsToKafkaOptions options) {
    setOptions(options);
    validateOptions(options);

    String bigtableProject =
        StringUtils.isEmpty(options.getBigtableReadProjectId())
            ? options.getProject()
            : options.getBigtableReadProjectId();

    Instant startTimestamp;
    if (options.getBigtableChangeStreamStartTimestamp().isEmpty()) {
      startTimestamp = Instant.now();
    } else {
      Timestamp ts = Timestamp.parseTimestamp(options.getBigtableChangeStreamStartTimestamp());
      startTimestamp = Instant.ofEpochMilli(ts.getSeconds() * 1000 + ts.getNanos() / 1000000);
    }
    BigtableSource sourceInfo = buildBigtableSource(options);

    /*
     * Stages:
     * 1) Read {@link ChangeStreamMutation} from change stream.
     * 2) Create {@link FailsafeElement} of {@link Mod} JSON and merge from:
     *    a) Stream of Bigtable mutations.
     *    b) GCS dead letter queue.
     * 3) Convert {@link Mod} JSON into ProduceRecord and write it to Kafka.
     * 4) Write failures to GCS dead letter queue.
     */
    Pipeline pipeline = Pipeline.create(options);

    // Register the coders.
    pipeline
        .getCoderRegistry()
        .registerCoderForType(STRING_CODER.getEncodedTypeDescriptor(), STRING_CODER);
    pipeline
        .getCoderRegistry()
        .registerCoderForType(
            FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    // Configure DLQ.
    DeadLetterQueueManager dlqManager = buildDlqManager(options);
    String dlqDirectory = dlqManager.getRetryDlqDirectoryWithDateTime();
    String tempDlqDirectory = dlqManager.getRetryDlqDirectory() + "tmp/";
    if (options.getDisableDlqRetries()) {
      LOG.info(
          "Disabling retries for the DLQ, directly writing into severe DLQ: {}",
          dlqManager.getSevereDlqDirectoryWithDateTime());
      dlqDirectory = dlqManager.getSevereDlqDirectoryWithDateTime();
      tempDlqDirectory = dlqManager.getSevereDlqDirectory() + "tmp/";
    }

    // Configure input Bigtable change stream.
    BigtableIO.ReadChangeStream readChangeStream =
        BigtableIO.readChangeStream()
            .withChangeStreamName(options.getBigtableChangeStreamName())
            .withExistingPipelineOptions(
                options.getBigtableChangeStreamResume()
                    ? BigtableIO.ExistingPipelineOptions.RESUME_OR_FAIL
                    : BigtableIO.ExistingPipelineOptions.FAIL_IF_EXISTS)
            .withProjectId(bigtableProject)
            .withMetadataTableInstanceId(options.getBigtableChangeStreamMetadataInstanceId())
            .withInstanceId(options.getBigtableReadInstanceId())
            .withTableId(options.getBigtableReadTableId())
            .withAppProfileId(options.getBigtableChangeStreamAppProfile())
            .withStartTime(startTimestamp);
    if (!StringUtils.isBlank(options.getBigtableChangeStreamMetadataTableTableId())) {
      readChangeStream =
          readChangeStream.withMetadataTableTableId(
              options.getBigtableChangeStreamMetadataTableTableId());
    }

    // Read the input, serialize it, and wrap into FailsafeElement.
    PCollection<FailsafeElement<String, String>> inputFailsafe =
        pipeline
            .apply("Read from Cloud Bigtable Change Streams", readChangeStream)
            .apply(Values.create())
            .apply(
                "ChangeStreamMutation To Mod JSON",
                ParDo.of(new ChangeStreamMutationToModJsonFn(sourceInfo)))
            .apply(
                "Wrap In FailsafeElement",
                ParDo.of(
                    new DoFn<String, FailsafeElement<String, String>>() {
                      @ProcessElement
                      public void process(
                          @Element String input,
                          OutputReceiver<FailsafeElement<String, String>> receiver) {
                        receiver.output(FailsafeElement.of(input, input));
                      }
                    }));

    // Read retryable input from GCS dead letter queue.
    PCollectionTuple dlqModJson =
        dlqManager.getReconsumerDataTransform(
            pipeline.apply(dlqManager.dlqReconsumer(options.getDlqRetryMinutes())));
    PCollection<FailsafeElement<String, String>> retryableDlqFailsafeModJson;
    if (options.getDisableDlqRetries()) {
      retryableDlqFailsafeModJson = pipeline.apply(Create.empty(FAILSAFE_ELEMENT_CODER));
    } else {
      retryableDlqFailsafeModJson = dlqModJson.get(DeadLetterQueueManager.RETRYABLE_ERRORS);
    }

    // Merge the input sources.
    PCollection<FailsafeElement<String, String>> failsafeModJson =
        PCollectionList.of(inputFailsafe)
            .and(retryableDlqFailsafeModJson)
            .apply("Merge Source And DLQ Mod JSON", Flatten.pCollections());

    // Serialize the records and write them to Kafka.
    PCollection<FailsafeElement<String, String>> failedToPublish =
        failsafeModJson.apply("Publish Mod JSON To Kafka", ParDo.of(new WriteToKafkaFn(options)));

    // Send the failed records to dead letter queue.
    failedToPublish
        .apply(
            "Failed Mod JSON During Table Row Transformation",
            MapElements.via(new StringDeadLetterQueueSanitizer()))
        .apply(
            "Write Failed Mod JSON To DLQ",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqDirectory)
                .withTmpDirectory(tempDlqDirectory)
                .setIncludePaneInfo(true)
                .build());
    LOG.info(
        "DLQ manager severe DLQ directory with date time: {}",
        dlqManager.getSevereDlqDirectoryWithDateTime());
    LOG.info("DLQ manager severe DLQ directory: {}", dlqManager.getSevereDlqDirectory() + "tmp/");
    dlqModJson
        .get(DeadLetterQueueManager.PERMANENT_ERRORS)
        .setCoder(FAILSAFE_ELEMENT_CODER)
        .apply(
            "Write Mod JSON With Non-retriable Error To DLQ",
            MapElements.via(new StringDeadLetterQueueSanitizer()))
        .apply(
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqManager.getSevereDlqDirectoryWithDateTime())
                .withTmpDirectory(dlqManager.getSevereDlqDirectory() + "tmp/")
                .setIncludePaneInfo(true)
                .build());

    return pipeline.run();
  }

  private static DeadLetterQueueManager buildDlqManager(
      BigtableChangeStreamsToKafkaOptions options) {
    String tempLocation =
        options.as(DataflowPipelineOptions.class).getTempLocation().endsWith("/")
            ? options.as(DataflowPipelineOptions.class).getTempLocation()
            : options.as(DataflowPipelineOptions.class).getTempLocation() + "/";
    String dlqDirectory =
        options.getDlqDirectory().isEmpty() ? tempLocation + "dlq/" : options.getDlqDirectory();

    LOG.info("DLQ directory: {}", dlqDirectory);
    return DeadLetterQueueManager.create(dlqDirectory, options.getDlqMaxRetries());
  }

  private static BigtableSource buildBigtableSource(BigtableChangeStreamsToKafkaOptions options) {
    return new BigtableSource(
        options.getBigtableReadInstanceId(),
        options.getBigtableReadTableId(),
        StringUtils.isEmpty(options.getBigtableChangeStreamCharset())
            ? "UTF-8"
            : options.getBigtableChangeStreamCharset(),
        options.getBigtableChangeStreamIgnoreColumnFamilies(),
        options.getBigtableChangeStreamIgnoreColumns());
  }

  /**
   * DoFn that converts a {@link ChangeStreamMutation} to multiple {@link Mod} in serialized JSON
   * format.
   */
  static class ChangeStreamMutationToModJsonFn extends DoFn<ChangeStreamMutation, String> {
    private final BigtableSource sourceInfo;
    private transient Charset charset;

    ChangeStreamMutationToModJsonFn(BigtableSource source) {
      this.sourceInfo = source;
      this.charset = Charset.forName(sourceInfo.getCharset());
    }

    @Setup
    public void setup() {
      charset = Charset.forName(sourceInfo.getCharset());
    }

    private static String toJsonString(Mod mod, ChangeStreamMutation inputMutation) {
      try {
        return mod.toJson();
      } catch (IOException e) {
        // Ignore exception and print bad format.
        // This record will fail serialization in WriteToKafkaFn and end up in DLQ.
        return String.format("\"%s\"", inputMutation);
      }
    }

    @ProcessElement
    public void process(@Element ChangeStreamMutation input, OutputReceiver<String> receiver)
        throws Exception {
      for (Entry entry : input.getEntries()) {
        Mod mod;
        if (entry instanceof SetCell setCell) {
          if (sourceInfo.isIgnoredColumnFamily(setCell.getFamilyName())
              || sourceInfo.isIgnoredColumn(
                  setCell.getFamilyName(), setCell.getQualifier().toString(charset))) {
            continue;
          }
          mod = new Mod(sourceInfo, input, setCell);
        } else if (entry instanceof DeleteCells deleteCells) {
          if (sourceInfo.isIgnoredColumnFamily(deleteCells.getFamilyName())
              || sourceInfo.isIgnoredColumn(
                  deleteCells.getFamilyName(), deleteCells.getQualifier().toString(charset))) {
            continue;
          }
          mod = new Mod(sourceInfo, input, deleteCells);
        } else if (entry instanceof DeleteFamily deleteFamily) {
          if (sourceInfo.isIgnoredColumnFamily(deleteFamily.getFamilyName())) {
            continue;
          }
          mod = new Mod(sourceInfo, input, deleteFamily);
        } else {
          throw new UnsupportedEntryException(
              "Cloud Bigtable change stream entry of type "
                  + entry.getClass().getName()
                  + " is not supported. The entry was put into a DLQ directory. "
                  + "Please update your Dataflow template with the latest template version");
        }
        receiver.output(toJsonString(mod, input));
      }
    }
  }

  /** DoFn that serializes and writes {@link FailsafeElement FailsafeElements} to Kafka. */
  static class WriteToKafkaFn
      extends DoFn<FailsafeElement<String, String>, FailsafeElement<String, String>> {
    private final Map<String, Object> kafkaProducerConfig;
    private final KafkaUtils kafkaUtils;
    private final String destinationTopic;
    private final boolean base64EncodeByteFields;
    private transient Producer<byte[], GenericRecord> producer;

    WriteToKafkaFn(BigtableChangeStreamsToKafkaOptions options) {
      Class<?> serializerClass = getSerializerClass(options.getMessageFormat());
      base64EncodeByteFields = JsonAvroSerializer.class.equals(serializerClass);
      kafkaUtils = new KafkaUtils(buildBigtableSource(options).getCharset());
      List<String> destinationBootstrapServerAndTopicList =
          KafkaTopicUtils.getBootstrapServerAndTopic(
              options.getWriteBootstrapServerAndTopic(), options.getProject());
      destinationTopic = destinationBootstrapServerAndTopicList.get(1);
      kafkaProducerConfig = KafkaConfig.fromWriteOptions(options);
      kafkaProducerConfig.put(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, destinationBootstrapServerAndTopicList.get(0));
      kafkaProducerConfig.put(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      kafkaProducerConfig.put(
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerClass.getName());
      kafkaProducerConfig.putAll(KafkaConfig.fromSchemaRegistryOptions(options));
      kafkaProducerConfig.put(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
          options.getSchemaRegistryConnectionUrl());
    }

    @Setup
    public void setup() {
      producer = new FileAwareProducerFactoryFn().apply(kafkaProducerConfig);
    }

    @Teardown
    public void teardown() {
      if (producer != null) {
        this.producer.close(Duration.ofMinutes(5));
      }
    }

    @ProcessElement
    public void process(
        @Element FailsafeElement<String, String> input,
        OutputReceiver<FailsafeElement<String, String>> receiver) {
      try {
        String modChangeJson = Mod.fromJson(input.getPayload()).getChangeJson();
        ProducerRecord<byte[], GenericRecord> record =
            base64EncodeByteFields
                ? kafkaUtils.getProducerRecordWithBase64EncodedFields(
                    modChangeJson, destinationTopic)
                : kafkaUtils.getProducerRecord(modChangeJson, destinationTopic);
        producer.send(record).get();
      } catch (Exception e) {
        receiver.output(
            FailsafeElement.of(input)
                .setErrorMessage(e.getMessage())
                .setStacktrace(Throwables.getStackTraceAsString(e)));
      }
    }

    private static Class<? extends Serializer<? super GenericRecord>> getSerializerClass(
        String messageFormat) {
      return switch (messageFormat) {
        case KafkaTemplateParameters.MessageFormatConstants
            .AVRO_CONFLUENT_WIRE_FORMAT -> KafkaAvroSerializer.class;
        case KafkaTemplateParameters.MessageFormatConstants
            .AVRO_BINARY_ENCODING -> BinaryAvroSerializer.class;
        case KafkaTemplateParameters.MessageFormatConstants.JSON -> JsonAvroSerializer.class;
        default -> throw new IllegalArgumentException(
            "Unexpected message format: " + messageFormat);
      };
    }
  }

  public static class FileAwareProducerFactoryFn
      extends FileAwareFactoryFn<Producer<byte[], GenericRecord>> {
    public FileAwareProducerFactoryFn() {
      super("producer", null);
    }

    @Override
    protected Producer<byte[], GenericRecord> createObject(Map<String, Object> config) {
      return new KafkaProducer<>(config);
    }
  }
}
