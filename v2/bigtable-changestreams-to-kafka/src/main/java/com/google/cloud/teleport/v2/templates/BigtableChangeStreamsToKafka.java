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

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions;
import com.google.cloud.teleport.v2.bigtable.utils.UnsupportedEntryException;
import com.google.cloud.teleport.v2.cdc.dlq.StringDeadLetterQueueSanitizer;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.kafka.options.KafkaWriteOptions;
import com.google.cloud.teleport.v2.kafka.options.SchemaRegistryOptions;
import com.google.cloud.teleport.v2.kafka.transforms.BinaryAvroSerializer;
import com.google.cloud.teleport.v2.kafka.transforms.JsonAvroSerializer;
import com.google.cloud.teleport.v2.kafka.utils.FileAwareProducerFactoryFn;
import com.google.cloud.teleport.v2.kafka.utils.FileAwareSchemaRegistryFactoryFn;
import com.google.cloud.teleport.v2.kafka.utils.KafkaConfig;
import com.google.cloud.teleport.v2.kafka.utils.KafkaTopicUtils;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters;
import com.google.cloud.teleport.v2.templates.model.BigtableSource;
import com.google.cloud.teleport.v2.templates.model.Mod;
import com.google.cloud.teleport.v2.templates.options.BigtableChangeStreamsToKafkaOptions;
import com.google.cloud.teleport.v2.templates.options.BigtableChangeStreamsToKafkaOptionsUtils;
import com.google.cloud.teleport.v2.templates.schemautils.KafkaUtils;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Throwables;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
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
        "Streaming pipeline. Streams Bigtable data change records and writes them into Kafka.\n\n"
            + "Changes are delivered to Kafka at least once: when Dataflow retries work, for example "
            + "after a failed write or a worker restart, some changes can be written more than once. "
            + "A duplicate is an exact copy of an earlier message, so consumers that need each change "
            + "only once must remove duplicates themselves.",
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

  private static final String DLQ_DATETIME_SUBDIRECTORY = "YYYY/MM/dd/HH/mm/";

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

  private static void setOptions(BigtableChangeStreamsToKafkaOptions options) {
    options.setStreaming(true);
    options.setEnableStreamingEngine(true);

    BigtableChangeStreamsToKafkaOptionsUtils.validate(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(BigtableChangeStreamsToKafkaOptions options) {
    setOptions(options);

    String bigtableProject =
        StringUtils.isEmpty(options.getBigtableReadProjectId())
            ? options.getProject()
            : options.getBigtableReadProjectId();
    Instant startTimestamp = BigtableChangeStreamsToKafkaOptionsUtils.getStartTimestamp(options);
    BigtableSource sourceInfo =
        BigtableChangeStreamsToKafkaOptionsUtils.buildBigtableSource(options);

    /*
     * Stages:
     * 1) Read {@link ChangeStreamMutation} from change stream.
     * 2) Create {@link FailsafeElement} of {@link Mod} JSON.
     * 3) Convert {@link Mod} JSON into serialized Kafka records.
     * 4) Write the Kafka records to Kafka using {@link KafkaIO}.
     * 5) Write records that failed conversion or serialization to GCS dead letter queue.
     */
    Pipeline pipeline = Pipeline.create(options);

    // Register the coders.
    pipeline
        .getCoderRegistry()
        .registerCoderForType(
            FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    // Configure DLQ.
    String dlqDirectory = buildDlqDirectory(options);
    LOG.info("DLQ directory: {}", dlqDirectory);

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
    PCollection<FailsafeElement<String, String>> failsafeModJson =
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

    // Convert the records into serialized Kafka records.
    List<String> bootstrapServerAndTopic =
        KafkaTopicUtils.getBootstrapServerAndTopic(
            options.getWriteBootstrapServerAndTopic(), options.getProject());
    String bootstrapServers = bootstrapServerAndTopic.get(0);
    String destinationTopic = bootstrapServerAndTopic.get(1);
    Map<String, Object> producerConfig = buildProducerConfig(options);
    PCollectionTuple kafkaRecords =
        failsafeModJson.apply(
            "Mod JSON To Kafka Record",
            ParDo.of(
                    new ModJsonToKafkaRecordFn(
                        destinationTopic, options.getMessageFormat(), producerConfig))
                .withOutputTags(
                    ModJsonToKafkaRecordFn.KAFKA_RECORDS,
                    TupleTagList.of(ModJsonToKafkaRecordFn.FAILED_MOD_JSON)));

    // Write the records to Kafka. A failed write fails the bundle, which is then retried.
    kafkaRecords
        .get(ModJsonToKafkaRecordFn.KAFKA_RECORDS)
        .apply(
            "Write To Kafka",
            KafkaIO.<byte[], byte[]>write()
                .withBootstrapServers(bootstrapServers)
                .withTopic(destinationTopic)
                .withKeySerializer(ByteArraySerializer.class)
                .withValueSerializer(ByteArraySerializer.class)
                .withProducerConfigUpdates(producerConfig)
                .withProducerFactoryFn(new FileAwareProducerFactoryFn()));

    // Send the records that could not be converted or serialized to dead letter queue.
    kafkaRecords
        .get(ModJsonToKafkaRecordFn.FAILED_MOD_JSON)
        .apply(
            "Failed Mod JSON During Table Row Transformation",
            MapElements.via(new StringDeadLetterQueueSanitizer()))
        .apply(
            "Write Failed Mod JSON To DLQ",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqDirectory + DLQ_DATETIME_SUBDIRECTORY)
                .withTmpDirectory(dlqDirectory + "tmp/")
                .setIncludePaneInfo(true)
                .build());

    return pipeline.run();
  }

  /**
   * Returns the DLQ directory, ending with a slash. Unless set explicitly, it's a directory unique
   * to this launch of the job in the temp location, so that different jobs don't share it.
   */
  private static String buildDlqDirectory(BigtableChangeStreamsToKafkaOptions options) {
    if (StringUtils.isNotEmpty(options.getDlqDirectory())) {
      return StringUtils.appendIfMissing(options.getDlqDirectory(), "/");
    }
    String tempLocation =
        StringUtils.defaultString(options.as(DataflowPipelineOptions.class).getTempLocation());
    tempLocation = StringUtils.appendIfMissing(tempLocation, "/");
    String randomSuffix = UUID.randomUUID().toString().substring(0, 8);
    return tempLocation + "dlq/" + options.getJobName() + "-" + randomSuffix + "/";
  }

  private static Map<String, Object> buildProducerConfig(
      BigtableChangeStreamsToKafkaOptions options) {
    Map<String, Object> config = KafkaConfig.fromWriteOptions(options);
    config.putAll(KafkaConfig.fromSchemaRegistryOptions(options));
    if (StringUtils.isNotBlank(options.getSchemaRegistryConnectionUrl())) {
      config.put(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
          options.getSchemaRegistryConnectionUrl());
    }
    // KafkaIO limits the retries to 3 by default. Restore Kafka's default of retrying until
    // delivery.timeout.ms passes, so that short broker unavailability doesn't fail the bundle.
    config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    // Allow the biggest records that Bigtable cells can produce. The Kafka broker and topic must
    // allow them as well, or writing them fails.
    config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Mod.MAX_SERIALIZED_SIZE_BYTES);
    config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, (long) Mod.MAX_SERIALIZED_SIZE_BYTES);
    return config;
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
        // This record will fail conversion in ModJsonToKafkaRecordFn and end up in DLQ.
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
                  + " is not supported. "
                  + "Please update your Dataflow template with the latest template version");
        }
        receiver.output(toJsonString(mod, input));
      }
    }
  }

  /**
   * DoFn that converts {@link Mod} JSON in {@link FailsafeElement FailsafeElements} to serialized
   * Kafka records keyed by the row key. Records that can't be converted or serialized are sent to
   * the {@link #FAILED_MOD_JSON} output.
   */
  static class ModJsonToKafkaRecordFn
      extends DoFn<FailsafeElement<String, String>, KV<byte[], byte[]>> {
    static final TupleTag<KV<byte[], byte[]>> KAFKA_RECORDS = new TupleTag<KV<byte[], byte[]>>() {};
    static final TupleTag<FailsafeElement<String, String>> FAILED_MOD_JSON =
        new TupleTag<FailsafeElement<String, String>>() {};

    private final KafkaUtils kafkaUtils;
    private final String destinationTopic;
    private final boolean base64EncodeByteFields;
    private final Class<? extends Serializer<? super GenericRecord>> valueSerializerClass;
    private final Map<String, Object> valueSerializerConfig;
    private transient Serializer<? super GenericRecord> valueSerializer;

    /**
     * @param valueSerializerConfig the config to {@link Serializer#configure configure} the value
     *     serializer with, the same one that a Kafka producer would pass to it
     */
    ModJsonToKafkaRecordFn(
        String destinationTopic, String messageFormat, Map<String, Object> valueSerializerConfig) {
      this.kafkaUtils = new KafkaUtils();
      this.destinationTopic = destinationTopic;
      this.valueSerializerClass = getSerializerClass(messageFormat);
      this.base64EncodeByteFields = JsonAvroSerializer.class.equals(valueSerializerClass);
      this.valueSerializerConfig = valueSerializerConfig;
    }

    @Setup
    public void setup() {
      // Replace the references to GCS files and Secret Manager secrets, as the producer does.
      Map<String, Object> config =
          new FileAwareSchemaRegistryFactoryFn().apply(valueSerializerConfig);
      valueSerializer = Utils.newInstance(valueSerializerClass);
      valueSerializer.configure(config, false);
    }

    @Teardown
    public void teardown() {
      if (valueSerializer != null) {
        valueSerializer.close();
      }
    }

    @ProcessElement
    public void process(
        @Element FailsafeElement<String, String> input, MultiOutputReceiver receiver) {
      KV<byte[], byte[]> kafkaRecord;
      try {
        String modChangeJson = Mod.fromJson(input.getPayload()).getChangeJson();
        ProducerRecord<byte[], GenericRecord> record =
            base64EncodeByteFields
                ? kafkaUtils.getProducerRecordWithBase64EncodedFields(
                    modChangeJson, destinationTopic)
                : kafkaUtils.getProducerRecord(modChangeJson, destinationTopic);
        kafkaRecord =
            KV.of(
                record.key(),
                valueSerializer.serialize(record.topic(), record.headers(), record.value()));
      } catch (Exception e) {
        receiver
            .get(FAILED_MOD_JSON)
            .output(
                FailsafeElement.of(input)
                    .setErrorMessage(e.getMessage())
                    .setStacktrace(Throwables.getStackTraceAsString(e)));
        return;
      }
      receiver.get(KAFKA_RECORDS).output(kafkaRecord);
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
}
