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

import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.kafka.dlq.KafkaDeadLetterQueue;
import com.google.cloud.teleport.v2.kafka.dlq.KafkaDeadLetterQueueOptions;
import com.google.cloud.teleport.v2.kafka.options.KafkaReadOptions;
import com.google.cloud.teleport.v2.transforms.WriteToGCSAvro;
import com.google.cloud.teleport.v2.transforms.WriteToGCSText;
import com.google.cloud.teleport.v2.transforms.WriteTransform;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

@Template(
    name = "Kafka_to_Gcs_Flex",
    category = TemplateCategory.STREAMING,
    displayName = "Kafka to Cloud Storage",
    description =
        "A streaming pipeline which ingests data from Kafka and writes to a pre-existing Cloud"
            + " Storage bucket with a variety of file types.",
    optionsClass = KafkaToGcsFlex.KafkaToGcsOptions.class,
    flexContainerName = "kafka-to-gcs-flex",
    contactInformation = "https://cloud.google.com/support",
    hidden = true,
    streaming = true,
    requirements = {"The output Google Cloud Storage directory must exist."})
public class KafkaToGcsFlex {
  /**
   * The {@link KafkaToGcsOptions} interface provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface KafkaToGcsOptions
      extends PipelineOptions,
          DataflowPipelineOptions,
          KafkaReadOptions,
          WriteToGCSText.WriteToGCSTextOptions,
          WriteToGCSAvro.WriteToGCSAvroOptions,
          KafkaDeadLetterQueueOptions {

    @TemplateParameter.Enum(
        order = 3,
        groupName = "MessageFormat",
        enumOptions = {
          @TemplateParameter.TemplateEnumOption("TEXT"),
          @TemplateParameter.TemplateEnumOption("AVRO"),
          @TemplateParameter.TemplateEnumOption("PARQUET")
        },
        description = "File format of the desired output files. (TEXT, AVRO or PARQUET)",
        helpText =
            "The file format of the desired output files. Can be TEXT, AVRO or PARQUET. Defaults to TEXT")
    @Default.String("TEXT")
    String getOutputFileFormat();

    void setOutputFileFormat(String outputFileFormat);

    @TemplateParameter.Duration(
        order = 4,
        optional = true,
        description = "Window duration",
        helpText =
            "The window duration/size in which data will be written to Cloud Storage. Allowed formats are: Ns (for "
                + "seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h).",
        example = "5m")
    @Default.String("5m")
    String getWindowDuration();

    void setWindowDuration(String windowDuration);

    @TemplateParameter.Text(
        order = 5,
        groupName = "MessageFormat",
        optional = true,
        description = "Schema Registry URL for decoding Confluent Wire Format messages",
        helpText =
            "Provide the full URL of your Schema Registry (e.g., http://your-registry:8081) if your Kafka messages are encoded in Confluent Wire Format. Leave blank for other formats.")
    String getSchemaRegistryURL();

    void setSchemaRegistryURL(String schemaRegistryURL);

    @TemplateParameter.Text(
        order = 6,
        optional = true,
        groupName = "MessageFormat",
        description = "Path to your Avro schema file (required for Avro formats)",
        example = "gs://<bucket_name>/schema1.avsc",
        helpText =
            "Specify the Google Cloud Storage path (or other accessible path) to the Avro schema (.avsc) file that defines the structure of your Kafka messages.")
    String getSchemaPath();

    void setSchemaPath(String schema);

    @TemplateParameter.Enum(
        order = 7,
        groupName = "MessageFormat",
        enumOptions = {
          @TemplateParameter.TemplateEnumOption("CONFLUENT_WIRE_FORMAT"),
          @TemplateParameter.TemplateEnumOption("AVRO_BINARY_ENCODING"),
          @TemplateParameter.TemplateEnumOption("AVRO_SINGLE_OBJECT_ENCODING")
        },
        optional = true,
        description = "The format in which your Kafka messages are encoded",
        helpText =
            "Choose the encoding used for your Kafka messages:\n"
                + " - CONFLUENT_WIRE_FORMAT: Confluent format, requires a Schema Registry URL.\n"
                + " - AVRO_BINARY_ENCODING: Avro's compact binary format.\n"
                + " - AVRO_SINGLE_OBJECT_ENCODING: Avro, but each message is a single Avro object.")
    @Default.String("CONFLUENT_WIRE_FORMAT")
    String getMessageFormat();

    void setMessageFormat(String messageFormat);

    @TemplateParameter.Text(
        order = 8,
        groupName = "Kafka SASL_PLAIN Authentication parameter",
        description =
            "Username to be used with SASL_PLAIN mechanism for Kafka, stored in Google Cloud Secret Manager",
        helpText =
            "Secret Manager secret ID for the SASL_PLAIN username. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}",
        example = "projects/your-project-id/secrets/your-secret/versions/your-secret-version",
        optional = true)
    @Default.String("")
    String getUserNameSecretID();

    void setUserNameSecretID(String userNameSecretID);

    @TemplateParameter.Text(
        order = 9,
        groupName = "Kafka SASL_PLAIN Authentication parameter",
        description =
            "Password to be used with SASL_PLAIN mechanism for Kafka, stored in Google Cloud Secret Manager",
        helpText =
            "Secret Manager secret ID for the SASL_PLAIN password. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}",
        example = "projects/your-project-id/secrets/your-secret/versions/your-secret-version",
        optional = true)
    @Default.String("")
    String getPasswordSecretID();

    void setPasswordSecretID(String passwordSecretID);
  }

  private static final String topicsSplitDelimiter = ",";
  private static boolean useKafkaAuth = true;
  // TODO: Add a DefaultErrorSink when no DLQ is specified.
  private static List<ErrorHandler<BadRecord, ?>> badRecordErrorHandlers = new ArrayList<>();

  public static class ClientAuthConfig {
    public static ImmutableMap<String, Object> getSaslPlainConfig(
        String username, String password) {
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
  }

  public static PipelineResult run(KafkaToGcsOptions options) {

    // Create the Pipeline
    Pipeline pipeline = Pipeline.create(options);

    PCollection<KafkaRecord<byte[], byte[]>> kafkaRecord;

    List<String> topics =
        new ArrayList<>(Arrays.asList(options.getKafkaReadTopics().split(topicsSplitDelimiter)));

    options.setStreaming(true);

    Map<String, Object> kafkaConfig = new HashMap<>();
    // Set offset to either earliest or latest.
    kafkaConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, options.getKafkaReadOffset());
    // Authenticate to Kafka only when user provides authentication params.
    if (useKafkaAuth) {
      String kafkaSaslPlainUserName = SecretManagerUtils.getSecret(options.getUserNameSecretID());
      String kafkaSaslPlainPassword = SecretManagerUtils.getSecret(options.getPasswordSecretID());
      kafkaConfig.putAll(
          ClientAuthConfig.getSaslPlainConfig(kafkaSaslPlainUserName, kafkaSaslPlainPassword));
    }

    if (options.getEnableKafkaDlq()) {
      ErrorHandler<BadRecord, ?> kafkaErrorHandler =
          pipeline.registerBadRecordErrorHandler(
              KafkaDeadLetterQueue.newBuilder()
                  .setTopic(options.getDeadLetterQueueKafkaTopic())
                  .setBootStrapServers(options.getReadBootstrapServers())
                  .setConfig(kafkaConfig)
                  .build());
      badRecordErrorHandlers.add(kafkaErrorHandler);
    }

    // Read from Kafka as bytes.
    kafkaRecord =
        pipeline.apply(
            KafkaIO.<byte[], byte[]>read()
                .withBootstrapServers(options.getReadBootstrapServers())
                .withTopics(topics)
                .withKeyDeserializerAndCoder(
                    ByteArrayDeserializer.class, NullableCoder.of(ByteArrayCoder.of()))
                .withValueDeserializerAndCoder(
                    ByteArrayDeserializer.class, NullableCoder.of(ByteArrayCoder.of()))
                .withConsumerConfigUpdates(kafkaConfig));

    // Write to Avro, Json or Parquet using WriteTransform.
    kafkaRecord.apply(
        WriteTransform.newBuilder()
            .setOptions(options)
            .setErrorHandlers(badRecordErrorHandlers)
            .build());

    // Close all the error handlers at the end of the pipeline.
    try {
      for (ErrorHandler<BadRecord, ?> errorHandler : badRecordErrorHandlers) {
        errorHandler.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return pipeline.run();
  }

  public static void validateAuthOptions(KafkaToGcsOptions options) {
    // Authenticate to Kafka brokers without any auth config. This can be the case when
    // the dataflow pipeline and Kafka broker is on the same network.
    if (options.getUserNameSecretID().isBlank() && options.getPasswordSecretID().isBlank()) {
      useKafkaAuth = false;
      return;
    }

    if ((options.getUserNameSecretID().isBlank() && !options.getPasswordSecretID().isBlank())
        || (options.getPasswordSecretID().isBlank() && !options.getUserNameSecretID().isBlank())) {
      throw new IllegalArgumentException(
          "Both username secret ID and password secret ID should be provided together or left null.");
    }

    if (!SecretVersionName.isParsableFrom(options.getUserNameSecretID())) {
      throw new IllegalArgumentException(
          "Provided Secret Username ID must be in the form"
              + " projects/{project}/secrets/{secret}/versions/{secret_version}");
    }
    if (!SecretVersionName.isParsableFrom(options.getPasswordSecretID())) {
      throw new IllegalArgumentException(
          "Provided Secret Password ID must be in the form"
              + " projects/{project}/secrets/{secret}/versions/{secret_version}");
    }
  }

  public static void main(String[] args) {
    KafkaToGcsOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaToGcsOptions.class);
    validateAuthOptions(options);
    run(options);
  }
}
