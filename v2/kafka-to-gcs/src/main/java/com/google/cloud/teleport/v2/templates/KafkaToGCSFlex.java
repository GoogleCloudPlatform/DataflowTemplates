package com.google.cloud.teleport.v2.templates;

import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.options.KafkaToGCSOptions;
import com.google.cloud.teleport.v2.transforms.WriteTransform;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

@Template(
        name = "Kafka_to_GCSFlex",
        category = TemplateCategory.STREAMING,
        displayName = "Kafka to Cloud Storage",
        description =
                "A streaming pipeline which ingests data from Kafka and writes to a pre-existing Cloud"
                        + " Storage bucket with a variety of file types.",
        optionsClass = KafkaToGCSOptions.class,
        flexContainerName = "kafka-to-gcs-flex",
        contactInformation = "https://cloud.google.com/support",
        hidden = true,
        streaming = true)
public class KafkaToGCSFlex {
    /* Logger for class */
    private static final Logger LOG = LoggerFactory.getLogger(KafkaToGCSFlex.class);
    private static final String topicsSplitDelimiter = ",";

    public static class ClientAuthConfig {
        public static ImmutableMap<String, Object> get(String username, String password){
            ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
            properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            properties.put(
                    SaslConfigs.SASL_JAAS_CONFIG,
                    "org.apache.kafka.common.security.plain.PlainLoginModule required"
                            + " username=\'"
                            +  username
                            + "\'"
                            + " password=\'"
                            + password
                            + "\';");
            return properties.buildOrThrow();
        }
    }

    public static PipelineResult run(KafkaToGCSOptions options) throws UnsupportedOperationException {

        // Create the Pipeline
        Pipeline pipeline = Pipeline.create(options);

        PCollection<KafkaRecord<byte[], byte[]>> kafkaRecord;

        List<String> topics = new ArrayList<>(Arrays.asList(options.getInputTopics().split(topicsSplitDelimiter)));

        options.setStreaming(true);

        String kafkaSaslPlainUserName = SecretManagerUtils.getSecret(options.getUserNameSecretID());
        String kafkaSaslPlainPassword = SecretManagerUtils.getSecret(options.getPasswordSecretID());

        Map<String, Object> kafkaConfig = new HashMap<>();
        // TODO: Make this configurable.
        kafkaConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaConfig.putAll(ClientAuthConfig.get(kafkaSaslPlainUserName, kafkaSaslPlainPassword));

        // Step 1: Read from Kafka as bytes.
        kafkaRecord = pipeline.apply(
                KafkaIO.<byte[], byte[]>read()
                        .withBootstrapServers(options.getBootstrapServers())
                        .withTopics(topics)
                        .withKeyDeserializer(ByteArrayDeserializer.class)
                        .withValueDeserializer(ByteArrayDeserializer.class)
                        .withConsumerConfigUpdates(kafkaConfig)
        );

        kafkaRecord.apply(
                WriteTransform
                        .newBuilder()
                        .setOptions(options)
                        .build());
        return pipeline.run();
    }

    public static void validateOptions(KafkaToGCSOptions options) {
        if (options.getUserNameSecretID().isBlank()
        || options.getPasswordSecretID().isBlank()) {
            throw new IllegalArgumentException(
                    "No Information to retrieve Kafka SASL_PLAIN username/password was provided."
            );
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

    public static void main(String[] args) throws RestClientException, IOException {

        KafkaToGCSOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaToGCSOptions.class);
        validateOptions(options);
        run(options);

    }
}
