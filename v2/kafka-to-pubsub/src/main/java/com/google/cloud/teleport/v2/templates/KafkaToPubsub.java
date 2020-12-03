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

import static com.google.cloud.teleport.v2.kafka.consumer.Utils.configureKafka;
import static com.google.cloud.teleport.v2.kafka.consumer.Utils.getKafkaCredentialsFromVault;
import static com.google.cloud.teleport.v2.transforms.FormatTransform.readFromKafka;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.options.KafkaToPubsubOptions;
import com.google.cloud.teleport.v2.transforms.FormatTransform;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link KafkaToPubsub} streaming pipeline reading json encoded data from Kafka and publishes
 * to Google Cloud PubSub. Input topics, output topic, Bootstrap servers are specified by the user
 * as template parameters. <br>
 * Kafka may be configured with SASL/SCRAM security mechanism over plain text or SSL encrypted
 * connection, in this case a Vault secret storage with credentials should be provided. URL to
 * credentials and Vault token are specified by the user as template parameters.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>Kafka Bootstrap Server(s).
 *   <li>Kafka Topic(s) exists.
 *   <li>The PubSub output topic exists.
 *   <li>(Optional) An existing HashiCorp Vault
 *   <li>(Optional) A configured secure SSL connection for Kafka
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT=id-of-my-project
 * BUCKET_NAME=my-bucket
 * REGION=my-region
 *
 * # Set containerization vars
 * IMAGE_NAME=my-image-name
 * TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
 * BASE_CONTAINER_IMAGE=my-base-container-image
 * BASE_CONTAINER_IMAGE_VERSION=my-base-container-image-version
 * TEMPLATE_PATH="gs://${BUCKET_NAME}/templates/kafka-pubsub.json"
 * TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
 *
 * # Create bucket in the cloud storage
 * gsutil mb gs://${BUCKET_NAME}
 *
 * # Go to the beam folder
 * cd /path/to/DataflowTemplates/v2
 *
 * <b>FLEX TEMPLATE</b>
 * # Assemble jar with dependencies
 * mvn package -am -pl kafka-to-pubsub
 *
 * # Go to the template folder
 * cd /path/to/DataflowTemplates/v2/kafka-to-pubsub
 *
 * # Build the flex template
 * gcloud dataflow flex-template build ${TEMPLATE_PATH} \
 *       --image-gcr-path "${TARGET_GCR_IMAGE}" \
 *       --sdk-language "JAVA" \
 *       --flex-template-base-image ${BASE_CONTAINER_IMAGE} \
 *       --metadata-file "src/main/resources/kafka_to_pubsub_metadata.json" \
 *       --jar "target/kafka-to-pubsub-1.0-SNAPSHOT.jar" \
 *       --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.google.cloud.teleport.v2.templates.KafkaToPubsub"
 *
 * # Execute template:
 * API_ROOT_URL="https://dataflow.googleapis.com"
 * TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/locations/${REGION}/flexTemplates:launch"
 * JOB_NAME="kafka-to-pubsub-`date +%Y%m%d-%H%M%S-%N`"
 *
 * time curl -X POST -H "Content-Type: application/json" \
 *         -H "Authorization: Bearer $(gcloud auth print-access-token)" \
 *         -d '
 *          {
*              "launch_parameter": {
*                  "jobName": "'$JOB_NAME'",
*                  "containerSpecGcsPath": "'$TEMPLATE_PATH'",
*                  "parameters": {
*                      "bootstrapServers": "broker_1:9091, broker_2:9092",
*                      "inputTopics": "topic1, topic2",
*                      "outputTopic": "projects/'$PROJECT'/topics/your-topic-name",
*                      "outputDeadLetterTopic": "projects/'$PROJECT'/topics/dead-letter-topic-name",
*                      "javascriptTextTransformGcsPath": "gs://path/to/udf",
*                      "javascriptTextTransformFunctionName": "your-js-function",
*                      "secretStoreUrl": "http(s)://host:port/path/to/credentials",
*                      "vaultToken": "your-token"
*                  }
 *              }
 *          }
 *         '
 *         "${TEMPLATES_LAUNCH_API}"
 * </pre>
 */
public class KafkaToPubsub {

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(KafkaToPubsub.class);

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    KafkaToPubsubOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaToPubsubOptions.class);

    run(options);
  }

  /**
   * Runs a pipeline which reads message from Kafka and writes to Pub/Sub.
   *
   * @param options arguments to the pipeline
   */
  public static PipelineResult run(KafkaToPubsubOptions options) {
    List<String> topicsList = new ArrayList<>(Arrays.asList(options.getInputTopics().split(",")));

    checkArgument(
        topicsList.size() > 0 && topicsList.stream().allMatch((s) -> s.trim().length() > 0),
        "inputTopics cannot be an empty string.");

    List<String> bootstrapServersList =
        new ArrayList<>(Arrays.asList(options.getBootstrapServers().split(",")));

    checkArgument(
        bootstrapServersList.size() > 0
            && bootstrapServersList.stream().allMatch((s) -> s.trim().length() > 0),
        "bootstrapServers cannot be an empty string.");

    // Configure Kafka consumer properties
    Map<String, Object> kafkaConfig = new HashMap<>();
    Map<String, String> sslConfig = null;
    if (options.getSecretStoreUrl() != null && options.getVaultToken() != null) {
      Map<String, Map<String, String>> credentials =
          getKafkaCredentialsFromVault(options.getSecretStoreUrl(), options.getVaultToken());
      kafkaConfig = configureKafka(credentials.get(KafkaPubsubConstants.KAFKA_CREDENTIALS));
      sslConfig = credentials.get(KafkaPubsubConstants.SSL_CREDENTIALS);
    } else {
      LOG.warn(
          "No information to retrieve Kafka credentials was provided. "
              + "Trying to initiate an unauthorized connection.");
    }

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

    TypeDescriptor<String> stringTypeDescriptor = TypeDescriptors.strings();

    LOG.info(
        "Starting Kafka-To-PubSub Pipeline with parameters bootstrap servers:{} input topics:{}"
            + " output pubsub topic:{} ",
        options.getBootstrapServers(),
        options.getInputTopics(),
        options.getOutputTopic());

    /*
     * Steps:
     *  1) Read messages in from Kafka
     *  2) Transform message payload via UDF
     *  3) Write successful records out to Pub/Sub
     *  4) Write failed records out to Pub/Sub dead-letter topic
     */
    PCollectionTuple appliedUdf =
        pipeline
            /* Step #1: Read messages in from Kafka */
            .apply(
                "readFromKafka",
                readFromKafka(options.getBootstrapServers(), topicsList, kafkaConfig, sslConfig))
            /* Step #2: Transform the Kafka Messages via UDF */
            .apply("applyUDF", new FormatTransform.UdfProcess(options));
    /* Step #3: Write the successful records out to Pub/Sub */
    appliedUdf
        .get(KafkaPubsubConstants.UDF_OUT)
        .apply(
            "getSuccessUDFOutElements",
            MapElements.into(stringTypeDescriptor).via(FailsafeElement::getPayload))
        .setCoder(NullableCoder.of(StringUtf8Coder.of()))
        .apply("writeSuccessMessages", PubsubIO.writeStrings().to(options.getOutputTopic()));

    /* Step #4: Write failed messages out to Pub/Sub */
    if (options.getOutputDeadLetterTopic() != null) {
      appliedUdf
          .get(KafkaPubsubConstants.UDF_DEADLETTER_OUT)
          .apply(
              "getFailedMessages",
              MapElements.into(TypeDescriptors.kvs(stringTypeDescriptor, stringTypeDescriptor))
                  .via(FailsafeElement::getOriginalPayload))
          .apply(
              "extractMessageValues",
              MapElements.into(stringTypeDescriptor).via(KV<String, String>::getValue))
          .setCoder(NullableCoder.of(StringUtf8Coder.of()))
          .apply(
              "writeFailureMessages",
              PubsubIO.writeStrings().to(options.getOutputDeadLetterTopic()));
    }

    return pipeline.run();
  }
}
