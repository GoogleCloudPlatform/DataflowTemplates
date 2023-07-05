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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.KafkaToGCSOptions;
import com.google.cloud.teleport.v2.transforms.FileFormatFactory;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link KafkaToGCS} pipeline reads messages from Kafka topic(s) and stores to Google Cloud
 * Storage bucket in user specified format. The sink data can be stored in a Text, Avro or a Parquet
 * File Format.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>Kafka Bootstrap Server(s).
 *   <li>Kafka Topic(s) exists.
 *   <li>Google Cloud Storage output bucket exists.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/kafka-to-gcs/README_Kafka_to_GCS.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Kafka_to_GCS",
    category = TemplateCategory.STREAMING,
    displayName = "Kafka to Cloud Storage",
    description =
        "A streaming pipeline which ingests data from Kafka and writes to a pre-existing Cloud"
            + " Storage bucket with a variety of file types.",
    optionsClass = KafkaToGCSOptions.class,
    flexContainerName = "kafka-to-gcs",
    contactInformation = "https://cloud.google.com/support")
public class KafkaToGCS {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(KafkaToGCS.class);

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    KafkaToGCSOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaToGCSOptions.class);

    run(options);
  }

  /**
   * Runs a pipeline which reads message from Kafka and writes it to GCS.
   *
   * @param options arguments to the pipeline
   */
  public static PipelineResult run(KafkaToGCSOptions options) {

    String outputFileFormat = options.getOutputFileFormat().toUpperCase();
    LOG.info("Requested File Format is " + outputFileFormat);

    final String errorMessage =
        "Invalid output format:"
            + outputFileFormat
            + ". Supported output formats:"
            + FileFormatFactory.EXPECTED_FILE_FORMAT;

    // Call the function to check File Format passed by user is valid.
    if (!WriteToGCSUtility.isValidFileFormat(outputFileFormat)) {
      LOG.info(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    List<String> topicsList = new ArrayList<>(Arrays.asList(options.getInputTopics().split(",")));

    checkArgument(
        topicsList.size() > 0 && topicsList.get(0).length() > 0,
        "inputTopics cannot be an empty string. ");

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    /**
     * Steps: 1) Read messages in from Kafka. 2) Window the messages into minute intervals specified
     * by the executor. 3) Write To GCS in user defined format.
     */
    PCollection<KV<String, String>> records =
        pipeline
            /*
             * Step #1: Read messages in from Kafka using {@link KafkaIO} and create a PCollection
             * of KV<String, String>.
             */
            .apply(
                "Read From Kafka",
                KafkaIO.<String, String>read()
                    .withConsumerConfigUpdates(
                        ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
                    .withBootstrapServers(options.getBootstrapServers())
                    .withTopics(topicsList)
                    .withKeyDeserializerAndCoder(
                        StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
                    .withValueDeserializerAndCoder(
                        StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
                    .withoutMetadata())
            /* Step #2: Window the messages into minute intervals specified by the executor. */
            .apply(
                "Creating " + options.getWindowDuration() + " Window",
                Window.into(
                    FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))));

    /* Step #3: Write To GCS in user defined format using the {@link FileFormatFactory}. */
    records.apply("Write To GCS", FileFormatFactory.newBuilder().setOptions(options).build());

    return pipeline.run();
  }
}
