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

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.kafka.options.KafkaReadOptions;
import com.google.cloud.teleport.v2.kafka.options.SchemaRegistryOptions;
import com.google.cloud.teleport.v2.kafka.transforms.KafkaTransform;
import com.google.cloud.teleport.v2.kafka.utils.KafkaConfig;
import com.google.cloud.teleport.v2.kafka.utils.KafkaTopicUtils;
import com.google.cloud.teleport.v2.transforms.WriteTransform;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

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
    requirements = {"The output Google Cloud Storage directory must exist."})
public class KafkaToGcsFlex {
  public interface KafkaToGcsOptions
      extends PipelineOptions, DataflowPipelineOptions, KafkaReadOptions, SchemaRegistryOptions {

    // This is a duplicate option that already exist in KafkaReadOptions but keeping it here
    // so the KafkaTopic appears above the authentication enum on the Templates UI.
    @TemplateParameter.KafkaTopic(
        order = 1,
        name = "readBootstrapServerAndTopic",
        groupName = "Source",
        description = "Source Kafka Topic",
        helpText = "Kafka Topic to read the input from.")
    String getReadBootstrapServerAndTopic();

    void setReadBootstrapServerAndTopic(String value);

    @TemplateParameter.Duration(
        order = 20,
        optional = true,
        groupName = "Destination",
        description = "Window duration",
        helpText =
            "The window duration/size in which data will be written to Cloud Storage. Allowed formats are: Ns (for "
                + "seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h).",
        example = "5m")
    @Default.String("5m")
    String getWindowDuration();

    void setWindowDuration(String windowDuration);

    @TemplateParameter.GcsWriteFolder(
        order = 21,
        groupName = "Destination",
        description = "Output file directory in Cloud Storage",
        helpText = "The path and filename prefix for writing output files. Must end with a slash.",
        example = "gs://your-bucket/your-path/")
    String getOutputDirectory();

    void setOutputDirectory(String outputDirectory);

    @TemplateParameter.Text(
        order = 22,
        optional = true,
        groupName = "Destination",
        description = "Output filename prefix of the files to write",
        helpText = "The prefix to place on each windowed file.",
        example = "output-")
    @Default.String("output")
    String getOutputFilenamePrefix();

    void setOutputFilenamePrefix(String outputFilenamePrefix);

    @TemplateParameter.Integer(
        order = 23,
        optional = true,
        description = "Maximum output shards",
        groupName = "Destination",
        helpText =
            "The maximum number of output shards produced when writing. A higher number of "
                + "shards means higher throughput for writing to Cloud Storage, but potentially higher "
                + "data aggregation cost across shards when processing output Cloud Storage files. "
                + "Default value is decided by Dataflow.")
    @Default.Integer(0)
    Integer getNumShards();

    void setNumShards(Integer numShards);
  }

  public static PipelineResult run(KafkaToGcsOptions options) throws UnsupportedOperationException {

    // Create the Pipeline
    Pipeline pipeline = Pipeline.create(options);
    String bootstrapServes;
    List<String> topicsList;
    if (options.getReadBootstrapServerAndTopic() != null) {
      List<String> bootstrapServerAndTopicList =
          KafkaTopicUtils.getBootstrapServerAndTopic(
              options.getReadBootstrapServerAndTopic(), options.getProject());
      topicsList = List.of(bootstrapServerAndTopicList.get(1));
      bootstrapServes = bootstrapServerAndTopicList.get(0);
    } else {
      throw new IllegalArgumentException(
          "Please provide a valid bootstrap server which matches `[,:a-zA-Z0-9._-]+` and a topic which matches `[,a-zA-Z0-9._-]+`");
    }

    options.setStreaming(true);

    Map<String, Object> kafkaConfig = new HashMap<>(KafkaConfig.fromReadOptions(options));

    PCollection<KafkaRecord<byte[], byte[]>> kafkaRecord;
    // Step 1: Read from Kafka as bytes.
    KafkaIO.Read<byte[], byte[]> kafkaTransform =
        KafkaTransform.readBytesFromKafka(
            bootstrapServes, topicsList, kafkaConfig, options.getEnableCommitOffsets());
    kafkaRecord = pipeline.apply(kafkaTransform);
    kafkaRecord.apply(WriteTransform.newBuilder().setOptions(options).build());
    return pipeline.run();
  }

  public static void main(String[] args) {
    KafkaToGcsOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaToGcsOptions.class);

    run(options);
  }
}
