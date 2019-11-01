/*
 * Copyright (C) 2018 Google Inc.
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

package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.kafka.connector.KafkaIO;
import com.google.cloud.teleport.util.DurationUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * This pipeline ingests incoming data from a Kafka topic and
 * outputs the raw data into windowed files at GCS text
 *
 * <p> Example Usage:
 *
 * <pre>
 *
 * # Set the pipeline vars
 * PIPELINE_NAME=KafkaToText
 * PROJECT_ID=PROJECT ID HERE
 * INPUT_TOPIC=INPUT TOPIC HERE
 * BUCKET_NAME=BUCKET NAME HERE
 * PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/kafka-to-text
 * outputFilenamePrefix
 * OUTPUT_FILENAME_PREFIX=gs://${BUCKET_NAME}/dataflow/pipelines/kafka-to-text/file
 * OUTPUT_FILENAME_SUFFIX=.json
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.${PIPELINE_NAME} \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --runner=${RUNNER} \
 * --windowDuration=10s \
 * --numShards=1 \
 * --inputTopic=${INPUT_TOPIC} \
 * --outputFilenamePrefix=${OUTPUT_FILENAME_PREFIX} \
 * --outputFilenameSuffix=${OUTPUT_FILENAME_SUFFIX}"
 * </pre>
 * </p>
 */
public class KafkaToText {

    /**
     * Options supported by the pipeline.
     *
     * <p>Inherits standard configuration options.</p>
     */
    public interface Options extends PipelineOptions, StreamingOptions {

        @Description("Kafka Bootstrap Servers")
        ValueProvider<String> getBootstrapServers();

        void setBootstrapServers(ValueProvider<String> value);

        @Description("Kafka topic to read the input from")
        ValueProvider<String> getInputTopic();

        void setInputTopic(ValueProvider<String> value);

        @Description("The maximum number of output shards produced when writing.")
        @Default.Integer(1)
        Integer getNumShards();

        void setNumShards(Integer value);

        @Description("The window duration in which data will be written. Defaults to 5m. "
                + "Allowed formats are: "
                + "Ns (for seconds, example: 5s), "
                + "Nm (for minutes, example: 12m), "
                + "Nh (for hours, example: 2h).")
        @Default.String("10s")
        String getWindowDuration();

        void setWindowDuration(String value);

        @Description("Output filename prefix")
        String getOutputFilenamePrefix();

        void setOutputFilenamePrefix(String value);

        @Description("Output filename suffix")
        String getOutputFilenameSuffix();

        void setOutputFilenameSuffix(String value);
    }

    /**
     * Main entry point for executing the pipeline.
     *
     * @param args The command-line arguments to the pipeline.
     */
    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        options.setStreaming(true);

        run(options);
    }

    /**
     * Runs the pipeline with the supplied options.
     *
     * @param options The execution parameters to the pipeline.
     * @return The result of the pipeline execution.
     */
    public static PipelineResult run(Options options) {
        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        /*
         * Steps:
         *   1) Read string messages from Kafka
         *   2) Window the messages into minute intervals specified by the executor.
         *   3) Output the windowed files to GCS
         */
        pipeline
                // Step 1. Read string messages from Kafka
                .apply(
                        "ReadFromKafka",
                        KafkaIO.<String, String>read()
                                .withBootstrapServers(options.getBootstrapServers())
                                .withTopic(options.getInputTopic())
                                .withKeyDeserializer(StringDeserializer.class)
                                .withValueDeserializer(StringDeserializer.class)
                                .withNumSplits(1)
                                .withoutMetadata())

                // Step 2. Window the messages into minute intervals specified by the executor.
                .apply(
                        "ApplyWindow",
                        Values.create())
                .apply(Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))

                // Step 3. Output the windowed files to GCS
                .apply(
                        "Write File(s)",
                        TextIO.write()
                        .withWindowedWrites()
                        .withNumShards(options.getNumShards())
                        .to(options.getOutputFilenamePrefix())
                        .withSuffix(options.getOutputFilenameSuffix()));

        // Execute the pipeline and return the result.
        return pipeline.run();
    }
}