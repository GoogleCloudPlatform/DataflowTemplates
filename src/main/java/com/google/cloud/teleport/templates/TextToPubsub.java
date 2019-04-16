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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * <p>
 * The {@code TextToPubsub} pipeline publishes records to Cloud Pub/Sub from a
 * set of files. By default, the pipeline reads each file row-by-row and
 * publishes each record as a string message. Using the option
 * {@code --format=json}, the pipeline reads each file row-by-row, interprets
 * each row as newline-delimited JSON, and publishes each record. The JSON
 * string contains a {@code data} attribute (a string that is some set of
 * Base64 encoded bytes) and an optional {@code attributes} object. The
 * {@code attributes} object contains a set of arbitrary attribute-string
 * pairs.
 * </p>
 *
 * <p>
 * Example String Usage:
 * </p>
 *
 * <pre>
 * {@code mvn compile exec:java \
    -Dexec.mainClass=com.google.cloud.teleport.templates.TextToPubsub \
    -Dexec.args=" \
    --project=${PROJECT_ID} \
    --stagingLocation=gs://${BUCKET}/TextToPubsub/staging \
    --tempLocation=gs://${BUCKET}/TextToPubsub/temp \
    --runner=DataflowRunner \
    --inputFilePattern=gs://${BUCKET}/TextToPubsub/input/demo_file.csv \
    --outputTopic=projects/${PROJECT_ID}/topics/TextToPubsub"
 * }
 * </pre>
 *
 * <p>
 * Pull messages:
 * </p>
 *
 * <pre>
 * gcloud --project ${PROJECT_ID} --format json pubsub subscriptions pull ${SUBSCRIPTION} --auto-ack
 * </pre>
 *
 * <p>
 * Example JSON Usage:
 * </p>
 *
 * <p>
 * Encode data:
 * </p>
 *
 * <pre>
 * $ echo -n 'Hello, world.' | base64
 * SGVsbG8sIHdvcmxkLg==
 *
 * $ echo -n 'Goodbye, world.' | base64
 * R29vZGJ5ZSwgd29ybGQu
 * </pre>
 *
 * <p>
 * gs://${BUCKET}/TextToPubsub/input/demo_file.json:
 * </p>
 *
 * <pre>
 * {"attributes":{"foo":"bar"},"data":"SGVsbG8sIHdvcmxkLg"}
 * {"attributes":{"foo":"baz"},"data":"R29vZGJ5ZSwgd29ybGQu"}
 * </pre>
 *
 * <pre>
 * {@code mvn compile exec:java \
    -Dexec.mainClass=com.google.cloud.teleport.templates.TextToPubsub \
    -Dexec.args=" \
    --project=${PROJECT_ID} \
    --stagingLocation=gs://${BUCKET}/TextToPubsub/staging \
    --tempLocation=gs://${BUCKET}/TextToPubsub/temp \
    --runner=DataflowRunner \
    --inputFilePattern=gs://${BUCKET}/TextToPubsub/input/demo_file.json \
    --format=json \
    --outputTopic=projects/${PROJECT_ID}/topics/TextToPubsub"
 * }
 * </pre>
 *
 * <p>
 * Pull messages:
 * </p>
 *
 * <pre>
 * gcloud --project ${PROJECT_ID} --format json pubsub subscriptions pull TextToPubsub-pull --auto-ack
 * gcloud --project ${PROJECT_ID} --format json pubsub subscriptions pull TextToPubsub-pull --auto-ack
 * </pre>
 */
public class TextToPubsub {


  /**
   * The custom options supported by the pipeline. Inherits
   * standard configuration options.
   */
  public interface Options extends PipelineOptions {
    @Description("The file pattern to read records from (e.g. gs://bucket/file-*.csv)")
    @Required
    ValueProvider<String> getInputFilePattern();
    void setInputFilePattern(ValueProvider<String> value);

    @Description("The name of the topic which data should be published to. "
        + "The name should be in the format of projects/<project-id>/topics/<topic-name>.")
    @Required
    ValueProvider<String> getOutputTopic();
    void setOutputTopic(ValueProvider<String> value);

    @Description("Input file format (e.g., string, json).")
    @Default.String("string")
    String getFormat();
    void setFormat(String value);
  }

  /**
   * Main entry-point for the pipeline. Reads in the
   * command-line arguments, parses them, and executes
   * the pipeline.
   *
   * @param args  Arguments passed in from the command-line.
   */
  public static void main(String[] args) {

    // Parse the user options passed from the command-line
    Options options = PipelineOptionsFactory
        .fromArgs(args)
        .withValidation()
        .as(Options.class);

    run(options);
  }

  /**
   * Executes the pipeline with the provided execution
   * parameters.
   *
   * @param options The execution parameters.
   */
  public static PipelineResult run(Options options) {
    // Create the pipeline.
    Pipeline pipeline = Pipeline.create(options);

    String format = options.getFormat();

    switch (format) {
      case "string":
        pipeline
            .apply("Read Text Data", TextIO.read().from(options.getInputFilePattern()))
            .apply("Write to PubSub", PubsubIO.writeStrings().to(options.getOutputTopic()))
        ;

        break;
      case "json":
        pipeline
            .apply("Read Text Data", TextIO.read().from(options.getInputFilePattern()))
            .apply("Deserialize JSON to PubsubMessage", ParDo.of(new JsonToPubsubMessage()))
            .apply("Write to Pubsub", PubsubIO.writeMessages().to(options.getOutputTopic()))
        ;

        break;
    }

    return pipeline.run();
  }

  static class JsonToPubsubMessage extends DoFn<String, PubsubMessage> {

    private static final Gson GSON =
        new GsonBuilder()
            .disableHtmlEscaping()
            .create()
        ;

    @ProcessElement
    public void processElement(@Element String string, OutputReceiver<PubsubMessage> out) {

      com.google.api.services.pubsub.model.PubsubMessage modelPubsubMessage =
          GSON.fromJson(
              string,
              com.google.api.services.pubsub.model.PubsubMessage.class
          );

      PubsubMessage pubsubMessage =
          new PubsubMessage(
              modelPubsubMessage.decodeData(),
              modelPubsubMessage.getAttributes()
          );

      out.output(pubsubMessage);
    }
  }
}
