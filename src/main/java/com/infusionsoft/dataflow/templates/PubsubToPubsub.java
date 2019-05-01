package com.infusionsoft.dataflow.templates;

import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.TransformTextViaJavascript;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadOptions;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubWriteOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;

/** An template that transforms messages from one Pubsub subscription and publishes to another Pubsub topic. */
public class PubsubToPubsub {

  /**
   * Options supported by {@link PubsubToPubsub}.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, StreamingOptions,
      PubsubReadOptions, JavascriptTextTransformerOptions, PubsubWriteOptions {}

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    // Parse the user options passed from the command-line
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

    pipeline
        .apply("Read Events", PubsubIO.readStrings()
            .fromTopic(options.getPubsubReadTopic()))
        .apply("Transform Events", TransformTextViaJavascript.newBuilder()
            .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
            .setFunctionName(options.getJavascriptTextTransformFunctionName())
            .build())
        .apply("Write Events", PubsubIO.writeStrings()
            .to(options.getPubsubWriteTopic()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}