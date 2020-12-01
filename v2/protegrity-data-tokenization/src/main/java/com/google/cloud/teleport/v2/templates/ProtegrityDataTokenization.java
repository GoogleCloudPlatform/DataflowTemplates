package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.options.ProtegrityDataTokenizationOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link ProtegrityDataTokenization} pipeline.
 */
public class ProtegrityDataTokenization {

    /** Logger for class. */
    private static final Logger LOG = LoggerFactory.getLogger(ProtegrityDataTokenization.class);

    /**
     * Main entry point for pipeline execution.
     *
     * @param args Command line arguments to the pipeline.
     */
    public static void main(String[] args) {
        ProtegrityDataTokenizationOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(ProtegrityDataTokenizationOptions.class);

        run(options);
    }

    /**
     * Runs the pipeline to completion with the specified options.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    public static PipelineResult run(ProtegrityDataTokenizationOptions options) {

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("readTextFromGCSFiles", TextIO.read().from(options.getInputGcsFilePattern()));

        return pipeline.run();
    }
}
