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

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link BulkCompressor} is a batch pipeline that compresses files on matched by an input file
 * pattern and outputs them to a specified file location. This pipeline can be useful when you need
 * to compress large batches of files as part of a perodic archival process. The supported
 * compression modes are: <code>BZIP2</code>, <code>DEFLATE</code>, <code>GZIP</code>, <code>ZIP
 * </code>. Files output to the destination location will follow a naming schema of original
 * filename appended with the compression mode extension. The extensions appended will be one of:
 * <code>.bzip2</code>, <code>.deflate</code>, <code>.gz</code>, <code>.zip</code> as determined by
 * the compression type.
 *
 * <p>Any errors which occur during the compression process will be output to the failure file in
 * CSV format of filename, error message. If no failures occur during execution, the error file will
 * still be created but will contain no error records.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The compression must be in one of the following formats: <code>BZIP2</code>, <code>DEFLATE
 *       </code>, <code>GZIP</code>, <code>ZIP</code>.
 *   <li>The output directory must exist prior to pipeline execution.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT ID HERE
 * PIPELINE_FOLDER=gs://${PROJECT_ID}/dataflow/pipelines/bulk-compressor
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.BulkCompressor \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --templateLocation=${PIPELINE_FOLDER}/template \
 * --runner=${RUNNER}"
 *
 * # Execute the template
 * JOB_NAME=bulk-compressor-$USER-`date +"%Y%m%d-%H%M%S%z"`
 *
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template \
 * --zone=us-east1-d \
 * --parameters \
 * "inputFilePattern=${PIPELINE_FOLDER}/test/uncompressed/*,\
 * outputDirectory=${PIPELINE_FOLDER}/test/compressed,\
 * outputFailureFile=${PIPELINE_FOLDER}/test/failure/failed-${JOB_NAME}.csv,\
 * compression=GZIP"
 * </pre>
 */
public class BulkCompressor {

  /** The logger to output status messages to. */
  private static final Logger LOG = LoggerFactory.getLogger(BulkCompressor.class);

  /** The tag used to identify the main output of the {@link Compressor}. */
  private static final TupleTag<String> COMPRESOR_MAIN_OUT = new TupleTag<String>() {};

  /** The tag used to identify the dead-letter output of the {@link Compressor}. */
  private static final TupleTag<KV<String, String>> DEADLETTER_TAG =
      new TupleTag<KV<String, String>>() {};

  /**
   * The {@link Options} class provides the custom execution options passed by the executor at the
   * command-line.
   */
  public interface Options extends PipelineOptions {
    @Description("The input file pattern to read from (e.g. gs://bucket-name/uncompressed/*.gz)")
    @Required
    ValueProvider<String> getInputFilePattern();

    void setInputFilePattern(ValueProvider<String> value);

    @Description("The output location to write to (e.g. gs://bucket-name/compressed)")
    @Required
    ValueProvider<String> getOutputDirectory();

    void setOutputDirectory(ValueProvider<String> value);

    @Description(
        "The output file to write failures during the compression process "
            + "(e.g. gs://bucket-name/compressed/failed.txt). The contents will be one line for "
            + "each file which failed compression. Note that this parameter will "
            + "allow the pipeline to continue processing in the event of a failure.")
    @Required
    ValueProvider<String> getOutputFailureFile();

    void setOutputFailureFile(ValueProvider<String> value);

    @Description("The compression algorithm to use on the matched files.")
    @Required
    ValueProvider<Compression> getCompression();

    void setCompression(ValueProvider<Compression> value);
  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * BulkCompressor#run(Options)} method to start the pipeline and invoke {@code
   * result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options. This method does not wait until the
   * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
   * object to block until the pipeline is finished running if blocking programmatic execution is
   * required.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(Options options) {

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    /*
     * Steps:
     *   1) Find all files matching the input pattern
     *   2) Compress the files found and output them to the output directory
     *   3) Write any errors to the failure output file
     */
    PCollectionTuple compressOut =
        pipeline
            .apply("Match File(s)", FileIO.match().filepattern(options.getInputFilePattern()))
            .apply(
                "Compress File(s)",
                ParDo.of(new Compressor(options.getOutputDirectory(), options.getCompression()))
                    .withOutputTags(COMPRESOR_MAIN_OUT, TupleTagList.of(DEADLETTER_TAG)));

    compressOut
        .get(DEADLETTER_TAG)
        .apply(
            "Format Errors",
            MapElements.into(TypeDescriptors.strings())
                .via(kv -> String.format("%s,%s", kv.getKey(), kv.getValue())))
        .apply(
            "Write Error File",
            TextIO.write()
                .to(options.getOutputFailureFile())
                .withHeader("Filename,Error")
                .withoutSharding());

    return pipeline.run();
  }

  /**
   * The {@link Compressor} accepts {@link MatchResult.Metadata} from the FileSystems API and
   * compresses each file to an output location. Any compression failures which occur during
   * execution will be output to a separate output for further processing.
   */
  @SuppressWarnings("serial")
  public static class Compressor extends DoFn<MatchResult.Metadata, String> {

    private final ValueProvider<String> destinationLocation;
    private final ValueProvider<Compression> compressionValue;

    Compressor(ValueProvider<String> destinationLocation, ValueProvider<Compression> compression) {
      this.destinationLocation = destinationLocation;
      this.compressionValue = compression;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      ResourceId inputFile = context.element().resourceId();
      Compression compression = compressionValue.get();

      // Add the compression extension to the output filename. Example: demo.txt -> demo.txt.gz
      String outputFilename = inputFile.getFilename() + compression.getSuggestedSuffix();

      // Resolve the necessary resources to perform the transfer
      ResourceId outputDir = FileSystems.matchNewResource(destinationLocation.get(), true);
      ResourceId outputFile =
          outputDir.resolve(outputFilename, StandardResolveOptions.RESOLVE_FILE);
      ResourceId tempFile =
          outputDir.resolve("temp-" + outputFilename, StandardResolveOptions.RESOLVE_FILE);

      // Perform the copy of the compressed channel to the destination.
      try (ReadableByteChannel readerChannel = FileSystems.open(inputFile)) {
        try (WritableByteChannel writerChannel =
            compression.writeCompressed(FileSystems.create(tempFile, MimeTypes.BINARY))) {

          // Execute the copy to the temporary file
          ByteStreams.copy(readerChannel, writerChannel);
        }

        // Rename the temporary file to the output file
        FileSystems.rename(ImmutableList.of(tempFile), ImmutableList.of(outputFile));

        // Output the path to the uncompressed file
        context.output(outputFile.toString());
      } catch (IOException e) {
        LOG.error("Error occurred during compression of {}", inputFile.toString(), e);
        context.output(DEADLETTER_TAG, KV.of(inputFile.toString(), e.getMessage()));
      }
    }
  }
}
