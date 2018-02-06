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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.CompressedSource.CompressionMode;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.compress.compressors.CompressorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline decompresses file(s) from Google Cloud Storage and re-uploads them to a destination
 * location.
 *
 * <p><b>Parameters</b>
 *
 * <p>The {@code --inputFilePattern} parameter specifies a file glob to process. Files found can be
 * expressed in the following formats:
 *
 * <pre>
 * --inputFilePattern=gs://bucket-name/compressed-dir/*
 * --inputFilePattern=gs://bucket-name/compressed-dir/demo*.gz
 * </pre>
 *
 * <p>The {@code --outputDirectory} parameter can be expressed in the following formats:
 *
 * <pre>
 * --outputDirectory=gs://bucket-name
 * --outputDirectory=gs://bucket-name/decompressed-dir
 * </pre>
 *
 * <p>The {@code --outputFailureFile} parameter indicates the file to write the names of the files
 * which failed decompression and their associated error messages. This file can then be used for
 * subsequent processing by another process outside of Dataflow (e.g. send an email with the
 * failures, etc.). If there are no failures, the file will still be created but will be empty. The
 * failure file structure contains both the file that caused the error and the error message in CSV
 * format. The file will contain one header row and two columns (Filename, Error). The filename
 * output to the failureFile will be the full path of the file for ease of debugging.
 *
 * <pre>
 * --outputFailureFile=gs://bucket-name/decompressed-dir/failed.csv
 * </pre>
 *
 * <p>Example Output File:
 *
 * <pre>
 * Filename,Error
 * gs://docs-demo/compressedFile.gz, File is malformed or not compressed in BZIP2 format.
 * </pre>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * mvn compile exec:java \
 * -Dexec.mainClass=org.apache.beam.examples.templates.Decompressor \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
 * --tempLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
 * --runner=DataflowRunner \
 * --inputFilePattern=gs://${PROJECT_ID}/compressed-dir/*.gz \
 * --outputDirectory=gs://${PROJECT_ID}/decompressed-dir \
 * --outputFailureFile=gs://${PROJECT_ID}/decompressed-dir/failed.csv"
 * </pre>
 */
public class Decompressor {

  /** The logger to output status messages to. */
  private static final Logger LOG = LoggerFactory.getLogger(Decompressor.class);

  /** The tag used to identify the main output of the decompress {@link ParDo}. */
  private static final TupleTag<KV<ResourceId, CompressionMode>> FILE_LISTER_MAIN_OUT_TAG =
      new TupleTag<KV<ResourceId, CompressionMode>>() {};

  /** The tag used to identify the main output of the decompress {@link ParDo}. */
  private static final TupleTag<Void> DECOMPRESS_MAIN_OUT_TAG = new TupleTag<Void>() {};

  /** The tag used to identify the dead-letter sideOutput. */
  @VisibleForTesting
  static final TupleTag<KV<String, String>> DEADLETTER_TAG =
      new TupleTag<KV<String, String>>() {};

  /**
   * The {@link Options} class provides the custom execution options passed by the executor at the
   * command-line.
   */
  public interface Options extends PipelineOptions {
    @Description("The input file pattern to read from (e.g. gs://bucket-name/compressed/*.gz)")
    @Required
    ValueProvider<String> getInputFilePattern();

    void setInputFilePattern(ValueProvider<String> value);

    @Description("The output location to write to (e.g. gs://bucket-name/decompressed)")
    @Required
    ValueProvider<String> getOutputDirectory();

    void setOutputDirectory(ValueProvider<String> value);

    @Description(
        "The output file to write failures during the decompression process "
            + "(e.g. gs://bucket-name/decompressed/failed.txt). The contents will be one line for "
            + "each file which failed decompression. Note that this parameter will "
            + "allow the pipeline to continue processing in the event of a failure.")
    @Required
    ValueProvider<String> getOutputFailureFile();

    void setOutputFailureFile(ValueProvider<String> value);
  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * Decompressor#run(Options)} method to start the pipeline and invoke {@code
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

    /*
     * Steps:
     *  1) Create a work item (file pattern) to decompress
     *  2) List the files which need to be processed
     *  3) Shuffle the work so we spread evenly across the worker pool
     *  4) Decompress the files
     */

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // Run the pipeline over the work items.
    PCollectionTuple listerOut =
        pipeline
            .apply("Create Work Item", Create.of(options.getInputFilePattern()))
            .apply(
                "List Files",
                ParDo.of(new FileLister())
                    .withOutputTags(FILE_LISTER_MAIN_OUT_TAG, TupleTagList.of(DEADLETTER_TAG)));

    PCollectionTuple decompressOut =
        listerOut
            .get(FILE_LISTER_MAIN_OUT_TAG)

            // Re-shuffle the work items to prevent fusion and distribute
            // evenly across the worker machines.
            .apply("Shuffle File(s)", Shuffle.of())
            .apply(
                "Decompress File(s)",
                ParDo.of(new Decompress(options.getOutputDirectory()))
                    .withOutputTags(DECOMPRESS_MAIN_OUT_TAG, TupleTagList.of(DEADLETTER_TAG)));

    PCollectionList.of(listerOut.get(DEADLETTER_TAG))
        .and(decompressOut.get(DEADLETTER_TAG))
        .apply("Collect Errors", Flatten.pCollections())
        .apply(
            "Format Errors",
            MapElements.into(TypeDescriptors.strings())
                .via(kv -> String.format("%s,%s", kv.getKey(), kv.getValue())))

        // We don't expect error files to be large so we'll create a single
        // file for ease of reprocessing by processes outside of Dataflow.
        .apply(
            "Write Error File",
            TextIO.write()
                .to(options.getOutputFailureFile())
                .withHeader("Filename,Error")
                .withoutSharding());

    return pipeline.run();
  }

  /**
   * The {@code FileLister} class takes in a source location and a compression type, lists the
   * file(s) at that location, and passes matches downstream for processing.
   *
   * <p>TODO: Refactor to use FileIO.match() and File.read() after Beam 2.2 is released.
   */
  @SuppressWarnings("serial")
  public static class FileLister
      extends DoFn<ValueProvider<String>, KV<ResourceId, CompressionMode>> {

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      String inputFilePattern = context.element().get();

      LOG.info("Finding all files matching: {} for decompression", inputFilePattern);

      // Retrieves all files matching the glob on the filesystem
      MatchResult result = FileSystems.match(inputFilePattern);

      // Fail the pipeline if the file pattern passed didn't match anything.
      checkArgument(
          result.status() == Status.OK && !result.metadata().isEmpty(),
          "Failed to match any files with the pattern " + inputFilePattern);

      /*
       * Send all the records downstream. We only expect to have one element in the
       * list since a single spec was matched on by the {@code FileSystems} class.
       * However, the one element should contain a list of the matched files in the
       * metadata.
       */
      result
          .metadata()
          .forEach(
              (Metadata metadata) -> {
                ResourceId resourceId = metadata.resourceId();
                String filename = resourceId.getFilename();

                // Find the compression mode which matches the file extension.
                Optional<CompressionMode> compressionMode =
                    Stream.of(CompressionMode.values())
                        .filter(mode -> mode.matches(filename))
                        .findFirst();

                if (compressionMode.isPresent()) {
                  context.output(KV.of(metadata.resourceId(), compressionMode.get()));
                } else {
                  String msg =
                      String.format(
                          "Skipping file %s because it did not match any compression mode (%s)",
                          filename, Arrays.toString(CompressionMode.values()));

                  LOG.warn(msg);
                  context.output(DEADLETTER_TAG, KV.of(resourceId.toString(), msg));
                }
              });
    }
  }

  /**
   * Performs the decompression of an object on Google Cloud Storage and uploads the decompressed
   * object back to a specified destination location.
   *
   * <p>TODO: Refactor to use FileIO.write() after Beam 2.2 is released.
   */
  @SuppressWarnings("serial")
  public static class Decompress extends DoFn<KV<ResourceId, CompressionMode>, Void> {

    private final ValueProvider<String> destinationLocation;

    Decompress(ValueProvider<String> destinationLocation) {
      this.destinationLocation = destinationLocation;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException, CompressorException {
      ResourceId inputFile = context.element().getKey();
      CompressionMode mode = context.element().getValue();

      // Remove the compressed extension from the file. Example: demo.txt.gz -> demo.txt
      String outputFilename = Files.getNameWithoutExtension(inputFile.getFilename());

      // Resolve the necessary resources to perform the transfer.
      ResourceId outputDir = FileSystems.matchNewResource(destinationLocation.get(), true);
      ResourceId outputFile =
          outputDir.resolve(outputFilename, StandardResolveOptions.RESOLVE_FILE);
      ResourceId tempFile =
          outputDir.resolve("temp-" + outputFilename, StandardResolveOptions.RESOLVE_FILE);

      // Perform the copy of the decompressed channel into the destination.
      try (ReadableByteChannel readerChannel =
          mode.createDecompressingChannel(FileSystems.open(inputFile))) {
        try (WritableByteChannel writerChannel = FileSystems.create(tempFile, MimeTypes.TEXT)) {
          ByteStreams.copy(readerChannel, writerChannel);
        }

        // Rename the temp file to the output file.
        FileSystems.rename(
            ImmutableList.of(tempFile),
            ImmutableList.of(outputFile),
            MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);

      } catch (IOException e) {
        String msg = e.getMessage();
        LOG.error("Error occurred during decompression of {}", inputFile.toString(), e);

        /*
         * The error messages coming from the compression library are not consistent
         * across compression modes. Here we'll attempt to unify the messages to inform
         * the user more clearly when we've encountered a file which is not compressed or
         * malformed. Note that GZIP and ZIP compression modes will not throw an exception
         * when a decompression is attempt on a file which is not compressed.
         */
        if (msg != null
            && (msg.contains("not in the BZip2 format")
                || msg.contains("incorrect header check"))) {
          msg =
              String.format(
                  "The file resource %s is malformed or not in %s compressed format.",
                  inputFile.toString(), mode);
        }

        LOG.error(msg);
        context.output(DEADLETTER_TAG, KV.of(inputFile.toString(), msg));
      }
    }
  }

  /**
   * The {@link Shuffle} class takes in a KV pair of {@link ResourceId} and {@link CompressionMode},
   * creates a shuffle key, shuffles the elements, and returns the shuffled records without the
   * shuffle key. This transform helps distribute the work to be processed evenly over the worker
   * machines by breaking fusion.
   *
   * <p>TODO: Remove when Reshuffle.viaRandomKey() is available in Beam 2.2 or 2.3
   */
  @SuppressWarnings("deprecation") // Suppresses the warning for the Reshuffle.of() call.
  public static class Shuffle
      extends PTransform<
          PCollection<KV<ResourceId, CompressionMode>>,
          PCollection<KV<ResourceId, CompressionMode>>> {

    private Shuffle() {}

    public static Shuffle of() {
      return new Shuffle();
    }

    @Override
    public PCollection<KV<ResourceId, CompressionMode>> expand(
        PCollection<KV<ResourceId, CompressionMode>> input) {

      return input
          .apply("Create Shuffle Key", WithKeys.of(kv -> kv.getValue().toString()))
          .setCoder(
              KvCoder.of(
                  StringUtf8Coder.of(),
                  KvCoder.of(
                      SerializableCoder.of(ResourceId.class),
                      SerializableCoder.of(CompressionMode.class))))
          .apply("Shuffle File(s)", Reshuffle.of())
          .apply("Remove Shuffle Key", Values.create());
    }
  }
}
