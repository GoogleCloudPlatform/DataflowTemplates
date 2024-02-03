/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.templates.BulkDecompressor.Options;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
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
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Bulk_Decompress_GCS_Files.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Bulk_Decompress_GCS_Files",
    category = TemplateCategory.UTILITIES,
    displayName = "Bulk Decompress Files on Cloud Storage",
    description = {
      "The Bulk Decompress Cloud Storage Files template is a batch pipeline that decompresses files on Cloud Storage to a specified location. "
          + "This functionality is useful when you want to use compressed data to minimize network bandwidth costs during a migration, but would like to maximize analytical processing speed by operating on uncompressed data after migration. "
          + "The pipeline automatically handles multiple compression modes during a single run and determines the decompression mode to use based on the file extension (.bzip2, .deflate, .gz, .zip).",
      "Note: The Bulk Decompress Cloud Storage Files template is intended for single compressed files and not compressed folders."
    },
    optionsClass = Options.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/bulk-decompress-cloud-storage",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The files to decompress must be in one of the following formats: Bzip2, Deflate, and Gzip.",
      "The output directory must exist prior to running the pipeline."
    })
public class BulkDecompressor {

  /** The logger to output status messages to. */
  private static final Logger LOG = LoggerFactory.getLogger(BulkDecompressor.class);

  /**
   * A list of the {@link Compression} values excluding {@link Compression#AUTO} and {@link
   * Compression#UNCOMPRESSED}.
   */
  @VisibleForTesting
  static final Set<Compression> SUPPORTED_COMPRESSIONS =
      Stream.of(Compression.values())
          .filter(value -> value != Compression.AUTO && value != Compression.UNCOMPRESSED)
          .collect(Collectors.toSet());

  /** The error msg given when the pipeline matches a file but cannot determine the compression. */
  @VisibleForTesting
  static final String UNCOMPRESSED_ERROR_MSG =
      "Skipping file %s because it did not match any compression mode (%s)";

  @VisibleForTesting
  static final String MALFORMED_ERROR_MSG =
      "The file resource %s is malformed or not in %s compressed format.";

  /** The tag used to identify the main output of the {@link Decompress} DoFn. */
  @VisibleForTesting
  static final TupleTag<String> DECOMPRESS_MAIN_OUT_TAG = new TupleTag<String>() {};

  /** The tag used to identify the dead-letter sideOutput of the {@link Decompress} DoFn. */
  @VisibleForTesting
  static final TupleTag<KV<String, String>> DEADLETTER_TAG = new TupleTag<KV<String, String>>() {};

  /**
   * The {@link Options} class provides the custom execution options passed by the executor at the
   * command-line.
   */
  public interface Options extends PipelineOptions {
    @TemplateParameter.GcsReadFile(
        order = 1,
        description = "Input Cloud Storage File(s)",
        helpText = "The Cloud Storage location of the files you'd like to process.",
        example = "gs://your-bucket/your-files/*.gz")
    @Required
    ValueProvider<String> getInputFilePattern();

    void setInputFilePattern(ValueProvider<String> value);

    @TemplateParameter.GcsWriteFolder(
        order = 2,
        description = "Output file directory in Cloud Storage",
        helpText =
            "The path and filename prefix for writing output files. Must end with a slash. DateTime formatting is used to parse directory path for date & time formatters.",
        example = "gs://your-bucket/decompressed/")
    @Required
    ValueProvider<String> getOutputDirectory();

    void setOutputDirectory(ValueProvider<String> value);

    @TemplateParameter.GcsWriteFile(
        order = 3,
        description = "The output file for failures during the decompression process",
        helpText =
            "The output file to write failures to during the decompression process. If there are no failures, the file will still be created but will be empty. The contents will be one line for each file which failed decompression in CSV format (Filename, Error). Note that this parameter will allow the pipeline to continue processing in the event of a failure.",
        example = "gs://your-bucket/decompressed/failed.csv")
    @Required
    ValueProvider<String> getOutputFailureFile();

    void setOutputFailureFile(ValueProvider<String> value);
  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * BulkDecompressor#run(Options)} method to start the pipeline and invoke {@code
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
     *   1) Find all files matching the input pattern
     *   2) Decompress the files found and output them to the output directory
     *   3) Write any errors to the failure output file
     */

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // Run the pipeline over the work items.
    PCollectionTuple decompressOut =
        pipeline
            .apply("MatchFile(s)", FileIO.match().filepattern(options.getInputFilePattern()))
            .apply(
                "DecompressFile(s)",
                ParDo.of(new Decompress(options.getOutputDirectory()))
                    .withOutputTags(DECOMPRESS_MAIN_OUT_TAG, TupleTagList.of(DEADLETTER_TAG)));

    decompressOut
        .get(DEADLETTER_TAG)
        .apply(
            "FormatErrors",
            MapElements.into(TypeDescriptors.strings())
                .via(
                    kv -> {
                      StringWriter stringWriter = new StringWriter();
                      try {
                        CSVPrinter printer =
                            new CSVPrinter(
                                stringWriter,
                                CSVFormat.DEFAULT
                                    .withEscape('\\')
                                    .withQuoteMode(QuoteMode.NONE)
                                    .withRecordSeparator('\n'));
                        printer.printRecord(kv.getKey(), kv.getValue());
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }

                      return stringWriter.toString();
                    }))

        // We don't expect error files to be large so we'll create a single
        // file for ease of reprocessing by processes outside of Dataflow.
        .apply(
            "WriteErrorFile",
            TextIO.write()
                .to(options.getOutputFailureFile())
                .withHeader("Filename,Error")
                .withoutSharding());

    return pipeline.run();
  }

  /**
   * Performs the decompression of an object on Google Cloud Storage and uploads the decompressed
   * object back to a specified destination location.
   */
  @SuppressWarnings("serial")
  public static class Decompress extends DoFn<MatchResult.Metadata, String> {

    private final ValueProvider<String> destinationLocation;

    Decompress(ValueProvider<String> destinationLocation) {
      this.destinationLocation = destinationLocation;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      ResourceId inputFile = context.element().resourceId();

      // Output a record to the failure file if the file doesn't match a known compression.
      if (!Compression.AUTO.isCompressed(inputFile.toString())) {
        String errorMsg =
            String.format(UNCOMPRESSED_ERROR_MSG, inputFile.toString(), SUPPORTED_COMPRESSIONS);

        context.output(DEADLETTER_TAG, KV.of(inputFile.toString(), errorMsg));
      } else {
        try {
          ResourceId outputFile = decompress(inputFile);
          context.output(outputFile.toString());
        } catch (IOException e) {
          LOG.error(e.getMessage());
          context.output(DEADLETTER_TAG, KV.of(inputFile.toString(), e.getMessage()));
        }
      }
    }

    /**
     * Decompresses the inputFile using the specified compression and outputs to the main output of
     * the {@link Decompress} doFn. Files output to the destination will be first written as temp
     * files with a "temp-" prefix within the output directory. If a file fails decompression, the
     * filename and the associated error will be output to the dead-letter.
     *
     * @param inputFile The inputFile to decompress.
     * @return A {@link ResourceId} which points to the resulting file from the decompression.
     */
    private ResourceId decompress(ResourceId inputFile) throws IOException {
      // Remove the compressed extension from the file. Example: demo.txt.gz -> demo.txt
      String outputFilename = Files.getNameWithoutExtension(inputFile.toString());

      // Resolve the necessary resources to perform the transfer.
      ResourceId outputDir = FileSystems.matchNewResource(destinationLocation.get(), true);
      ResourceId outputFile =
          outputDir.resolve(outputFilename, StandardResolveOptions.RESOLVE_FILE);
      ResourceId tempFile =
          outputDir.resolve(
              Files.getFileExtension(inputFile.toString()) + "-temp-" + outputFilename,
              StandardResolveOptions.RESOLVE_FILE);

      // Resolve the compression
      Compression compression = Compression.detect(inputFile.toString());

      // Perform the copy of the decompressed channel into the destination.
      try (ReadableByteChannel readerChannel =
          compression.readDecompressed(FileSystems.open(inputFile))) {
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
        throw new IOException(sanitizeDecompressionErrorMsg(msg, inputFile, compression));
      }

      return outputFile;
    }

    /**
     * The error messages coming from the compression library are not consistent across compression
     * modes. Here we'll attempt to unify the messages to inform the user more clearly when we've
     * encountered a file which is not compressed or malformed. Note that GZIP and ZIP compression
     * modes will not throw an exception when a decompression is attempted on a file which is not
     * compressed.
     *
     * @param errorMsg The error message thrown during decompression.
     * @param inputFile The input file which failed decompression.
     * @param compression The compression mode used during decompression.
     * @return The sanitized error message. If the error was not from a malformed file, the same
     *     error message passed will be returned (if not null) or an empty string will be returned
     *     (if null).
     */
    private String sanitizeDecompressionErrorMsg(
        @Nullable String errorMsg, ResourceId inputFile, Compression compression) {
      if (errorMsg != null
          && (errorMsg.contains("not in the BZip2 format")
              || errorMsg.contains("incorrect header check"))) {
        errorMsg = String.format(MALFORMED_ERROR_MSG, inputFile.toString(), compression);
      }

      return errorMsg == null ? "" : errorMsg;
    }
  }
}
