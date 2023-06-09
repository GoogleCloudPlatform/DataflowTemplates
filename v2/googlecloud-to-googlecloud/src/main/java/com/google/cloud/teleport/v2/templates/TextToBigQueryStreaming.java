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
package com.google.cloud.teleport.v2.templates;

import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.BigQueryStorageApiStreamingOptions;
import com.google.cloud.teleport.v2.templates.TextToBigQueryStreaming.TextToBigQueryStreamingOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.v2.transforms.ErrorConverters.WriteStringMessageErrors;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import com.google.cloud.teleport.v2.utils.GCSUtils;
import com.google.cloud.teleport.v2.utils.ResourceUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Watch.Growth;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link TextToBigQueryStreaming} is a streaming version of {@link TextIOToBigQuery} pipeline
 * that reads text files, applies a JavaScript UDF and writes the output to BigQuery. The pipeline
 * continuously polls for new files, reads them row-by-row and processes each record into BigQuery.
 * The polling interval is set at 10 seconds.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-googlecloud/README_Stream_GCS_Text_to_BigQuery_Flex.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Stream_GCS_Text_to_BigQuery_Flex",
    category = TemplateCategory.STREAMING,
    displayName = "Cloud Storage Text to BigQuery (Stream)",
    description =
        "A streaming pipeline that can read text files stored in Cloud Storage, perform a transform"
            + " via a user defined JavaScript function, and stream the results into BigQuery. This"
            + " pipeline requires a JavaScript function and a JSON representation of the BigQuery"
            + " TableSchema.",
    optionsClass = TextToBigQueryStreamingOptions.class,
    flexContainerName = "text-to-bigquery-streaming",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/text-to-bigquery-stream",
    contactInformation = "https://cloud.google.com/support")
public class TextToBigQueryStreaming {

  private static final Logger LOG = LoggerFactory.getLogger(TextToBigQueryStreaming.class);

  /** The tag for the main output for the UDF. */
  private static final TupleTag<FailsafeElement<String, String>> UDF_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  /** The tag for the dead-letter output of the udf. */
  private static final TupleTag<FailsafeElement<String, String>> UDF_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  /** The tag for the main output of the json transformation. */
  private static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<TableRow>() {};

  /** The tag for the dead-letter output of the json to table row transform. */
  private static final TupleTag<FailsafeElement<String, String>> TRANSFORM_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  /** The default suffix for error tables if dead letter table is not specified. */
  private static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = "_error_records";

  /** Default interval for polling files in GCS. */
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(10);

  /** Coder for FailsafeElement. */
  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

  private static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link
   * TextToBigQueryStreaming#run(TextToBigQueryStreamingOptions)} method to start the pipeline and
   * invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    // Parse the user options passed from the command-line
    TextToBigQueryStreamingOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(TextToBigQueryStreamingOptions.class);
    BigQueryIOUtils.validateBQStorageApiOptionsStreaming(options);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(TextToBigQueryStreamingOptions options) {

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // Register the coder for pipeline
    FailsafeElementCoder<String, String> coder =
        FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    /*
     * Steps:
     *  1) Read from the text source continuously.
     *  2) Convert to FailsafeElement.
     *  3) Apply Javascript udf transformation.
     *    - Tag records that were successfully transformed and those
     *      that failed transformation.
     *  4) Convert records to TableRow.
     *    - Tag records that were successfully converted and those
     *      that failed conversion.
     *  5) Insert successfully converted records into BigQuery.
     *    - Errors encountered while streaming will be sent to deadletter table.
     *  6) Insert records that failed into deadletter table.
     */

    PCollectionTuple transformedOutput =
        pipeline

            // 1) Read from the text source continuously.
            .apply(
                "ReadFromSource",
                TextIO.read()
                    .from(options.getInputFilePattern())
                    .watchForNewFiles(DEFAULT_POLL_INTERVAL, Growth.never()))

            // 2) Convert to FailsafeElement.
            .apply(
                "ConvertToFailsafeElement",
                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                    .via(input -> FailsafeElement.of(input, input)))

            // 3) Apply Javascript udf transformation.
            .apply(
                "ApplyUDFTransformation",
                FailsafeJavascriptUdf.<String>newBuilder()
                    .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                    .setFunctionName(options.getJavascriptTextTransformFunctionName())
                    .setSuccessTag(UDF_OUT)
                    .setFailureTag(UDF_DEADLETTER_OUT)
                    .build());

    PCollectionTuple convertedTableRows =
        transformedOutput

            // 4) Convert records to TableRow.
            .get(UDF_OUT)
            .apply(
                "ConvertJSONToTableRow",
                FailsafeJsonToTableRow.<String>newBuilder()
                    .setSuccessTag(TRANSFORM_OUT)
                    .setFailureTag(TRANSFORM_DEADLETTER_OUT)
                    .build());

    WriteResult writeResult =
        convertedTableRows

            // 5) Insert successfully converted records into BigQuery.
            .get(TRANSFORM_OUT)
            .apply(
                "InsertIntoBigQuery",
                BigQueryIO.writeTableRows()
                    .withJsonSchema(GCSUtils.getGcsFileAsString(options.getJSONPath()))
                    .to(options.getOutputTable())
                    .withExtendedErrorInfo()
                    .withoutValidation()
                    .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                    .withCustomGcsTempLocation(
                        StaticValueProvider.of(options.getBigQueryLoadingTemporaryDirectory())));

    // Elements that failed inserts into BigQuery are extracted and converted to FailsafeElement
    PCollection<FailsafeElement<String, String>> failedInserts =
        BigQueryIOUtils.writeResultToBigQueryInsertErrors(writeResult, options)
            .apply(
                "WrapInsertionErrors",
                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                    .via(TextToBigQueryStreaming::wrapBigQueryInsertError));

    // 6) Insert records that failed transformation or conversion into deadletter table
    PCollectionList.of(
            ImmutableList.of(
                transformedOutput.get(UDF_DEADLETTER_OUT),
                convertedTableRows.get(TRANSFORM_DEADLETTER_OUT),
                failedInserts))
        .apply("Flatten", Flatten.pCollections())
        .apply(
            "WriteFailedRecords",
            WriteStringMessageErrors.newBuilder()
                .setErrorRecordsTable(
                    StringUtils.isNotEmpty(options.getOutputDeadletterTable())
                        ? options.getOutputDeadletterTable()
                        : options.getOutputTable() + DEFAULT_DEADLETTER_TABLE_SUFFIX)
                .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson())
                .build());

    return pipeline.run();
  }

  /**
   * Method to wrap a {@link BigQueryInsertError} into a {@link FailsafeElement}.
   *
   * @param insertError BigQueryInsert error.
   * @return FailsafeElement object.
   * @throws IOException
   */
  static FailsafeElement<String, String> wrapBigQueryInsertError(BigQueryInsertError insertError) {

    FailsafeElement<String, String> failsafeElement;
    try {

      String rowPayload = JSON_FACTORY.toString(insertError.getRow());
      String errorMessage = JSON_FACTORY.toString(insertError.getError());

      failsafeElement = FailsafeElement.of(rowPayload, rowPayload);
      failsafeElement.setErrorMessage(errorMessage);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return failsafeElement;
  }

  /**
   * The {@link TextToBigQueryStreamingOptions} class provides the custom execution options passed
   * by the executor at the command-line.
   */
  public interface TextToBigQueryStreamingOptions
      extends TextIOToBigQuery.Options, BigQueryStorageApiStreamingOptions {
    @TemplateParameter.BigQueryTable(
        order = 1,
        optional = true,
        description = "The dead-letter table name to output failed messages to BigQuery",
        helpText =
            "Messages failed to reach the output table for all kind of reasons (e.g., mismatched"
                + " schema, malformed json) are written to this table. If it doesn't exist, it will"
                + " be created during pipeline execution. If not specified,"
                + " \"outputTableSpec_error_records\" is used instead.",
        example = "your-project-id:your-dataset.your-table-name")
    String getOutputDeadletterTable();

    void setOutputDeadletterTable(String value);
  }
}
