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

import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.templates.TextToBigQueryStreaming.TextToBigQueryStreamingOptions;
import com.google.cloud.teleport.templates.common.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.templates.common.ErrorConverters.WriteStringMessageErrors;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.util.ResourceUtils;
import com.google.cloud.teleport.util.ValueProviderUtils;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Watch.Growth;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link TextToBigQueryStreaming} is a streaming version of {@link TextIOToBigQuery} pipeline
 * that reads text files, applies a JavaScript UDF and writes the output to BigQuery. The pipeline
 * continuously polls for new files, reads them row-by-row and processes each record into BigQuery.
 * The polling interval is set at 10 seconds.
 *
 * <p>Example Usage:
 *
 * <pre>
 * {@code mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.TextToBigQueryStreaming \
 * -Dexec.args="\
 * --project=${PROJECT_ID} \
 * --stagingLocation=gs://${STAGING_BUCKET}/staging \
 * --tempLocation=gs://${STAGING_BUCKET}/tmp \
 * --runner=DataflowRunner \
 * --inputFilePattern=gs://path/to/input* \
 * --JSONPath=gs://path/to/json/schema.json \
 * --outputTable={$PROJECT_ID}:${OUTPUT_DATASET}.${OUTPUT_TABLE} \
 * --javascriptTextTransformGcsPath=gs://path/to/transform/udf.js \
 * --javascriptTextTransformFunctionName=${TRANSFORM_NAME} \
 * --bigQueryLoadingTemporaryDirectory=gs://${STAGING_BUCKET}/tmp \
 * --outputDeadletterTable=${PROJECT_ID}:${ERROR_DATASET}.${ERROR_TABLE}"
 * }
 * </pre>
 */
@Template(
    name = "Stream_GCS_Text_to_BigQuery",
    category = TemplateCategory.STREAMING,
    displayName = "Cloud Storage Text to BigQuery (Stream)",
    description =
        "A streaming pipeline that can read text files stored in Cloud Storage, perform a transform via a user defined JavaScript function, and stream the results into BigQuery. This pipeline requires a JavaScript function and a JSON representation of the BigQuery TableSchema.",
    optionsClass = TextToBigQueryStreamingOptions.class,
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

    // Parse the user options passed from the command-line
    TextToBigQueryStreamingOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(TextToBigQueryStreamingOptions.class);

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
                    .withJsonSchema(getSchemaFromGCS(options.getJSONPath()))
                    .to(options.getOutputTable())
                    .withExtendedErrorInfo()
                    .withoutValidation()
                    .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                    .withMethod(Method.STREAMING_INSERTS)
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                    .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory()));

    // Elements that failed inserts into BigQuery are extracted and converted to FailsafeElement
    PCollection<FailsafeElement<String, String>> failedInserts =
        writeResult
            .getFailedInsertsWithErr()
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
                    ValueProviderUtils.maybeUseDefaultDeadletterTable(
                        options.getOutputDeadletterTable(),
                        options.getOutputTable(),
                        DEFAULT_DEADLETTER_TABLE_SUFFIX))
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
  public static FailsafeElement<String, String> wrapBigQueryInsertError(
      BigQueryInsertError insertError) {

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
   * Method to read a BigQuery schema file from GCS and return the file contents as a string.
   *
   * @param gcsPath Path string for the schema file in GCS.
   * @return File contents as a string.
   */
  private static ValueProvider<String> getSchemaFromGCS(ValueProvider<String> gcsPath) {
    return NestedValueProvider.of(
        gcsPath,
        new SimpleFunction<String, String>() {
          @Override
          public String apply(String input) {
            ResourceId sourceResourceId = FileSystems.matchNewResource(input, false);

            String schema;
            try (ReadableByteChannel rbc = FileSystems.open(sourceResourceId)) {
              try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                try (WritableByteChannel wbc = Channels.newChannel(baos)) {
                  ByteStreams.copy(rbc, wbc);
                  schema = baos.toString(Charsets.UTF_8.name());
                  LOG.info("Extracted schema: " + schema);
                }
              }
            } catch (IOException e) {
              LOG.error("Error extracting schema: " + e.getMessage());
              throw new RuntimeException(e);
            }
            return schema;
          }
        });
  }

  /**
   * The {@link TextToBigQueryStreamingOptions} class provides the custom execution options passed
   * by the executor at the command-line.
   */
  public interface TextToBigQueryStreamingOptions extends TextIOToBigQuery.Options {
    @TemplateParameter.BigQueryTable(
        order = 1,
        optional = true,
        description = "The dead-letter table name to output failed messages to BigQuery",
        helpText =
            "Messages failed to reach the output table for all kind of reasons (e.g., mismatched "
                + "schema, malformed json) are written to this table. If it doesn't exist, it will be "
                + "created during pipeline execution. If not specified, \"outputTableSpec_error_records\" "
                + "is used instead.",
        example = "your-project-id:your-dataset.your-table-name")
    ValueProvider<String> getOutputDeadletterTable();

    void setOutputDeadletterTable(ValueProvider<String> value);
  }
}
