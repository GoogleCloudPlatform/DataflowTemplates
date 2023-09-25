/*
 * Copyright (C) 2022 Google LLC
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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.coders.SplunkEventCoder;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.templates.GCSToSplunk.GCSToSplunkOptions;
import com.google.cloud.teleport.v2.transforms.CsvConverters;
import com.google.cloud.teleport.v2.transforms.CsvConverters.LineToFailsafeJson;
import com.google.cloud.teleport.v2.transforms.CsvConverters.ReadCsv;
import com.google.cloud.teleport.v2.transforms.ErrorConverters.LogErrors;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.v2.transforms.SplunkConverters;
import com.google.cloud.teleport.v2.transforms.SplunkConverters.FailsafeStringToSplunkEvent;
import com.google.cloud.teleport.v2.transforms.SplunkConverters.SplunkOptions;
import com.google.cloud.teleport.v2.utils.KMSUtils;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.cloud.teleport.v2.values.SplunkTokenSource;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.EnumUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.splunk.SplunkEvent;
import org.apache.beam.sdk.io.splunk.SplunkIO;
import org.apache.beam.sdk.io.splunk.SplunkWriteError;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link com.google.cloud.teleport.v2.templates.GCSToSplunk} pipeline is a batch pipeline which
 * ingests data from GCS, optionally executes a UDF, converts the output to {@link
 * org.apache.beam.sdk.io.splunk.SplunkEvent}s and writes those records into Splunk's HEC endpoint.
 * Any errors which occur in the execution of the UDF, conversion to {@link
 * org.apache.beam.sdk.io.splunk.SplunkEvent} or writing to HEC will be outputted to a GCS link.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-splunk/README_GCS_To_Splunk.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "GCS_To_Splunk",
    category = TemplateCategory.BATCH,
    displayName = "Cloud Storage To Splunk",
    description = {
      "A pipeline that reads a set of Text (CSV) files in Cloud Storage and writes to Splunk's"
          + " HTTP Event Collector (HEC).",
      "The template creates the Splunk payload as a JSON element using either CSV headers (default), JSON schema or JavaScript UDF. "
          + "If a Javascript UDF and JSON schema are both inputted as parameters, only the Javascript UDF will be executed."
    },
    optionsClass = GCSToSplunkOptions.class,
    flexContainerName = "gcs-to-splunk",
    contactInformation = "https://cloud.google.com/support")
public final class GCSToSplunk {

  /** String/String Coder for FailsafeElement. */
  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

  /** The tag for the headers of the CSV if required. */
  private static final TupleTag<String> CSV_HEADERS = new TupleTag<String>() {};

  /** The tag for the lines of the CSV. */
  private static final TupleTag<String> CSV_LINES = new TupleTag<String>() {};

  /** The tag for the main output for the UDF. */
  @VisibleForTesting
  static final TupleTag<FailsafeElement<String, String>> UDF_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  /** The tag for the error output of the udf. */
  @VisibleForTesting
  static final TupleTag<FailsafeElement<String, String>> UDF_ERROR_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  /** The tag for successful {@link SplunkEvent} conversion. */
  @VisibleForTesting
  static final TupleTag<SplunkEvent> SPLUNK_EVENT_OUT = new TupleTag<SplunkEvent>() {};

  /** The tag for failed {@link SplunkEvent} conversion. */
  @VisibleForTesting
  static final TupleTag<FailsafeElement<String, String>> SPLUNK_EVENT_ERROR_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  /** The tag for all the elements that failed. */
  @VisibleForTesting static final TupleTag<String> COMBINED_ERRORS = new TupleTag<String>() {};

  /** Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(GCSToSplunk.class);

  /**
   * The {@link GCSToSplunkOptions} class provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface GCSToSplunkOptions
      extends CsvConverters.CsvPipelineOptions, SplunkOptions, JavascriptTextTransformerOptions {

    @TemplateParameter.GcsWriteFolder(
        order = 1,
        description = "Invalid events output path",
        helpText =
            "Cloud Storage path where to write objects that could not be converted to Splunk"
                + " objects or pushed to Splunk.",
        example = "gs://your-bucket/your-path")
    String getInvalidOutputPath();

    void setInvalidOutputPath(String value);
  }

  /**
   * Main entry-point for the pipeline. Reads in the command-line arguments, parses them, and
   * executes the pipeline.
   *
   * @param args Arguments passed in from the command-line.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    GCSToSplunkOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(GCSToSplunkOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(GCSToSplunkOptions options) {
    ValueProvider<String> token = getMaybeDecryptedToken(options);
    Pipeline pipeline = Pipeline.create(options);

    CoderRegistry registry = pipeline.getCoderRegistry();
    registry.registerCoderForClass(SplunkEvent.class, SplunkEventCoder.of());
    registry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    PCollectionTuple readCsvTuple = pipeline.apply("Read CSV", readFromCsv(options));

    PCollectionTuple failsafeTransformedLines =
        readCsvTuple.apply("Convert To JSON", convertToFailsafeAndMaybeApplyUdf(options));

    PCollectionTuple splunkEventTuple =
        failsafeTransformedLines
            .get(UDF_OUT)
            .apply("Convert to Splunk Event", convertToSplunkEvent());

    PCollection<SplunkWriteError> wrappedSplunkWriteErrors =
        splunkEventTuple
            .get(SPLUNK_EVENT_OUT)
            .apply("Write to Splunk", writeToSplunk(options, token));

    flattenErrorsAndConvertToString(
            failsafeTransformedLines.get(UDF_ERROR_OUT),
            splunkEventTuple.get(SPLUNK_EVENT_ERROR_OUT),
            wrappedSplunkWriteErrors)
        .apply("Output Errors To GCS", writeErrorsToGCS(options));

    return pipeline.run();
  }

  static ReadCsv readFromCsv(GCSToSplunkOptions options) {
    return CsvConverters.ReadCsv.newBuilder()
        .setCsvFormat(options.getCsvFormat())
        .setDelimiter(options.getDelimiter())
        .setHasHeaders(options.getContainsHeaders())
        .setInputFileSpec(options.getInputFileSpec())
        .setHeaderTag(CSV_HEADERS)
        .setLineTag(CSV_LINES)
        .setFileEncoding(options.getCsvFileEncoding())
        .build();
  }

  static LineToFailsafeJson convertToFailsafeAndMaybeApplyUdf(GCSToSplunkOptions options) {
    return CsvConverters.LineToFailsafeJson.newBuilder()
        .setDelimiter(options.getDelimiter())
        .setUdfFileSystemPath(options.getJavascriptTextTransformGcsPath())
        .setUdfFunctionName(options.getJavascriptTextTransformFunctionName())
        .setJsonSchemaPath(options.getJsonSchemaPath())
        .setHeaderTag(CSV_HEADERS)
        .setLineTag(CSV_LINES)
        .setUdfOutputTag(UDF_OUT)
        .setUdfDeadletterTag(UDF_ERROR_OUT)
        .build();
  }

  static FailsafeStringToSplunkEvent convertToSplunkEvent() {
    return SplunkConverters.failsafeStringToSplunkEvent(SPLUNK_EVENT_OUT, SPLUNK_EVENT_ERROR_OUT);
  }

  static SplunkIO.Write writeToSplunk(GCSToSplunkOptions options, ValueProvider<String> token) {
    return SplunkIO.write(StaticValueProvider.of(options.getUrl()), token)
        .withBatchCount(options.getBatchCount())
        .withParallelism(options.getParallelism())
        .withDisableCertificateValidation(options.getDisableCertificateValidation())
        .withRootCaCertificatePath(options.getRootCaCertificatePath())
        .withEnableBatchLogs(options.getEnableBatchLogs())
        .withEnableGzipHttpCompression(options.getEnableGzipHttpCompression());
  }

  static PCollectionTuple flattenErrorsAndConvertToString(
      PCollection<FailsafeElement<String, String>> failsafeFailedTransformedLines,
      PCollection<FailsafeElement<String, String>> splunkEventFailedTransformedLines,
      PCollection<SplunkWriteError> splunkWriteErrors) {
    PCollection<FailsafeElement<String, String>> wrappedSplunkWriteErrors =
        splunkWriteErrors.apply(
            "Wrap Splunk Write Errors", ParDo.of(new SplunkWriteErrorToFailsafeElementDoFn()));

    return PCollectionTuple.of(
        COMBINED_ERRORS,
        PCollectionList.of(
                ImmutableList.of(
                    failsafeFailedTransformedLines,
                    wrappedSplunkWriteErrors,
                    splunkEventFailedTransformedLines))
            .apply("Flatten Errors", Flatten.pCollections())
            .apply("Convert Errors To String", ParDo.of(new FailsafeElementToStringDoFn())));
  }

  static LogErrors writeErrorsToGCS(GCSToSplunkOptions options) {
    return LogErrors.newBuilder()
        .setErrorWritePath(options.getInvalidOutputPath())
        .setErrorTag(COMBINED_ERRORS)
        .build();
  }

  private static class SplunkWriteErrorToFailsafeElementDoFn
      extends DoFn<SplunkWriteError, FailsafeElement<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      SplunkWriteError error = context.element();
      FailsafeElement<String, String> failsafeElement =
          FailsafeElement.of(error.payload(), error.payload());

      String statusMessage = "";
      if (error.statusMessage() != null) {
        failsafeElement.setErrorMessage(error.statusMessage());
        statusMessage = failsafeElement.getErrorMessage();
      }

      if (error.statusCode() != null) {
        failsafeElement.setErrorMessage(
            statusMessage + ". Splunk write status code: " + error.statusCode());
      }
      context.output(failsafeElement);
    }
  }

  private static class FailsafeElementToStringDoFn
      extends DoFn<FailsafeElement<String, String>, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      FailsafeElement<String, String> error = context.element();
      String errorString = "";

      if (error.getPayload() != null) {
        errorString += "Payload: " + error.getPayload() + ". ";
      }

      if (error.getErrorMessage() != null) {
        errorString += "Error Message: " + error.getErrorMessage() + ".";
      }
      context.output(errorString);
    }
  }

  private static ValueProvider<String> getMaybeDecryptedToken(GCSToSplunkOptions options) {
    SplunkTokenSource splunkTokenSource =
        EnumUtils.getEnum(SplunkTokenSource.class, options.getTokenSource());
    switch (splunkTokenSource) {
      case SECRET_MANAGER:
        checkArgument(
            options.getTokenSecretId() != null,
            "tokenSecretId is required to retrieve token from Secret Manager");
        LOG.info("Using token secret stored in Secret Manager");
        return StaticValueProvider.of(SecretManagerUtils.getSecret(options.getTokenSecretId()));
      case KMS:
        checkArgument(
            options.getToken() != null && options.getTokenKMSEncryptionKey() != null,
            "token and tokenKmsEncryptionKey are required while decrypting using KMS Key");
        LOG.info("Using KMS Key to decrypt token");
        return KMSUtils.maybeDecrypt(options.getToken(), options.getTokenKMSEncryptionKey());
      case PLAINTEXT:
      default:
        checkArgument(options.getToken() != null, "token is required for writing events");
        LOG.warn(
            "Using plaintext token. Consider storing the token in Secret Manager or "
                + "pass an encrypted token and a KMS Key to decrypt it");
        return StaticValueProvider.of(options.getToken());
    }
  }
}
