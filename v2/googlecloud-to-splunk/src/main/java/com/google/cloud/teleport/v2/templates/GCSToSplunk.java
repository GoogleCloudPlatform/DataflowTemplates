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

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.coders.SplunkEventCoder;
import com.google.cloud.teleport.v2.transforms.CsvConverters;
import com.google.cloud.teleport.v2.transforms.CsvConverters.ReadCsv;
import com.google.cloud.teleport.v2.transforms.ErrorConverters.LogErrors;
import com.google.cloud.teleport.v2.transforms.SplunkConverters;
import com.google.cloud.teleport.v2.transforms.SplunkConverters.SplunkOptions;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.splunk.SplunkEvent;
import org.apache.beam.sdk.io.splunk.SplunkIO;
import org.apache.beam.sdk.io.splunk.SplunkWriteError;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link com.google.cloud.teleport.v2.templates.GCSToSplunk} pipeline is a streaming pipeline
 * which ingests data from GCS, executes a UDF, converts the output to {@link
 * org.apache.beam.sdk.io.splunk.SplunkEvent}s and writes those records into Splunk's HEC endpoint.
 * Any errors which occur in the execution of the UDF, conversion to {@link
 * org.apache.beam.sdk.io.splunk.SplunkEvent} or writing to HEC will be outputted to a GCS link.
 */
public final class GCSToSplunk {

  /** String/String Coder for FailsafeElement. */
  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

  /** The tag for the headers of the CSV if required. */
  static final TupleTag<String> CSV_HEADERS = new TupleTag<String>() {};

  /** The tag for the lines of the CSV. */
  static final TupleTag<String> CSV_LINES = new TupleTag<String>() {};

  /** The tag for the main output for the UDF. */
  static final TupleTag<FailsafeElement<String, String>> UDF_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  /** The tag for the error output of the udf. */
  static final TupleTag<FailsafeElement<String, String>> UDF_ERROR_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  /** The tag for successful {@link SplunkEvent} conversion. */
  static final TupleTag<SplunkEvent> SPLUNK_EVENT_OUT = new TupleTag<SplunkEvent>() {};

  /** The tag for failed {@link SplunkEvent} conversion. */
  static final TupleTag<FailsafeElement<String, String>> SPLUNK_EVENT_ERROR_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  /** The tag for all the elements that failed. */
  static final TupleTag<String> COMBINED_ERRORS = new TupleTag<String>() {};

  /** Logger for class. */
  static final Logger LOG = LoggerFactory.getLogger(GCSToSplunk.class);

  /**
   * The {@link GCSToSplunkOptions} class provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface GCSToSplunkOptions extends CsvConverters.CsvPipelineOptions, SplunkOptions {

    @Description("Pattern of where to write errors, ex: gs://mybucket/somepath/errors.txt")
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
    GCSToSplunkOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(GCSToSplunkOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options. This method does not wait until the
   * pipeline is finished before returning.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(GCSToSplunkOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    CoderRegistry registry = pipeline.getCoderRegistry();
    registry.registerCoderForClass(SplunkEvent.class, SplunkEventCoder.of());
    registry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    PCollectionTuple readCsvTuple = pipeline.apply("Read CSV", readFromCsv(options));

    PCollectionTuple failsafeTransformedLines =
        convertToFailsafeAndMaybeApplyUdf(readCsvTuple, options);

    PCollectionTuple splunkEventTuple = convertToSplunkEvent(failsafeTransformedLines.get(UDF_OUT));

    PCollection<SplunkWriteError> wrappedSplunkWriteErrors =
        writeToSplunk(splunkEventTuple.get(SPLUNK_EVENT_OUT), options);

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

  static PCollectionTuple convertToFailsafeAndMaybeApplyUdf(
      PCollectionTuple csvLines, GCSToSplunkOptions options) {

    String transformStepName = "Convert To Failsafe Element";

    // In order to avoid generating a graph that makes it look like a UDF was called when none was
    // intended, simply convert the input to FailsafeElements.
    if (Strings.isNullOrEmpty(options.getJavascriptTextTransformGcsPath())) {
      if (!Strings.isNullOrEmpty(options.getJsonSchemaPath())) {
        transformStepName = "Apply JSON Schema";
      }
      return csvLines.apply(
          transformStepName,
          CsvConverters.LineToFailsafeJson.newBuilder()
              .setDelimiter(options.getDelimiter())
              .setJsonSchemaPath(options.getJsonSchemaPath())
              .setHeaderTag(CSV_HEADERS)
              .setLineTag(CSV_LINES)
              .setUdfOutputTag(UDF_OUT)
              .setUdfDeadletterTag(UDF_ERROR_OUT)
              .build());
    }

    // For testing purposes, we need to do this check before creating the PTransform. Otherwise, we
    // get a NullPointerException due to the PTransform not returning
    // a value.
    if (Strings.isNullOrEmpty(options.getJavascriptTextTransformFunctionName())) {
      throw new IllegalArgumentException(
          "JavaScript function name cannot be null or empty if file is set");
    }
    transformStepName = "Apply UDF";

    return csvLines.apply(
        transformStepName,
        CsvConverters.LineToFailsafeJson.newBuilder()
            .setDelimiter(options.getDelimiter())
            .setUdfFileSystemPath(options.getJavascriptTextTransformGcsPath())
            .setUdfFunctionName(options.getJavascriptTextTransformFunctionName())
            .setJsonSchemaPath(options.getJsonSchemaPath())
            .setHeaderTag(CSV_HEADERS)
            .setLineTag(CSV_LINES)
            .setUdfOutputTag(UDF_OUT)
            .setUdfDeadletterTag(UDF_ERROR_OUT)
            .build());
  }

  static PCollectionTuple convertToSplunkEvent(
      PCollection<FailsafeElement<String, String>> transformedLines) {
    return transformedLines.apply(
        "Convert To Splunk Event",
        SplunkConverters.failsafeStringToSplunkEvent(SPLUNK_EVENT_OUT, SPLUNK_EVENT_ERROR_OUT));
  }

  static PCollection<SplunkWriteError> writeToSplunk(
      PCollection<SplunkEvent> splunkEvents, GCSToSplunkOptions options) {
    return splunkEvents.apply(
        "Write To Splunk",
        SplunkIO.write(options.getUrl(), options.getToken())
            .withBatchCount(options.getBatchCount())
            .withParallelism(options.getParallelism())
            .withDisableCertificateValidation(options.getDisableCertificateValidation()));
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
}
