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
import com.google.cloud.teleport.v2.transforms.ErrorConverters.LogErrors;
import com.google.cloud.teleport.v2.transforms.SplunkConverters;
import com.google.cloud.teleport.v2.transforms.SplunkConverters.SplunkOptions;
import com.google.cloud.teleport.v2.values.FailsafeElement;
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
 * The {@link com.google.cloud.teleport.v2.templates.PubSubToSplunk} pipeline is a streaming
 * pipeline which ingests data from Cloud Pub/Sub, executes a UDF, converts the output to {@link
 * org.apache.beam.sdk.io.splunk.SplunkEvent}s and writes those records into Splunk's HEC endpoint.
 * Any errors which occur in the execution of the UDF, conversion to {@link
 * org.apache.beam.sdk.io.splunk.SplunkEvent} or writing to HEC will be streamed into a Pub/Sub
 * topic.
 *
 * <p>NOTE: This is a work in progress, do not attempt to run this pipeline
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

  /** The tag for the dead-letter output of the udf. */
  static final TupleTag<FailsafeElement<String, String>> UDF_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  /** The tag for successful {@link SplunkEvent} conversion. */
  static final TupleTag<SplunkEvent> SPLUNK_EVENT_OUT = new TupleTag<SplunkEvent>() {};

  /** The tag for failed {@link SplunkEvent} conversion. */
  static final TupleTag<FailsafeElement<String, String>> SPLUNK_EVENT_DEADLETTER_OUT =
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
   * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
   * object to block until the pipeline is finished running if blocking programmatic execution is
   * required.
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

    /*
     * Steps:
     *  1) Step 1: Read CSV file(s) from Cloud Storage using {@link CsvConverters.ReadCsv}.
     *  2) Apply user provided UDF (if any) on the input strings.
     *  3) Convert successfully transformed messages into SplunkEvent objects
     *  4) Write SplunkEvents to Splunk's HEC end point.
     *  4a) Wrap write failures into a FailsafeElement.
     *  5) Collect errors from UDF transform (#3), SplunkEvent transform (#4)
     *     and writing to Splunk HEC (#5) and place into a GCS folder.
     */

    PCollectionTuple convertToEventTuple =
        pipeline
            // 1) Read CSV file(s) from Cloud Storage using {@link CsvConverters.ReadCsv}.
            .apply(
                "ReadCsv",
                CsvConverters.ReadCsv.newBuilder()
                    .setCsvFormat(options.getCsvFormat())
                    .setDelimiter(options.getDelimiter())
                    .setHasHeaders(options.getContainsHeaders())
                    .setInputFileSpec(options.getInputFileSpec())
                    .setHeaderTag(CSV_HEADERS)
                    .setLineTag(CSV_LINES)
                    .setFileEncoding(options.getCsvFileEncoding())
                    .build())

            // 2) Apply user provided UDF (if any) on the input strings.
            .apply(
                "ConvertLine",
                com.google.cloud.teleport.v2.transforms.CsvConverters.LineToFailsafeJson
                    .newBuilder()
                    .setDelimiter(options.getDelimiter())
                    .setUdfFileSystemPath(options.getJavascriptTextTransformGcsPath())
                    .setUdfFunctionName(options.getJavascriptTextTransformFunctionName())
                    .setJsonSchemaPath(options.getJsonSchemaPath())
                    .setHeaderTag(CSV_HEADERS)
                    .setLineTag(CSV_LINES)
                    .setUdfOutputTag(UDF_OUT)
                    .setUdfDeadletterTag(UDF_DEADLETTER_OUT)
                    .build())

            // 3) Convert successfully transformed messages into SplunkEvent objects
            .get(UDF_OUT)
            .apply(
                "ConvertToSplunkEvent",
                SplunkConverters.failsafeStringToSplunkEvent(
                    SPLUNK_EVENT_OUT, SPLUNK_EVENT_DEADLETTER_OUT));

    // 4) Write SplunkEvents to Splunk's HEC end point.
    PCollection<SplunkWriteError> writeErrors =
        convertToEventTuple
            .get(SPLUNK_EVENT_OUT)
            .apply(
                "WriteToSplunk",
                SplunkIO.write(options.getUrl(), options.getToken())
                    .withBatchCount(options.getBatchCount())
                    .withParallelism(options.getParallelism())
                    .withDisableCertificateValidation(options.getDisableCertificateValidation()));

    // 4a) Wrap write failures into a FailsafeElement.
    PCollection<FailsafeElement<String, String>> wrappedSplunkWriteErrors =
        writeErrors.apply(
            "WrapSplunkWriteErrors", ParDo.of(new SplunkWriteErrorToFailsafeElementDoFn()));

    // 5) Collect errors from UDF transform (#2), SplunkEvent transform (#3)
    //     and writing to Splunk HEC (#4) and place into a GCS folder.
    PCollectionTuple.of(
            COMBINED_ERRORS,
            PCollectionList.of(
                    ImmutableList.of(
                        convertToEventTuple.get(SPLUNK_EVENT_DEADLETTER_OUT),
                        wrappedSplunkWriteErrors,
                        convertToEventTuple.get(UDF_DEADLETTER_OUT)))
                .apply("FlattenErrors", Flatten.pCollections())
                .apply("ConvertErrorsToString", ParDo.of(new FailsafeElementToStringDoFn())))
        .apply(
            LogErrors.newBuilder()
                .setErrorWritePath(options.getInvalidOutputPath())
                .setErrorTag(COMBINED_ERRORS)
                .build());

    return pipeline.run();
  }
}

class SplunkWriteErrorToFailsafeElementDoFn
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

class FailsafeElementToStringDoFn extends DoFn<FailsafeElement<String, String>, String> {
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
