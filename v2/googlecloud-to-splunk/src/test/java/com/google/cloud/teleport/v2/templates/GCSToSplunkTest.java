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

import static com.google.cloud.teleport.v2.templates.GCSToSplunk.SPLUNK_EVENT_OUT;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.UDF_OUT;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.convertToFailsafeAndMaybeApplyUdf;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.convertToSplunkEvent;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.readFromCsv;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.coders.SplunkEventCoder;
import com.google.cloud.teleport.v2.templates.GCSToSplunk.GCSToSplunkOptions;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.io.Resources;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.splunk.SplunkEvent;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;

/** Test cases for the {@link GCSToSplunk} class. */
public final class GCSToSplunkTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

  private static final String TRANSFORM_FILE_PATH = Resources.getResource("splunkUdf.js").getPath();
  private static final String NO_HEADER_CSV_FILE_PATH =
      Resources.getResource("no_header.csv").getPath();
  private static final String HEADER_CSV_FILE_PATH =
      Resources.getResource("with_headers.csv").getPath();
  private static final String JSON_SCHEMA_FILE_PATH =
      Resources.getResource("testSchema.json").getPath();

  @Test
  public void testGCSToSplunkUdf() {
    // Arrange
    final String stringifiedJsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";
    SplunkEvent expectedSplunkEvent =
        SplunkEvent.newBuilder().withEvent(stringifiedJsonRecord).create();

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForClass(SplunkEvent.class, SplunkEventCoder.of());
    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    GCSToSplunkOptions options = PipelineOptionsFactory.create().as(GCSToSplunkOptions.class);

    options.setJavascriptTextTransformGcsPath(TRANSFORM_FILE_PATH);
    options.setJavascriptTextTransformFunctionName("transform");
    options.setContainsHeaders(false);
    options.setInputFileSpec(NO_HEADER_CSV_FILE_PATH);

    // Act
    PCollectionTuple readCsvOut = pipeline.apply("Read CSV", readFromCsv(options));
    PCollectionTuple transformedLines = convertToFailsafeAndMaybeApplyUdf(readCsvOut, options);
    PCollectionTuple splunkEventTuple = convertToSplunkEvent(transformedLines.get(UDF_OUT));

    // Assert
    PAssert.that(transformedLines.get(UDF_OUT))
        .satisfies(
            collection -> {
              FailsafeElement element = collection.iterator().next();
              assertThat(element.getPayload()).isEqualTo(stringifiedJsonRecord);
              return null;
            });
    PAssert.that(splunkEventTuple.get(SPLUNK_EVENT_OUT)).containsInAnyOrder(expectedSplunkEvent);

    //  Execute pipeline
    pipeline.run();
  }

  @Test
  public void testGCSToSplunkHeaders() {
    // Arrange
    final String stringifiedJsonRecord = "{\"id\":\"008\",\"state\":\"CA\",\"price\":\"26.23\"}";
    SplunkEvent expectedSplunkEvent =
        SplunkEvent.newBuilder().withEvent(stringifiedJsonRecord).create();

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForClass(SplunkEvent.class, SplunkEventCoder.of());
    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    GCSToSplunkOptions options = PipelineOptionsFactory.create().as(GCSToSplunkOptions.class);

    options.setContainsHeaders(true);
    options.setInputFileSpec(HEADER_CSV_FILE_PATH);

    // Act
    PCollectionTuple readCsvOut = pipeline.apply("Read CSV", readFromCsv(options));
    PCollectionTuple transformedLines = convertToFailsafeAndMaybeApplyUdf(readCsvOut, options);
    PCollectionTuple splunkEventTuple = convertToSplunkEvent(transformedLines.get(UDF_OUT));

    // Assert
    PAssert.that(transformedLines.get(UDF_OUT))
        .satisfies(
            collection -> {
              FailsafeElement element = collection.iterator().next();
              assertThat(element.getPayload()).isEqualTo(stringifiedJsonRecord);
              return null;
            });
    PAssert.that(splunkEventTuple.get(SPLUNK_EVENT_OUT)).containsInAnyOrder(expectedSplunkEvent);

    //  Execute pipeline
    pipeline.run();
  }

  @Test
  public void testGCSToSplunkJsonSchema() {
    // Arrange
    final String stringifiedJsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";
    SplunkEvent expectedSplunkEvent =
        SplunkEvent.newBuilder().withEvent(stringifiedJsonRecord).create();

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForClass(SplunkEvent.class, SplunkEventCoder.of());
    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    GCSToSplunkOptions options = PipelineOptionsFactory.create().as(GCSToSplunkOptions.class);

    options.setJsonSchemaPath(JSON_SCHEMA_FILE_PATH);
    options.setContainsHeaders(false);
    options.setInputFileSpec(NO_HEADER_CSV_FILE_PATH);

    // Act
    PCollectionTuple readCsvOut = pipeline.apply("Read CSV", readFromCsv(options));
    PCollectionTuple transformedLines = convertToFailsafeAndMaybeApplyUdf(readCsvOut, options);
    PCollectionTuple splunkEventTuple = convertToSplunkEvent(transformedLines.get(UDF_OUT));

    // Assert
    PAssert.that(transformedLines.get(UDF_OUT))
        .satisfies(
            collection -> {
              FailsafeElement element = collection.iterator().next();
              assertThat(element.getPayload()).isEqualTo(stringifiedJsonRecord);
              return null;
            });
    PAssert.that(splunkEventTuple.get(SPLUNK_EVENT_OUT)).containsInAnyOrder(expectedSplunkEvent);

    //  Execute pipeline
    pipeline.run();
  }

  // @Test
  // public void testGCSToSplunkWriteErrors() {
  //   // Arrange
  //   final String stringifiedError =
  //       "Payload: test-payload. Error Message: test-message. Splunk write status code: 123.";
  //   CoderRegistry coderRegistry = pipeline.getCoderRegistry();
  //   coderRegistry.registerCoderForType(
  //       FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);
  //
  //   String payload = "test-payload";
  //   String message = "test-message";
  //   Integer statusCode = 123;
  //
  //   SplunkWriteError splunkWriteError =
  //       SplunkWriteError.newBuilder()
  //           .withPayload(payload)
  //           .withStatusCode(statusCode)
  //           .withStatusMessage(message)
  //           .create();
  //
  //   // Act
  //   PCollection<SplunkWriteError> splunkErrorCollection =
  //       pipeline.apply(
  //           "Create Input data",
  // Create.of(splunkWriteError).withCoder(SplunkWriteErrorCoder.of()));
  //
  //   // Assert
  //   PAssert.that(splunkErrorCollection).containsInAnyOrder(splunkWriteError);
  //
  //   // Act
  //   PCollection<FailsafeElement<String, String>> wrappedSplunkWriteErrors =
  //       wrapSplunkErrorsToFailsafe(splunkErrorCollection);
  //
  //   // Assert
  //   PAssert.that(wrappedSplunkWriteErrors)
  //       .satisfies(
  //           collection -> {
  //             FailsafeElement element = collection.iterator().next();
  //             assertThat(element.getPayload(), is(equalTo(payload)));
  //             assertThat(
  //                 element.getErrorMessage(),
  //                 is(equalTo(message + ". Splunk write status code: " + statusCode)));
  //             return null;
  //           });
  //
  //   // Act
  //   PCollectionTuple flattenedErrors = getStringifiedFailsafeErrors(wrappedSplunkWriteErrors);
  //
  //   // Assert
  //   PAssert.that(flattenedErrors.get(COMBINED_ERRORS)).containsInAnyOrder(stringifiedError);
  //
  //   //  Execute pipeline
  //   pipeline.run();
  // }
}
