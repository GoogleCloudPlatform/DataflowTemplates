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

import static com.google.cloud.teleport.v2.templates.GCSToSplunk.COMBINED_ERRORS;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.SPLUNK_EVENT_ERROR_OUT;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.SPLUNK_EVENT_OUT;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.UDF_ERROR_OUT;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.UDF_OUT;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.convertToFailsafeAndMaybeApplyUdf;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.convertToSplunkEvent;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.flattenErrorsAndConvertToString;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.readFromCsv;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.writeErrorsToGCS;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.coders.SplunkEventCoder;
import com.google.cloud.teleport.v2.coders.SplunkWriteErrorCoder;
import com.google.cloud.teleport.v2.templates.GCSToSplunk.GCSToSplunkOptions;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.splunk.SplunkEvent;
import org.apache.beam.sdk.io.splunk.SplunkWriteError;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test cases for the {@link GCSToSplunk} class. */
public final class GCSToSplunkTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

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
  public void testGCSToSplunkReadUdf() {
    // Arrange
    String stringifiedJsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";
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
    PAssert.that(transformedLines.get(UDF_ERROR_OUT)).empty();
    PAssert.that(splunkEventTuple.get(SPLUNK_EVENT_OUT)).containsInAnyOrder(expectedSplunkEvent);
    PAssert.that(splunkEventTuple.get(SPLUNK_EVENT_ERROR_OUT)).empty();

    //  Execute pipeline
    pipeline.run();
  }

  @Test
  public void testGCSToSplunkReadHeaders() {
    // Arrange
    String stringifiedJsonRecord = "{\"id\":\"008\",\"state\":\"CA\",\"price\":\"26.23\"}";
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
    PAssert.that(transformedLines.get(UDF_ERROR_OUT)).empty();
    PAssert.that(splunkEventTuple.get(SPLUNK_EVENT_OUT)).containsInAnyOrder(expectedSplunkEvent);
    PAssert.that(splunkEventTuple.get(SPLUNK_EVENT_ERROR_OUT)).empty();

    //  Execute pipeline
    pipeline.run();
  }

  @Test
  public void testGCSToSplunkReadJsonSchema() {
    // Arrange
    String stringifiedJsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";
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
    PAssert.that(transformedLines.get(UDF_ERROR_OUT)).empty();
    PAssert.that(splunkEventTuple.get(SPLUNK_EVENT_OUT)).containsInAnyOrder(expectedSplunkEvent);
    PAssert.that(splunkEventTuple.get(SPLUNK_EVENT_ERROR_OUT)).empty();

    //  Execute pipeline
    pipeline.run();
  }

  @Test
  public void testGCSToSplunkConvertWriteErrors() {
    // Arrange
    String stringifiedSplunkError =
        "Payload: test-payload. Error Message: test-message. Splunk write status code: 123.";
    String firstStringifiedFailsafeError = "Payload: world. Error Message: failed!.";
    String secondStringifiedFailsafeError = "Payload: one. Error Message: error!.";

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    SplunkWriteError splunkWriteError =
        SplunkWriteError.newBuilder()
            .withPayload("test-payload")
            .withStatusCode(123)
            .withStatusMessage("test-message")
            .create();

    PCollection<SplunkWriteError> splunkErrorCollection =
        pipeline.apply(
            "Add Splunk Errors", Create.of(splunkWriteError).withCoder(SplunkWriteErrorCoder.of()));

    FailsafeElement<String, String> firstFailsafeElement =
        FailsafeElement.of("hello", "world").setErrorMessage("failed!");

    PCollection<FailsafeElement<String, String>> firstFailsafeElementCollection =
        pipeline.apply(
            "Add FailsafeElements to First",
            Create.of(firstFailsafeElement).withCoder(FAILSAFE_ELEMENT_CODER));

    FailsafeElement<String, String> secondFailsafeElement =
        FailsafeElement.of("another", "one").setErrorMessage("error!");

    PCollection<FailsafeElement<String, String>> secondFailsafeElementCollection =
        pipeline.apply(
            "Add FailsafeElements to Second",
            Create.of(secondFailsafeElement).withCoder(FAILSAFE_ELEMENT_CODER));

    // Act
    PCollectionTuple stringifiedErrors =
        flattenErrorsAndConvertToString(
            firstFailsafeElementCollection, secondFailsafeElementCollection, splunkErrorCollection);

    // Assert
    PAssert.that(stringifiedErrors.get(COMBINED_ERRORS))
        .containsInAnyOrder(
            stringifiedSplunkError, firstStringifiedFailsafeError, secondStringifiedFailsafeError);

    //  Execute pipeline
    pipeline.run();
  }

  @Test
  public void testGCSToSplunkWriteErrorsToFolder() throws IOException {
    // Arrange
    String stringifiedSplunkError =
        "Payload: test-payload. Error Message: test-message. Splunk write status code: 123.";

    PCollection<String> stringifiedErrorCollection =
        pipeline.apply(
            "Add Stringified Errors",
            Create.of(stringifiedSplunkError).withCoder(StringUtf8Coder.of()));

    PCollectionTuple stringifiedErrorTuple =
        PCollectionTuple.of(COMBINED_ERRORS, stringifiedErrorCollection);


    GCSToSplunkOptions options = PipelineOptionsFactory.create().as(GCSToSplunkOptions.class);

    options.setInvalidOutputPath(tmpFolder.getRoot().getAbsolutePath() + "errors.txt");

    // Act
    stringifiedErrorTuple.apply("Output Errors To GCS", writeErrorsToGCS(options));

    //  Execute pipeline
    pipeline.run();

    // Assert
    File file = new File(tmpFolder.getRoot().getAbsolutePath() + "errors.txt-00000-of-00001");
    String fileContents = Files.toString(file, Charsets.UTF_8);
    assertThat(fileContents).contains(stringifiedSplunkError);
  }
}
