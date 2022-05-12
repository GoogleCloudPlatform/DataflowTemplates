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

import static com.google.cloud.teleport.v2.templates.GCSToSplunk.CSV_HEADERS;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.CSV_LINES;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.SPLUNK_EVENT_DEADLETTER_OUT;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.SPLUNK_EVENT_OUT;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.UDF_DEADLETTER_OUT;
import static com.google.cloud.teleport.v2.templates.GCSToSplunk.UDF_OUT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.coders.SplunkEventCoder;
import com.google.cloud.teleport.v2.templates.GCSToSplunk.GCSToSplunkOptions;
import com.google.cloud.teleport.v2.transforms.CsvConverters.ReadCsv;
import com.google.cloud.teleport.v2.transforms.SplunkConverters;
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
public class GCSToSplunkTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

  private static final String TRANSFORM_FILE_PATH =
      Resources.getResource("elasticUdf.js").getPath();
  private static final String NO_HEADER_CSV_FILE_PATH =
      Resources.getResource("no_header.csv").getPath();
  private static final String HEADER_CSV_FILE_PATH =
      Resources.getResource("with_headers.csv").getPath();
  private static final String JSON_SCHEMA_FILE_PATH =
      Resources.getResource("testSchema.json").getPath();

  /** Tests the {@link GCSToSplunk} pipeline using a Udf to parse the Csv. */
  @Test
  public void testGCSToSplunkUdfE2E() {
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

    PCollectionTuple readCsvOut =
        pipeline
            .apply(
                "ReadCsv",
                ReadCsv.newBuilder()
                    .setCsvFormat(options.getCsvFormat())
                    .setDelimiter(options.getDelimiter())
                    .setHasHeaders(options.getContainsHeaders())
                    .setInputFileSpec(options.getInputFileSpec())
                    .setHeaderTag(CSV_HEADERS)
                    .setLineTag(CSV_LINES)
                    .setFileEncoding(options.getCsvFileEncoding())
                    .build())
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
                    .build());

    PAssert.that(readCsvOut.get(UDF_OUT))
        .satisfies(
            collection -> {
              FailsafeElement element = collection.iterator().next();
              System.out.println(element);
              assertThat(element.getPayload(), is(equalTo(stringifiedJsonRecord)));
              return null;
            });

    PCollectionTuple convertToSplunkEventOut =
        readCsvOut
            .get(UDF_OUT)
            .apply(
                "ConvertToSplunkEvent",
                SplunkConverters.failsafeStringToSplunkEvent(
                    SPLUNK_EVENT_OUT, SPLUNK_EVENT_DEADLETTER_OUT));
    ;

    // Assert
    PAssert.that(convertToSplunkEventOut.get(SPLUNK_EVENT_OUT))
        .containsInAnyOrder(expectedSplunkEvent);

    //  Execute pipeline
    pipeline.run();
  }

  /** Tests the {@link GCSToSplunk} pipeline with the headers of the Csv. */
  @Test
  public void testGCSToSplunkHeadersE2E() {
    final String stringifiedJsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":\"26.23\"}";
    SplunkEvent expectedSplunkEvent =
        SplunkEvent.newBuilder().withEvent(stringifiedJsonRecord).create();

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForClass(SplunkEvent.class, SplunkEventCoder.of());
    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    GCSToSplunkOptions options = PipelineOptionsFactory.create().as(GCSToSplunkOptions.class);

    options.setContainsHeaders(true);
    options.setInputFileSpec(HEADER_CSV_FILE_PATH);

    PCollectionTuple readCsvOut =
        pipeline
            .apply(
                "ReadCsv",
                ReadCsv.newBuilder()
                    .setCsvFormat(options.getCsvFormat())
                    .setDelimiter(options.getDelimiter())
                    .setHasHeaders(options.getContainsHeaders())
                    .setInputFileSpec(options.getInputFileSpec())
                    .setHeaderTag(CSV_HEADERS)
                    .setLineTag(CSV_LINES)
                    .setFileEncoding(options.getCsvFileEncoding())
                    .build())
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
                    .build());

    PAssert.that(readCsvOut.get(UDF_OUT))
        .satisfies(
            collection -> {
              FailsafeElement element = collection.iterator().next();
              System.out.println(element);
              assertThat(element.getPayload(), is(equalTo(stringifiedJsonRecord)));
              return null;
            });

    PCollectionTuple convertToSplunkEventOut =
        readCsvOut
            .get(UDF_OUT)
            .apply(
                "ConvertToSplunkEvent",
                SplunkConverters.failsafeStringToSplunkEvent(
                    SPLUNK_EVENT_OUT, SPLUNK_EVENT_DEADLETTER_OUT));
    ;

    // Assert
    PAssert.that(convertToSplunkEventOut.get(SPLUNK_EVENT_OUT))
        .containsInAnyOrder(expectedSplunkEvent);

    //  Execute pipeline
    pipeline.run();
  }
}
