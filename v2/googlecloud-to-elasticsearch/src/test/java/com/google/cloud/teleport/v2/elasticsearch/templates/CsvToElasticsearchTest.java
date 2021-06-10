/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.teleport.v2.elasticsearch.templates;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.CsvConverters;
import com.google.cloud.teleport.v2.transforms.ElasticsearchTransforms;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.io.Resources;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test cases for the {@link CsvToElasticsearch} class. */
public class CsvToElasticsearchTest {

  private static final String CSV_RESOURCES_DIR = "CsvToElasticsearchTest/";
  private static final String TRANSFORM_FILE_PATH =
      Resources.getResource(CSV_RESOURCES_DIR + "elasticUdf.js").getPath();
  private static final String NO_HEADER_CSV_FILE_PATH =
      Resources.getResource(CSV_RESOURCES_DIR + "no_header.csv").getPath();
  private static final String HEADER_CSV_FILE_PATH =
      Resources.getResource(CSV_RESOURCES_DIR + "with_headers.csv").getPath();
  private static final String JSON_SCHEMA_FILE_PATH =
      Resources.getResource(CSV_RESOURCES_DIR + "testSchema.json").getPath();
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  /** Tests the {@link CsvToElasticsearch} pipeline using a Udf to parse the Csv. */
  @Test
  public void testCsvToElasticsearchUdfE2E() {

    final String record = "007,CA,26.23";
    final String stringifiedJsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";

    final FailsafeElementCoder<String, String> coder =
        FailsafeElementCoder.of(
            NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    CsvToElasticsearch.CsvToElasticsearchOptions options =
        PipelineOptionsFactory.create().as(CsvToElasticsearch.CsvToElasticsearchOptions.class);

    options.setJavascriptTextTransformGcsPath(TRANSFORM_FILE_PATH);
    options.setJavascriptTextTransformFunctionName("transform");
    options.setContainsHeaders(false);
    options.setInputFileSpec(NO_HEADER_CSV_FILE_PATH);

    // Build pipeline with no headers.
    PCollectionTuple readCsvOut =
        pipeline
            .apply(
                "ReadCsv",
                CsvConverters.ReadCsv.newBuilder()
                    .setCsvFormat(options.getCsvFormat())
                    .setDelimiter(options.getDelimiter())
                    .setHasHeaders(options.getContainsHeaders())
                    .setInputFileSpec(options.getInputFileSpec())
                    .setHeaderTag(CsvToElasticsearch.CSV_HEADERS)
                    .setLineTag(CsvToElasticsearch.CSV_LINES)
                    .setFileEncoding(options.getCsvFileEncoding())
                    .build())
            .apply(
                "ConvertLine",
                CsvConverters.LineToFailsafeJson.newBuilder()
                    .setDelimiter(options.getDelimiter())
                    .setUdfFileSystemPath(options.getJavascriptTextTransformGcsPath())
                    .setUdfFunctionName(options.getJavascriptTextTransformFunctionName())
                    .setJsonSchemaPath(options.getJsonSchemaPath())
                    .setHeaderTag(CsvToElasticsearch.CSV_HEADERS)
                    .setLineTag(CsvToElasticsearch.CSV_LINES)
                    .setUdfOutputTag(CsvToElasticsearch.PROCESSING_OUT)
                    .setUdfDeadletterTag(CsvToElasticsearch.PROCESSING_DEADLETTER_OUT)
                    .build());

    // Assert
    PAssert.that(readCsvOut.get(CsvToElasticsearch.PROCESSING_OUT))
        .satisfies(
            collection -> {
              FailsafeElement element = collection.iterator().next();
              assertThat(element.getOriginalPayload(), is(equalTo(record)));
              assertThat(element.getPayload(), is(equalTo(stringifiedJsonRecord)));
              return null;
            });

    //  Execute pipeline
    pipeline.run();
  }

  /** Tests the {@link CsvToElasticsearch} pipeline the headers of the Csv to parse it. */
  @Test
  public void testCsvToElasticsearchHeadersE2E() {

    final String header = "id,state,price";
    final String record = "007,CA,26.23";
    final String stringJsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":\"26.23\"}";

    final FailsafeElementCoder<String, String> coder =
        FailsafeElementCoder.of(
            NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    CsvToElasticsearch.CsvToElasticsearchOptions options =
        PipelineOptionsFactory.create().as(CsvToElasticsearch.CsvToElasticsearchOptions.class);

    options.setContainsHeaders(true);
    options.setInputFileSpec(HEADER_CSV_FILE_PATH);

    // Build pipeline with no headers.
    PCollectionTuple readCsvOut =
        pipeline
            .apply(
                "ReadCsv",
                CsvConverters.ReadCsv.newBuilder()
                    .setCsvFormat(options.getCsvFormat())
                    .setDelimiter(options.getDelimiter())
                    .setHasHeaders(options.getContainsHeaders())
                    .setInputFileSpec(options.getInputFileSpec())
                    .setHeaderTag(CsvToElasticsearch.CSV_HEADERS)
                    .setLineTag(CsvToElasticsearch.CSV_LINES)
                    .setFileEncoding(options.getCsvFileEncoding())
                    .build())
            .apply(
                "ConvertLine",
                CsvConverters.LineToFailsafeJson.newBuilder()
                    .setDelimiter(options.getDelimiter())
                    .setUdfFileSystemPath(options.getJavascriptTextTransformGcsPath())
                    .setUdfFunctionName(options.getJavascriptTextTransformFunctionName())
                    .setJsonSchemaPath(options.getJsonSchemaPath())
                    .setHeaderTag(CsvToElasticsearch.CSV_HEADERS)
                    .setLineTag(CsvToElasticsearch.CSV_LINES)
                    .setUdfOutputTag(CsvToElasticsearch.PROCESSING_OUT)
                    .setUdfDeadletterTag(CsvToElasticsearch.PROCESSING_DEADLETTER_OUT)
                    .build());

    // Assert
    PAssert.that(readCsvOut.get(CsvToElasticsearch.PROCESSING_OUT))
        .satisfies(
            collection -> {
              FailsafeElement element = collection.iterator().next();
              assertThat(element.getOriginalPayload(), is(equalTo(record)));
              assertThat(element.getPayload(), is(equalTo(stringJsonRecord)));
              return null;
            });

    //  Execute pipeline
    pipeline.run();
  }

  /** Tests the {@link CsvToElasticsearch} pipeline using a JSON schema to parse the Csv. */
  @Test
  public void testCsvToElasticsearchJsonSchemaE2E() {

    final String record = "007,CA,26.23";
    final String stringifiedJsonRecord = "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";

    final FailsafeElementCoder<String, String> coder =
        FailsafeElementCoder.of(
            NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    CsvToElasticsearch.CsvToElasticsearchOptions options =
        PipelineOptionsFactory.create().as(CsvToElasticsearch.CsvToElasticsearchOptions.class);

    options.setJsonSchemaPath(JSON_SCHEMA_FILE_PATH);
    options.setContainsHeaders(false);
    options.setInputFileSpec(NO_HEADER_CSV_FILE_PATH);

    // Build pipeline with no headers.
    PCollectionTuple readCsvOut =
        pipeline
            .apply(
                "ReadCsv",
                CsvConverters.ReadCsv.newBuilder()
                    .setCsvFormat(options.getCsvFormat())
                    .setDelimiter(options.getDelimiter())
                    .setHasHeaders(options.getContainsHeaders())
                    .setInputFileSpec(options.getInputFileSpec())
                    .setHeaderTag(CsvToElasticsearch.CSV_HEADERS)
                    .setLineTag(CsvToElasticsearch.CSV_LINES)
                    .setFileEncoding(options.getCsvFileEncoding())
                    .build())
            .apply(
                "ConvertLine",
                CsvConverters.LineToFailsafeJson.newBuilder()
                    .setDelimiter(options.getDelimiter())
                    .setUdfFileSystemPath(options.getJavascriptTextTransformGcsPath())
                    .setUdfFunctionName(options.getJavascriptTextTransformFunctionName())
                    .setJsonSchemaPath(options.getJsonSchemaPath())
                    .setHeaderTag(CsvToElasticsearch.CSV_HEADERS)
                    .setLineTag(CsvToElasticsearch.CSV_LINES)
                    .setUdfOutputTag(CsvToElasticsearch.PROCESSING_OUT)
                    .setUdfDeadletterTag(CsvToElasticsearch.PROCESSING_DEADLETTER_OUT)
                    .build());

    // Assert
    PAssert.that(readCsvOut.get(CsvToElasticsearch.PROCESSING_OUT))
        .satisfies(
            collection -> {
              FailsafeElement element = collection.iterator().next();
              assertThat(element.getOriginalPayload(), is(equalTo(record)));
              assertThat(element.getPayload(), is(equalTo(stringifiedJsonRecord)));
              return null;
            });

    //  Execute pipeline
    pipeline.run();
  }

  /**
   * Tests that the {@link ElasticsearchTransforms.WriteToElasticsearch} throws exception when only
   * one retry configuration value is provided.
   */
  @Test
  public void testWriteToElasticsearchBuilder() {
    exceptionRule.expect(IllegalArgumentException.class);

    CsvToElasticsearch.CsvToElasticsearchOptions options =
        PipelineOptionsFactory.create().as(CsvToElasticsearch.CsvToElasticsearchOptions.class);

    options.setNodeAddresses("http://my-node");
    options.setContainsHeaders(false);
    options.setInputFileSpec(NO_HEADER_CSV_FILE_PATH);
    options.setIndex("test");
    options.setDocumentType("_doc");
    options.setBatchSize(10000L);
    options.setBatchSizeBytes(500000L);
    options.setMaxRetryAttempts(5);
    options.setMaxRetryDuration(null);
    options.setUsePartialUpdate(false);
    pipeline
        .apply(Create.of("{}").withCoder(StringUtf8Coder.of()))
        .apply(
            "BuildWriteToElasticSearchObject",
           ElasticsearchTransforms.WriteToElasticsearch
                .newBuilder()
                .setOptions(options.as(ElasticsearchTransforms.WriteToElasticsearchOptions.class))
                .build());
    pipeline.run();
  }
}
