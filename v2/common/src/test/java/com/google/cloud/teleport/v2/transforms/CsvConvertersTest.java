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
package com.google.cloud.teleport.v2.transforms;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.io.Resources;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.csv.CSVFormat;
import org.junit.Rule;
import org.junit.Test;

/** Test cases for the {@link CsvConverters} class. */
public class CsvConvertersTest {

  private static final TupleTag<String> CSV_HEADERS = new TupleTag<String>() {};

  private static final TupleTag<String> CSV_LINES = new TupleTag<String>() {};

  private static final TupleTag<FailsafeElement<String, String>> PROCESSING_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  private static final TupleTag<FailsafeElement<String, String>> PROCESSING_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

  private static final String CSV_RESOURCES_DIR = "CsvConvertersTest/";

  private static final String NO_HEADER_CSV_FILE_PATH =
      Resources.getResource(CSV_RESOURCES_DIR + "no_header.csv").getPath();

  private static final String HEADER_CSV_FILE_PATH =
      Resources.getResource(CSV_RESOURCES_DIR + "with_headers.csv").getPath();

  private static final String TEST_JSON_SCHEMA__PATH =
      Resources.getResource(CSV_RESOURCES_DIR + "testSchema.json").getPath();

  private static final String TEST_AVRO_SCHEMA_PATH =
      Resources.getResource(CSV_RESOURCES_DIR + "testAvroSchema.json").getPath();

  private static final String HEADER_STRING = "id,state,price";

  private static final String RECORD_STRING = "007,CA,26.23";

  private static final String JSON_STRING_RECORD =
      "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";

  private static final String JSON_STRINGS_RECORD =
      "{\"id\":\"007\",\"state\":\"CA\",\"price\":\"26.23\"}";

  private static final String BAD_JSON_STRING_RECORD = "this,is,a,bad,record";

  private static final String TRANSFORM_FILE_PATH =
      Resources.getResource(CSV_RESOURCES_DIR + "elasticUdf.js").getPath();

  private static final String SCRIPT_PARSE_EXCEPTION_FILE_PATH =
      Resources.getResource(CSV_RESOURCES_DIR + "elasticUdfBad.js").getPath();

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Tests {@link CsvConverters.ReadCsv} reads a Csv with no headers correctly. */
  @Test
  public void testReadNoHeadersCsv() {

    CsvConverters.CsvPipelineOptions options =
        PipelineOptionsFactory.create().as(CsvConverters.CsvPipelineOptions.class);

    options.setContainsHeaders(false);
    options.setDelimiter(",");
    options.setCsvFormat("Default");
    options.setInputFileSpec(NO_HEADER_CSV_FILE_PATH);

    // Build pipeline with no headers.
    PCollectionTuple readCsvOut =
        pipeline.apply(
            "TestReadCsvNoHeaders",
            CsvConverters.ReadCsv.newBuilder()
                .setCsvFormat(options.getCsvFormat())
                .setDelimiter(options.getDelimiter())
                .setHasHeaders(options.getContainsHeaders())
                .setInputFileSpec(options.getInputFileSpec())
                .setHeaderTag(CSV_HEADERS)
                .setLineTag(CSV_LINES)
                .build());

    PAssert.that(readCsvOut.get(CSV_LINES))
        .satisfies(
            collection -> {
              String result = collection.iterator().next();
              assertThat(result, is(equalTo(RECORD_STRING)));
              return null;
            });

    //  Execute pipeline
    pipeline.run();
  }

  /** Tests {@link CsvConverters.ReadCsv} reads a Csv with headers correctly. */
  @Test
  public void testReadWithHeadersCsv() {

    CsvConverters.CsvPipelineOptions options =
        PipelineOptionsFactory.create().as(CsvConverters.CsvPipelineOptions.class);

    options.setContainsHeaders(true);
    options.setDelimiter(",");
    options.setCsvFormat("Default");
    options.setInputFileSpec(HEADER_CSV_FILE_PATH);

    // Build pipeline with headers.
    PCollectionTuple readCsvHeadersOut =
        pipeline.apply(
            "TestReadCsvHeaders",
            CsvConverters.ReadCsv.newBuilder()
                .setCsvFormat(options.getCsvFormat())
                .setDelimiter(options.getDelimiter())
                .setHasHeaders(options.getContainsHeaders())
                .setInputFileSpec(options.getInputFileSpec())
                .setHeaderTag(CSV_HEADERS)
                .setLineTag(CSV_LINES)
                .build());

    PAssert.that(readCsvHeadersOut.get(CSV_LINES))
        .satisfies(
            collection -> {
              Iterator<String> iterator = collection.iterator();
              String result = iterator.next();
              assertThat(result, is(equalTo(RECORD_STRING)));
              return null;
            });

    PAssert.that(readCsvHeadersOut.get(CSV_HEADERS))
        .satisfies(
            collection -> {
              String result = collection.iterator().next();
              assertThat(result, is(equalTo(HEADER_STRING)));
              return null;
            });

    //  Execute pipeline
    pipeline.run();
  }

  /**
   * Tests if {@link CsvConverters.ReadCsv} throws an exception if no input Csv file is provided.
   */
  @Test(expected = NullPointerException.class)
  public void testReadCsvWithoutInputFile() {
    pipeline.apply(
        "TestReadCsvWithoutInputFile",
        CsvConverters.ReadCsv.newBuilder()
            .setHasHeaders(false)
            .setInputFileSpec(null)
            .setHeaderTag(CSV_HEADERS)
            .setLineTag(CSV_LINES)
            .setCsvFormat("Default")
            .setDelimiter(",")
            .build());

    pipeline.run();
  }

  /**
   * Tests {@link CsvConverters.ReadCsv} throws an exception if no header information is provided.
   */
  @Test(expected = NullPointerException.class)
  public void testReadCsvWithoutHeaderInformation() {
    pipeline.apply(
        "TestReadCsvWithoutHeaderInformation",
        CsvConverters.ReadCsv.newBuilder()
            .setHasHeaders(null)
            .setInputFileSpec(NO_HEADER_CSV_FILE_PATH)
            .setHeaderTag(CSV_HEADERS)
            .setLineTag(CSV_LINES)
            .setCsvFormat("Default")
            .setDelimiter(",")
            .build());

    pipeline.run();
  }

  /**
   * Tests {@link CsvConverters.LineToFailsafeJson} converts a line to a {@link FailsafeElement}
   * correctly using a JSON schema.
   */
  @Test
  public void testLineToFailsafeJsonNoHeadersJsonSchema() {

    FailsafeElementCoder<String, String> coder = FAILSAFE_ELEMENT_CODER;

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    PCollection<String> lines =
        pipeline.apply(Create.of(RECORD_STRING).withCoder(StringUtf8Coder.of()));

    PCollectionTuple linesTuple = PCollectionTuple.of(CSV_LINES, lines);

    PCollectionTuple failsafe =
        linesTuple.apply(
            "TestLineToFailsafeJson",
            CsvConverters.LineToFailsafeJson.newBuilder()
                .setDelimiter(",")
                .setUdfFileSystemPath(null)
                .setUdfFunctionName(null)
                .setJsonSchemaPath(TEST_JSON_SCHEMA__PATH)
                .setHeaderTag(CSV_HEADERS)
                .setLineTag(CSV_LINES)
                .setUdfOutputTag(PROCESSING_OUT)
                .setUdfDeadletterTag(PROCESSING_DEADLETTER_OUT)
                .build());

    PAssert.that(failsafe.get(PROCESSING_OUT))
        .satisfies(
            collection -> {
              FailsafeElement<String, String> result = collection.iterator().next();
              assertThat(result.getPayload(), is(equalTo(JSON_STRING_RECORD)));
              return null;
            });

    pipeline.run();
  }

  /**
   * Tests {@link CsvConverters.LineToFailsafeJson} converts a line to a {@link FailsafeElement}
   * correctly using a Javascript Udf. Udf processing is handled by {@link
   * JavascriptTextTransformer}.
   */
  @Test
  public void testLineToFailsafeJsonNoHeadersUdf() {
    FailsafeElementCoder<String, String> coder = FAILSAFE_ELEMENT_CODER;

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    PCollection<String> lines =
        pipeline.apply(Create.of(RECORD_STRING).withCoder(StringUtf8Coder.of()));

    PCollectionTuple linesTuple = PCollectionTuple.of(CSV_LINES, lines);

    CsvConverters.CsvPipelineOptions options =
        PipelineOptionsFactory.create().as(CsvConverters.CsvPipelineOptions.class);

    options.setDelimiter(",");
    options.setJavascriptTextTransformGcsPath(TRANSFORM_FILE_PATH);
    options.setJavascriptTextTransformFunctionName("transform");

    PCollectionTuple failsafe =
        linesTuple.apply(
            "TestLineToFailsafeJsonNoHeadersUdf",
            CsvConverters.LineToFailsafeJson.newBuilder()
                .setDelimiter(options.getDelimiter())
                .setUdfFileSystemPath(options.getJavascriptTextTransformGcsPath())
                .setUdfFunctionName(options.getJavascriptTextTransformFunctionName())
                .setJsonSchemaPath(options.getJsonSchemaPath())
                .setHeaderTag(CSV_HEADERS)
                .setLineTag(CSV_LINES)
                .setUdfOutputTag(PROCESSING_OUT)
                .setUdfDeadletterTag(PROCESSING_DEADLETTER_OUT)
                .build());

    PAssert.that(failsafe.get(PROCESSING_OUT))
        .satisfies(
            collection -> {
              FailsafeElement<String, String> result = collection.iterator().next();
              assertThat(result.getPayload(), is(equalTo(JSON_STRING_RECORD)));
              return null;
            });

    PAssert.that(failsafe.get(PROCESSING_DEADLETTER_OUT)).empty();

    pipeline.run();
  }

  /**
   * Tests {@link CsvConverters.LineToFailsafeJson} converts a line to a {@link FailsafeElement}
   * correctly using a Javascript Udf. Udf processing is handled by {@link
   * JavascriptTextTransformer}. Should output record to deadletter table tag.
   */
  @Test
  public void testLineToFailsafeJsonNoHeadersUdfDeadletter() {
    FailsafeElementCoder<String, String> coder = FAILSAFE_ELEMENT_CODER;

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    PCollection<String> lines =
        pipeline.apply(Create.of(BAD_JSON_STRING_RECORD).withCoder(StringUtf8Coder.of()));

    PCollectionTuple linesTuple = PCollectionTuple.of(CSV_LINES, lines);

    CsvConverters.CsvPipelineOptions options =
        PipelineOptionsFactory.create().as(CsvConverters.CsvPipelineOptions.class);

    options.setDelimiter(",");
    options.setJavascriptTextTransformGcsPath(SCRIPT_PARSE_EXCEPTION_FILE_PATH);
    options.setJavascriptTextTransformFunctionName("transform");

    PCollectionTuple failsafe =
        linesTuple.apply(
            "TestLineToFailsafeJsonNoHeadersUdfBad",
            CsvConverters.LineToFailsafeJson.newBuilder()
                .setDelimiter(options.getDelimiter())
                .setUdfFileSystemPath(options.getJavascriptTextTransformGcsPath())
                .setUdfFunctionName(options.getJavascriptTextTransformFunctionName())
                .setJsonSchemaPath(options.getJsonSchemaPath())
                .setJsonSchemaPath(null)
                .setHeaderTag(CSV_HEADERS)
                .setLineTag(CSV_LINES)
                .setUdfOutputTag(PROCESSING_OUT)
                .setUdfDeadletterTag(PROCESSING_DEADLETTER_OUT)
                .build());

    PAssert.that(failsafe.get(PROCESSING_OUT)).empty();
    PAssert.that(failsafe.get(PROCESSING_DEADLETTER_OUT))
        .satisfies(
            collection -> {
              FailsafeElement result = collection.iterator().next();
              assertThat(result.getPayload(), is(equalTo(BAD_JSON_STRING_RECORD)));
              return null;
            });

    pipeline.run();
  }

  /**
   * Tests {@link CsvConverters.LineToFailsafeJson} converts a line to a {@link FailsafeElement}
   * correctly using the headers of the Csv file.
   */
  @Test
  public void testLineToFailsafeJsonHeaders() {
    FailsafeElementCoder<String, String> coder = FAILSAFE_ELEMENT_CODER;

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    PCollection<String> lines =
        pipeline.apply("Create lines", Create.of(RECORD_STRING).withCoder(StringUtf8Coder.of()));

    PCollection<String> header =
        pipeline.apply("Create headers", Create.of(HEADER_STRING).withCoder(StringUtf8Coder.of()));

    PCollectionTuple linesTuple = PCollectionTuple.of(CSV_LINES, lines).and(CSV_HEADERS, header);

    CsvConverters.CsvPipelineOptions options =
        PipelineOptionsFactory.create().as(CsvConverters.CsvPipelineOptions.class);

    options.setDelimiter(",");
    options.setJavascriptTextTransformFunctionName(null);
    options.setJavascriptTextTransformGcsPath(null);
    options.setJsonSchemaPath(null);

    PCollectionTuple failsafe =
        linesTuple.apply(
            "TestLineToFailsafeJsonHeaders",
            CsvConverters.LineToFailsafeJson.newBuilder()
                .setDelimiter(options.getDelimiter())
                .setUdfFileSystemPath(options.getJavascriptTextTransformGcsPath())
                .setUdfFunctionName(options.getJavascriptTextTransformFunctionName())
                .setJsonSchemaPath(options.getJsonSchemaPath())
                .setHeaderTag(CSV_HEADERS)
                .setLineTag(CSV_LINES)
                .setUdfOutputTag(PROCESSING_OUT)
                .setUdfDeadletterTag(PROCESSING_DEADLETTER_OUT)
                .build());

    PAssert.that(failsafe.get(PROCESSING_OUT))
        .satisfies(
            collection -> {
              FailsafeElement<String, String> result = collection.iterator().next();
              assertThat(result.getPayload(), is(equalTo(JSON_STRINGS_RECORD)));
              return null;
            });

    PAssert.that(failsafe.get(PROCESSING_DEADLETTER_OUT)).empty();

    pipeline.run();
  }

  /**
   * Tests {@link CsvConverters#getCsvFormat(String, String)} throws error if invalid {@link
   * CSVFormat} is specified.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testCsvFormatInvalid() {
    CSVFormat csvFormat = CsvConverters.getCsvFormat("random", null);
  }

  /**
   * Tests {@link CsvConverters#getCsvFormat(String, String)} creates a proper {@link CSVFormat}.
   */
  @Test
  public void testCsvFormatWithDelimiter() {
    String delim = ";";
    CSVFormat csvFormat = CsvConverters.getCsvFormat("Excel", delim);
    assertEquals(delim, String.valueOf(csvFormat.getDelimiter()));
  }

  /** Tests {@link CsvConverters#buildJsonString(List, List, String)} creates proper string. */
  @Test
  public void testBuildJsonStringHeaders() throws Exception {
    List<String> headers = Arrays.asList(HEADER_STRING.split(","));
    List<String> record = Arrays.asList(RECORD_STRING.split(","));
    String jsonSchema = null;

    String json = CsvConverters.buildJsonString(headers, record, jsonSchema);
    assertEquals(JSON_STRINGS_RECORD, json);
  }

  /** Tests that if different headers are found an exception is thrown. */
  @Test(expected = RuntimeException.class)
  public void testDifferentHeaders() {

    FailsafeElementCoder<String, String> coder = FAILSAFE_ELEMENT_CODER;

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    PCollection<String> headers =
        pipeline.apply("CreateInput", Create.of(HEADER_STRING, "wrong,header,thing\n"));
    PCollection<String> lines = pipeline.apply("Create lines", Create.of(RECORD_STRING));

    PCollectionTuple readCsvHeadersOut =
        PCollectionTuple.of(CSV_HEADERS, headers).and(CSV_LINES, lines);

    PCollectionTuple test =
        readCsvHeadersOut.apply(
            "TestDifferentHeaders",
            CsvConverters.LineToFailsafeJson.newBuilder()
                .setDelimiter(",")
                .setUdfFileSystemPath(null)
                .setUdfFunctionName(null)
                .setJsonSchemaPath(null)
                .setHeaderTag(CSV_HEADERS)
                .setLineTag(CSV_LINES)
                .setUdfDeadletterTag(PROCESSING_DEADLETTER_OUT)
                .setUdfOutputTag(PROCESSING_OUT)
                .build());

    pipeline.run();
  }

  @Test
  public void testNullDelimiterNoHeaders() {

    CsvConverters.CsvPipelineOptions options =
        PipelineOptionsFactory.create().as(CsvConverters.CsvPipelineOptions.class);

    options.setContainsHeaders(false);
    options.setDelimiter(null);
    options.setCsvFormat("Default");
    options.setInputFileSpec(NO_HEADER_CSV_FILE_PATH);

    // Build pipeline with no headers.
    PCollectionTuple readCsvOut =
        pipeline.apply(
            "TestReadCsvNoHeaders",
            CsvConverters.ReadCsv.newBuilder()
                .setCsvFormat(options.getCsvFormat())
                .setDelimiter(options.getDelimiter())
                .setHasHeaders(options.getContainsHeaders())
                .setInputFileSpec(options.getInputFileSpec())
                .setHeaderTag(CSV_HEADERS)
                .setLineTag(CSV_LINES)
                .build());

    PAssert.that(readCsvOut.get(CSV_LINES))
        .satisfies(
            collection -> {
              Iterator<String> iterator = collection.iterator();
              String result = iterator.next();
              assertThat(result, is(equalTo(RECORD_STRING)));
              return null;
            });

    pipeline.run();
  }

  /** Tests if {@link CsvConverters.StringToGenericRecordFn} creates a proper GenericRecord. */
  @Test
  public void testStringToGenericRecord() {
    Schema schema = AvroConverters.getAvroSchema(TEST_AVRO_SCHEMA_PATH);

    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put("id", "007");
    genericRecord.put("state", "CA");
    genericRecord.put("price", 26.23);

    PCollection<GenericRecord> pCollection =
        pipeline
            .apply(
                "ReadCsvFile",
                CsvConverters.ReadCsv.newBuilder()
                    .setHasHeaders(false)
                    .setInputFileSpec(NO_HEADER_CSV_FILE_PATH)
                    .setHeaderTag(CSV_HEADERS)
                    .setLineTag(CSV_LINES)
                    .setCsvFormat("Default")
                    .setDelimiter(",")
                    .build())
            .get(CSV_LINES)
            .apply(
                "ConvertStringToGenericRecord",
                ParDo.of(new CsvConverters.StringToGenericRecordFn(TEST_AVRO_SCHEMA_PATH, ",")))
            .setCoder(AvroCoder.of(GenericRecord.class, schema));

    PAssert.that(pCollection).containsInAnyOrder(genericRecord);

    pipeline.run();
  }

  /**
   * Tests {@link CsvConverters.StringToGenericRecordFn} throws an exception if incorrect header
   * information is provided. (for e.g. if a Csv file containing headers is passed and hasHeaders is
   * set to false.)
   */
  @Test(expected = RuntimeException.class)
  public void testIncorrectHeaderInformation() {
    Schema schema = AvroConverters.getAvroSchema(TEST_AVRO_SCHEMA_PATH);

    pipeline
        .apply(
            "TestIncorrectHeaderInformation",
            CsvConverters.ReadCsv.newBuilder()
                .setInputFileSpec(HEADER_CSV_FILE_PATH)
                .setHasHeaders(false)
                .setHeaderTag(CSV_HEADERS)
                .setLineTag(CSV_LINES)
                .setCsvFormat("Default")
                .setDelimiter(",")
                .build())
        .get(CSV_LINES)
        .apply(
            "ConvertStringToGenericRecord",
            ParDo.of(new CsvConverters.StringToGenericRecordFn(TEST_AVRO_SCHEMA_PATH, ",")))
        .setCoder(AvroCoder.of(GenericRecord.class, schema));

    pipeline.run();
  }
}
