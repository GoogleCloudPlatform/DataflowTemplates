/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.templates.common;

import static org.junit.Assert.assertEquals;

import com.google.common.io.Resources;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;

/** Test cases for the {@link CsvConverters} class. */
public class CsvConvertersTest {

  private static final TupleTag<Iterable<String>> CSV_HEADERS = new TupleTag<Iterable<String>>() {};

  private static final TupleTag<Iterable<String>> CSV_LINES = new TupleTag<Iterable<String>>() {};
  private static final String CSV_RESOURCES_DIR = "CsvConvertersTest/";
  private static final String HEADER_CSV_FILE_PATH =
      Resources.getResource(CSV_RESOURCES_DIR + "with_headers.csv").getPath();
  private static final String NO_HEADER_CSV_FILE_PATH =
      Resources.getResource(CSV_RESOURCES_DIR + "no_header.csv").getPath();
  private ValueProvider<Boolean> containsHeaders = StaticValueProvider.of(true);
  private ValueProvider<Boolean> containsNoHeaders = StaticValueProvider.of(false);
  private ValueProvider<String> delimiter = StaticValueProvider.of(",");
  private ValueProvider<String> csvFormat = StaticValueProvider.of("Default");
  private ValueProvider<String> csvFileEncoding = StaticValueProvider.of("UTF-8");
  private ValueProvider<String> inputFileSpecWithHeaders =
      StaticValueProvider.of(HEADER_CSV_FILE_PATH);
  private ValueProvider<String> inputFileSpecWithoutHeaders =
      StaticValueProvider.of(NO_HEADER_CSV_FILE_PATH);

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Tests {@link CsvConverters.ReadCsv} reads a Csv with commas in quotes correctly. */
  @Test
  public void testReadWithHeaders() {

    CsvConverters.CsvPipelineOptions options =
        PipelineOptionsFactory.create().as(CsvConverters.CsvPipelineOptions.class);

    options.setContainsHeaders(containsHeaders);
    options.setDelimiter(delimiter);
    options.setCsvFormat(csvFormat);
    options.setCsvFileEncoding(csvFileEncoding);

    // Build pipeline with headers.
    PCollectionTuple readCsvHeadersOut =
        pipeline.apply(
            "TestReadWithCommasInQuotes",
            CsvConverters.ReadCsv.newBuilder()
                .setCsvFormat(options.getCsvFormat())
                .setDelimiter(options.getDelimiter())
                .setHasHeaders(options.getContainsHeaders())
                .setInputFileSpec(inputFileSpecWithHeaders)
                .setHeaderTag(CSV_HEADERS)
                .setLineTag(CSV_LINES)
                .setFileEncoding(options.getCsvFileEncoding())
                .build());

    List<String> expectedLines = Arrays.asList("abc", "def", "123,456", "\"quoted_string\"");

    PAssert.that(readCsvHeadersOut.get(CSV_LINES))
        .satisfies(
            collection -> {
              Iterable<String> result = collection.iterator().next();
              assertEquals(result, expectedLines);
              return null;
            });

    List<String> expectedHeaders = Arrays.asList("a", "b", "c1,c2", "\"d\"");
    PAssert.that(readCsvHeadersOut.get(CSV_HEADERS))
        .satisfies(
            collection -> {
              Iterable<String> result = collection.iterator().next();
              assertEquals(result, expectedHeaders);
              return null;
            });

    //  Execute pipeline
    pipeline.run();
  }

  @Test
  public void testReadWithoutHeaders() {

    CsvConverters.CsvPipelineOptions options =
        PipelineOptionsFactory.create().as(CsvConverters.CsvPipelineOptions.class);

    options.setContainsHeaders(containsNoHeaders);
    options.setDelimiter(delimiter);
    options.setCsvFormat(csvFormat);
    options.setCsvFileEncoding(csvFileEncoding);

    // Build pipeline with headers.
    PCollectionTuple readCsvHeadersOut =
        pipeline.apply(
            "TestReadWithCommasInQuotes",
            CsvConverters.ReadCsv.newBuilder()
                .setCsvFormat(options.getCsvFormat())
                .setDelimiter(options.getDelimiter())
                .setHasHeaders(options.getContainsHeaders())
                .setInputFileSpec(inputFileSpecWithoutHeaders)
                .setHeaderTag(CSV_HEADERS)
                .setLineTag(CSV_LINES)
                .setFileEncoding(options.getCsvFileEncoding())
                .build());

    List<String> expectedLines = Arrays.asList("abc", "def", "123,456", "\"quoted_string\"");

    PAssert.that(readCsvHeadersOut.get(CSV_LINES))
        .satisfies(
            collection -> {
              Iterable<String> result = collection.iterator().next();
              assertEquals(result, expectedLines);
              return null;
            });

    //  Execute pipeline
    pipeline.run();
  }
}
