/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.elasticsearch.templates;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.elasticsearch.options.ElasticsearchWriteOptions;
import com.google.cloud.teleport.v2.elasticsearch.options.GCSToElasticsearchOptions;
import com.google.cloud.teleport.v2.elasticsearch.transforms.WriteToElasticsearch;
import com.google.cloud.teleport.v2.transforms.CsvConverters;
import com.google.cloud.teleport.v2.transforms.ErrorConverters.WriteStringMessageErrors;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link GCSToElasticsearch} pipeline exports data from one or more CSV files in Cloud Storage
 * to Elasticsearch.
 *
 * <p>Please refer to <b><a href=
 * "https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/master/v2/googlecloud-to-elasticsearch/docs/GCSToElasticsearch/README.md">
 * README.md</a></b> for further information.
 */
public class GCSToElasticsearch {

  /** The tag for the headers of the CSV if required. */
  static final TupleTag<String> CSV_HEADERS = new TupleTag<String>() {};

  /** The tag for the lines of the CSV. */
  static final TupleTag<String> CSV_LINES = new TupleTag<String>() {};

  /** The tag for the dead-letter output of the udf. */
  static final TupleTag<FailsafeElement<String, String>> PROCESSING_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  /** The tag for the main output for the UDF. */
  static final TupleTag<FailsafeElement<String, String>> PROCESSING_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(GCSToElasticsearch.class);

  /** String/String Coder for FailsafeElement. */
  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    GCSToElasticsearchOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(GCSToElasticsearchOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  private static PipelineResult run(GCSToElasticsearchOptions options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // Register the coder for pipeline
    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    // Throw error if containsHeaders is true and a schema or Udf is also set.
    if (options.getContainsHeaders()) {
      checkArgument(
          options.getJavascriptTextTransformGcsPath() == null
              && options.getJsonSchemaPath() == null,
          "Cannot parse file containing headers with UDF or Json schema.");
    }

    // Throw error if only one retry configuration parameter is set.
    checkArgument(
        (options.getMaxRetryAttempts() == null && options.getMaxRetryDuration() == null)
            || (options.getMaxRetryAttempts() != null && options.getMaxRetryDuration() != null),
        "To specify retry configuration both max attempts and max duration must be set.");

    /*
     * Steps: 1) Read records from CSV(s) via {@link CsvConverters.ReadCsv}.
     *        2) Convert lines to JSON strings via {@link CsvConverters.LineToFailsafeJson}.
     *        3a) Write JSON strings as documents to Elasticsearch via {@link ElasticsearchIO}.
     *        3b) Write elements that failed processing to {@link org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO}.
     */
    PCollectionTuple convertedCsvLines =
        pipeline
            /*
             * Step 1: Read CSV file(s) from Cloud Storage using {@link CsvConverters.ReadCsv}.
             */
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
            /*
             * Step 2: Convert lines to Elasticsearch document.
             */
            .apply(
                "ConvertLine",
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
    /*
     * Step 3a: Write elements that were successfully processed to Elasticsearch using {@link WriteToElasticsearch}.
     */
    convertedCsvLines
        .get(PROCESSING_OUT)
        .apply(
            "GetJsonDocuments",
            MapElements.into(TypeDescriptors.strings()).via(FailsafeElement::getPayload))
        .apply(
            "WriteToElasticsearch",
            WriteToElasticsearch.newBuilder()
                .setOptions(options.as(ElasticsearchWriteOptions.class))
                .build());

    /*
     * Step 3b: Write elements that failed processing to deadletter table via {@link BigQueryIO}.
     */
    convertedCsvLines
        .get(PROCESSING_DEADLETTER_OUT)
        .apply(
            "AddTimestamps",
            WithTimestamps.of((FailsafeElement<String, String> failures) -> new Instant()))
        .apply(
            "WriteFailedElementsToBigQuery",
            WriteStringMessageErrors.newBuilder()
                .setErrorRecordsTable(options.getDeadletterTable())
                .setErrorRecordsTableSchema(SchemaUtils.DEADLETTER_SCHEMA)
                .build());

    return pipeline.run();
  }
}
