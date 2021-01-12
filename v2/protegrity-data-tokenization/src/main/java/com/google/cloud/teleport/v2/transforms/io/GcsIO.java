/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.transforms.io;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.options.ProtegrityDataTokenizationOptions;
import com.google.cloud.teleport.v2.transforms.CsvConverters;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.utils.RowToCsv;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@link GcsIO} class to read/write data from/into Google Cloud Storage.
 */
public class GcsIO {

  /**
   * The tag for the headers of the CSV if required.
   */
  static final TupleTag<String> CSV_HEADERS = new TupleTag<String>() {
  };

  /**
   * The tag for the lines of the CSV.
   */
  static final TupleTag<String> CSV_LINES = new TupleTag<String>() {
  };

  /**
   * The tag for the dead-letter output.
   */
  static final TupleTag<FailsafeElement<String, String>> PROCESSING_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<String, String>>() {
      };

  /**
   * The tag for the main output.
   */
  static final TupleTag<FailsafeElement<String, String>> PROCESSING_OUT =
      new TupleTag<FailsafeElement<String, String>>() {
      };

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(GcsIO.class);

  public static final String DEAD_LETTER_PREFIX = "CSV_CONVERTOR";

  /**
   * String/String Coder for FailsafeElement.
   */
  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

  /**
   * Supported format to read from GCS.
   */
  public enum FORMAT {
    JSON,
    CSV
  }

  /**
   * Necessary {@link PipelineOptions} options for Pipelines that operate with JSON/CSV data in
   * GCS.
   */
  public interface GcsPipelineOptions extends PipelineOptions {

    @Description("GCS filepattern for files in bucket to read data from")
    String getInputGcsFilePattern();

    void setOutputGcsDirectory(String outputGcsDirectory);

    @Description("File format of input files. Supported formats: JSON, CSV")
    @Default.Enum("JSON")
    GcsIO.FORMAT getInputGcsFileFormat();

    void setInputGcsFilePattern(String inputGcsFilePattern);

    @Description("GCS directory in bucket to write data to")
    String getOutputGcsDirectory();

    void setInputGcsFileFormat(GcsIO.FORMAT inputGcsFileFormat);

    @Description("File format of output files. Supported formats: JSON, CSV")
    @Default.Enum("JSON")
    GcsIO.FORMAT getOutputGcsFileFormat();

    void setOutputGcsFileFormat(GcsIO.FORMAT outputGcsFileFormat);

    @Description(
        "The window duration in which data will be written. "
            + "Should be specified only for 'Pub/Sub -> GCS' case. Defaults to 30s. "
            + "Allowed formats are: "
            + "Ns (for seconds, example: 5s), "
            + "Nm (for minutes, example: 12m), "
            + "Nh (for hours, example: 2h).")
    @Default.String("30s")
    String getWindowDuration();

    void setWindowDuration(String windowDuration);

    // CSV parameters
    @Description("If file(s) contain headers")
    Boolean getCsvContainsHeaders();

    void setCsvContainsHeaders(Boolean csvContainsHeaders);

    @Description("Delimiting character in CSV. Default: use delimiter provided in csvFormat")
    @Default.InstanceFactory(CsvConverters.DelimiterFactory.class)
    String getCsvDelimiter();

    void setCsvDelimiter(String csvDelimiter);

    @Description(
        "Csv format according to Apache Commons CSV format. Default is: Apache Commons CSV"
            + " default\n"
            + "https://static.javadoc.io/org.apache.commons/commons-csv/1.7/org/apache/commons/csv/CSVFormat.html#DEFAULT\n"
            + "Must match format names exactly found at: "
            + "https://static.javadoc.io/org.apache.commons/commons-csv/1.7/org/apache/commons/csv/CSVFormat.Predefined.html")
    @Default.String("Default")
    String getCsvFormat();

    void setCsvFormat(String csvFormat);
  }

  private final ProtegrityDataTokenizationOptions options;

  public GcsIO(ProtegrityDataTokenizationOptions options) {
    this.options = options;
  }

  public PCollection<String> read(Pipeline pipeline, String schema) {
    if (options.getInputGcsFileFormat() == FORMAT.JSON) {
      return pipeline
          .apply("ReadJsonFromGCSFiles",
              TextIO.read().from(options.getInputGcsFilePattern()));
    } else if (options.getInputGcsFileFormat() == FORMAT.CSV) {
      PCollectionTuple jsons = pipeline
          /*
           * Step 1: Read CSV file(s) from Cloud Storage using {@link CsvConverters.ReadCsv}.
           */
          .apply(
              "ReadCsvFromGcsFiles",
              CsvConverters.ReadCsv.newBuilder()
                  .setCsvFormat(options.getCsvFormat())
                  .setDelimiter(options.getCsvDelimiter())
                  .setHasHeaders(options.getCsvContainsHeaders())
                  .setInputFileSpec(options.getInputGcsFilePattern())
                  .setHeaderTag(CSV_HEADERS)
                  .setLineTag(CSV_LINES)
                  .build())
          /*
           * Step 2: Convert lines to Json.
           */
          .apply(
              "LineToJson",
              CsvConverters.LineToFailsafeJson.newBuilder()
                  .setDelimiter(options.getCsvDelimiter())
                  .setJsonSchema(schema)
                  .setHeaderTag(CSV_HEADERS)
                  .setLineTag(CSV_LINES)
                  .setUdfOutputTag(PROCESSING_OUT)
                  .setUdfDeadletterTag(PROCESSING_DEADLETTER_OUT)
                  .build());

      if (options.getNonTokenizedDeadLetterGcsPath() != null) {
        /*
         * Step 3: Write jsons to dead-letter gcs that were successfully processed.
         */
        jsons.get(PROCESSING_DEADLETTER_OUT)
            .apply("WriteCsvConversionErrorsToGcs",
                ErrorConverters.WriteStringMessageErrorsAsCsv.newBuilder()
                    .setCsvDelimiter(options.getCsvDelimiter())
                    .setErrorWritePath(options.getNonTokenizedDeadLetterGcsPath())
                    .build());
      }

      /*
       * Step 4: Get jsons that were successfully processed.
       */
      return jsons.get(PROCESSING_OUT)
          .apply(
              "GetJson",
              MapElements.into(TypeDescriptors.strings()).via(FailsafeElement::getPayload));
    } else {
      throw new IllegalStateException(
          "No valid format for input data is provided. Please, choose JSON or CSV.");
    }
  }

  public PDone write(PCollection<Row> input, Schema schema) {
    if (options.getOutputGcsFileFormat() == FORMAT.JSON) {
      PCollection<String> jsons = input.apply("RowsToJSON", ToJson.of());

      if (jsons.isBounded() == IsBounded.BOUNDED) {
        return jsons
            .apply("WriteToGCS", TextIO.write().to(options.getOutputGcsDirectory()));
      } else {
        return jsons
            .apply("WriteToGCS", TextIO.write().withWindowedWrites().withNumShards(1)
                .to(options.getOutputGcsDirectory()));
      }
    } else if (options.getOutputGcsFileFormat() == FORMAT.CSV) {
      String header = String.join(options.getCsvDelimiter(), schema.getFieldNames());
      String csvDelimiter = options.getCsvDelimiter();
      PCollection<String> csvs = input
          .apply("ConvertToCSV", MapElements.into(TypeDescriptors.strings())
              .via((Row inputRow) ->
                  new RowToCsv(csvDelimiter).getCsvFromRow(inputRow))
          );

      if (csvs.isBounded() == IsBounded.BOUNDED) {
        return csvs
            .apply("WriteToGCS",
                TextIO.write().to(options.getOutputGcsDirectory()).withHeader(header));
      } else {
        return csvs
            .apply("WriteToGCS",
                TextIO.write().withWindowedWrites().withNumShards(1)
                    .to(options.getOutputGcsDirectory()).withHeader(header));
      }

    } else {
      throw new IllegalStateException(
          "No valid format for output data is provided. Please, choose JSON or CSV.");
    }
  }
}
