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
import com.google.cloud.teleport.v2.transforms.JsonToBeamRow;
import com.google.cloud.teleport.v2.transforms.SerializableFunctions;
import com.google.cloud.teleport.v2.utils.RowToCsv;
import com.google.cloud.teleport.v2.utils.SchemasUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
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
    CSV,
    AVRO
  }

  /**
   * Necessary {@link PipelineOptions} options for Pipelines that operate with JSON/CSV data in
   * GCS.
   */
  public interface GcsPipelineOptions extends PipelineOptions {


    @Description("File format of input files. Supported formats: JSON, CSV")
    @Default.Enum("JSON")
    GcsIO.FORMAT getInputGcsFileFormat();

    void setInputGcsFileFormat(GcsIO.FORMAT inputGcsFileFormat);

    @Description("File format of output files. Supported formats: JSON, CSV")
    @Default.Enum("JSON")
    GcsIO.FORMAT getOutputGcsFileFormat();

    void setOutputGcsFileFormat(GcsIO.FORMAT outputGcsFileFormat);


  }

  private final ProtegrityDataTokenizationOptions options;

  public GcsIO(ProtegrityDataTokenizationOptions options) {
    this.options = options;
  }

  private PCollection<String> readJson(Pipeline pipeline) {
    return pipeline
        .apply("ReadJsonFromGCSFiles",
            TextIO.read().from(options.getInputGcsFilePattern()));
  }

  private PCollection<Row> readAvro(Pipeline pipeline, Schema beamSchema) {
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);

    return pipeline
        .apply(
            "ReadAvroFiles",
            AvroIO.readGenericRecords(avroSchema).from(options.getInputGcsFilePattern()))
        .apply(
            "GenericRecordToRow",
            MapElements.into(TypeDescriptor.of(Row.class))
                .via(AvroUtils.getGenericRecordToRowFunction(beamSchema)))
        .setCoder(RowCoder.of(beamSchema));
  }

  private PCollectionTuple readCsv(Pipeline pipeline) {
    return pipeline
        .apply(
            "ReadCsvFromGcsFiles",
            CsvConverters.ReadCsv.newBuilder()
                .setCsvFormat(options.getCsvFormat())
                .setDelimiter(options.getCsvDelimiter())
                .setHasHeaders(options.getCsvContainsHeaders())
                .setInputFileSpec(options.getInputGcsFilePattern())
                .setHeaderTag(CSV_HEADERS)
                .setLineTag(CSV_LINES)
                .build());
  }

  private PCollectionTuple csvLineToJson(PCollectionTuple csvLines, String jsonBeamSchema) {
    return csvLines.apply(
        "LineToJson",
        CsvConverters.LineToFailsafeJson.newBuilder()
            .setDelimiter(options.getCsvDelimiter())
            .setJsonSchema(jsonBeamSchema)
            .setHeaderTag(CSV_HEADERS)
            .setLineTag(CSV_LINES)
            .setUdfOutputTag(PROCESSING_OUT)
            .setUdfDeadletterTag(PROCESSING_DEADLETTER_OUT)
            .build());
  }

  private POutput writeJson(PCollection<Row> outputCollection) {
    PCollection<String> jsons = outputCollection.apply("RowsToJSON", ToJson.of());

    if (jsons.isBounded() == IsBounded.BOUNDED) {
      return jsons
          .apply(
              "WriteToGCS",
              TextIO.write().to(options.getOutputGcsDirectory()));
    } else {
      return jsons
          .apply(
              "WriteToGCS",
              TextIO.write().withWindowedWrites().withNumShards(1)
                  .to(options.getOutputGcsDirectory()));
    }
  }

  private POutput writeCsv(PCollection<Row> outputCollection, Iterable<String> fieldNames) {
    String header = String.join(options.getCsvDelimiter(), fieldNames);
    String csvDelimiter = options.getCsvDelimiter();
    PCollection<String> csvs = outputCollection
        .apply(
            "ConvertToCSV",
            MapElements.into(TypeDescriptors.strings())
                .via((Row inputRow) ->
                    new RowToCsv(csvDelimiter).getCsvFromRow(inputRow))
        );

    if (csvs.isBounded() == IsBounded.BOUNDED) {
      return csvs
          .apply(
              "WriteToGCS",
              TextIO.write().to(options.getOutputGcsDirectory()).withHeader(header));
    } else {
      return csvs
          .apply(
              "WriteToGCS",
              TextIO.write().withWindowedWrites().withNumShards(1)
                  .to(options.getOutputGcsDirectory()).withHeader(header));
    }
  }

  private POutput writeAvro(PCollection<Row> outputCollection, Schema schema) {
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
    return outputCollection
        .apply(
            "RowToGenericRecord",
            MapElements.into(TypeDescriptor.of(GenericRecord.class))
                .via(AvroUtils.getRowToGenericRecordFunction(avroSchema)))
        .setCoder(AvroCoder.of(GenericRecord.class, avroSchema))
        .apply(
            "WriteToAvro",
            AvroIO.writeGenericRecords(avroSchema)
                .to(options.getOutputGcsDirectory())
                .withSuffix(".avro"));
  }

  public PCollection<Row> read(Pipeline pipeline, SchemasUtils schema) {
    switch (options.getInputGcsFileFormat()) {
      case CSV:
        /*
         * Step 1: Read CSV file(s) from Cloud Storage using {@link CsvConverters.ReadCsv}.
         */
        PCollectionTuple csvLines = readCsv(pipeline);
        /*
         * Step 2: Convert lines to Json.
         */
        PCollectionTuple jsons = csvLineToJson(csvLines, schema.getJsonBeamSchema());

        if (options.getNonTokenizedDeadLetterGcsPath() != null) {
          /*
           * Step 3: Write jsons to dead-letter gcs that were not successfully processed.
           */
          jsons.get(PROCESSING_DEADLETTER_OUT)
              .apply(
                  "WriteCsvConversionErrorsToGcs",
                  ErrorConverters.WriteErrorsToTextIO.<String, String>newBuilder()
                      .setErrorWritePath(options.getNonTokenizedDeadLetterGcsPath())
                      .setTranslateFunction(SerializableFunctions.getCsvErrorConverter())
                      .build());
        }
        /*
         * Step 4: Get jsons that were successfully processed.
         */
        return jsons.get(PROCESSING_OUT)
            .apply(
                "GetJson",
                MapElements.into(TypeDescriptors.strings()).via(FailsafeElement::getPayload))
            .apply(
                "TransformToBeamRow",
                new JsonToBeamRow(options.getNonTokenizedDeadLetterGcsPath(), schema));
      case JSON:
        return readJson(pipeline)
            .apply(
                "TransformToBeamRow",
                new JsonToBeamRow(options.getNonTokenizedDeadLetterGcsPath(), schema));
      case AVRO:
        return readAvro(pipeline, schema.getBeamSchema());
      default:
        throw new IllegalStateException(
            "No valid format for input data is provided. Please, choose JSON, CSV or AVRO.");
    }
  }

  public POutput write(PCollection<Row> input, Schema schema) {
    switch (options.getOutputGcsFileFormat()) {
      case JSON:
        return writeJson(input);
      case AVRO:
        return writeAvro(input, schema);
      case CSV:
        return writeCsv(input, schema.getFieldNames());
      default:
        throw new IllegalStateException(
            "No valid format for output data is provided. Please, choose JSON, CSV or AVRO.");
    }
  }
}
