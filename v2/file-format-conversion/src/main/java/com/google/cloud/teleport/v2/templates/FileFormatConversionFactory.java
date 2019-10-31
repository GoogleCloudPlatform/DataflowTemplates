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
package com.google.cloud.teleport.v2.templates;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.templates.FileFormatConversion.FileFormatConversionOptions;
import com.google.cloud.teleport.v2.templates.FileFormatConversion.ValidFileFormats;
import com.google.cloud.teleport.v2.transforms.AvroConverters;
import com.google.cloud.teleport.v2.transforms.CsvConverters;
import com.google.cloud.teleport.v2.transforms.ParquetConverters;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory class for the {@link FileFormatConversion} class. */
public class FileFormatConversionFactory {
  /** The tag for the headers of the CSV if required. */
  static final TupleTag<String> CSV_HEADERS = new TupleTag<String>() {};

  /** The tag for the lines of the CSV. */
  static final TupleTag<String> CSV_LINES = new TupleTag<String>() {};

  /** Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(FileFormatConversionFactory.class);

  /**
   * The {@link FileFormat} class is a {@link PTransform} that reads an input file, converts it to a
   * desired format and returns a {@link PDone}.
   */
  @AutoValue
  public abstract static class FileFormat extends PTransform<PBegin, PDone> {

    public static Builder newBuilder() {
      return new AutoValue_FileFormatConversionFactory_FileFormat.Builder();
    }

    public abstract FileFormatConversionOptions options();

    @Override
    public PDone expand(PBegin input) {
      String inputFileFormat = options().getInputFileFormat().toUpperCase();
      String outputFileFormat = options().getOutputFileFormat().toUpperCase();

      PCollection<GenericRecord> inputFile;

      switch (ValidFileFormats.valueOf(inputFileFormat)) {
        case CSV:
          checkNotNull(
              options().getSchema(),
              "Schema needs to be provided to convert a Csv file to a GenericRecord.");

          inputFile =
              input
                  .apply(
                      "ReadCsvFile",
                      CsvConverters.ReadCsv.newBuilder()
                          .setInputFileSpec(options().getInputFileSpec())
                          .setHasHeaders(options().getContainsHeaders())
                          .setHeaderTag(CSV_HEADERS)
                          .setLineTag(CSV_LINES)
                          .setCsvFormat(options().getCsvFormat())
                          .setDelimiter(options().getDelimiter())
                          .build())
                  .get(CSV_LINES)
                  .apply(
                      "ConvertToGenericRecord",
                      ParDo.of(
                          new CsvConverters.StringToGenericRecordFn(
                              options().getSchema(), options().getDelimiter())))
                  .setCoder(
                      AvroCoder.of(
                          GenericRecord.class,
                          AvroConverters.getAvroSchema(options().getSchema())));
          break;

        case AVRO:
          inputFile =
              input.apply(
                  "ReadAvroFile",
                  AvroConverters.ReadAvroFile.newBuilder()
                      .withInputFileSpec(options().getInputFileSpec())
                      .withSchema(options().getSchema())
                      .build());
          break;

        case PARQUET:
          inputFile =
              input.apply(
                  "ReadParquetFile",
                  ParquetConverters.ReadParquetFile.newBuilder()
                      .withInputFileSpec(options().getInputFileSpec())
                      .withSchema(options().getSchema())
                      .build());
          break;

        default:
          LOG.error("Invalid input file format.");
          throw new IllegalArgumentException("Invalid input file format.");
      }

      switch (ValidFileFormats.valueOf(outputFileFormat)) {
        case AVRO:
          inputFile.apply(
              "WriteAvroFile(s)",
              AvroConverters.WriteAvroFile.newBuilder()
                  .withOutputFile(options().getOutputBucket())
                  .withSchema(options().getSchema())
                  .setOutputFilePrefix(options().getOutputFilePrefix())
                  .setNumShards(options().getNumShards())
                  .build());
          break;

        case PARQUET:
          inputFile.apply(
              "WriteParquetFile(s)",
              ParquetConverters.WriteParquetFile.newBuilder()
                  .withOutputFile(options().getOutputBucket())
                  .withSchema(options().getSchema())
                  .setOutputFilePrefix(options().getOutputFilePrefix())
                  .setNumShards(options().getNumShards())
                  .build());
          break;

        default:
          LOG.error("Invalid output file format.");
          throw new IllegalArgumentException("Invalid output file format.");
      }
      return PDone.in(input.getPipeline());
    }

    /** Builder for {@link FileFormat}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setOptions(FileFormatConversionOptions options);

      public abstract FileFormat build();
    }
  }
}
