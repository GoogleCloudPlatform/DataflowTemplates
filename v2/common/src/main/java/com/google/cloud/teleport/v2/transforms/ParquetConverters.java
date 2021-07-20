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
package com.google.cloud.teleport.v2.transforms;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

/** Common transforms for Parquet files. */
public class ParquetConverters {

  /** Options for reading or writing Parquet files. */
  public interface ParquetOptions extends PipelineOptions {
    @Description("GCS bucket to read input file from (e.g. gs://mybucket/path/file).")
    String getInputFileSpec();

    void setInputFileSpec(String inputFile);

    @Description(
        "GCS bucket to write output file(s) to (e.g. gs://mybucket/path/). Must end with a slash.")
    String getOutputBucket();

    void setOutputBucket(String outputBucket);

    @Description("Path to schema (e.g. gs://mybucket/path/).")
    String getSchema();

    void setSchema(String schema);

    @Description(
        "The maximum number of output shards produced when writing. Default value is decided by Runner.")
    @Default.Integer(0)
    Integer getNumShards();

    void setNumShards(Integer numShards);

    @Description("The prefix of the files to write to. Default is: output.")
    @Default.String("output")
    String getOutputFilePrefix();

    void setOutputFilePrefix(String outputFilePrefix);
  }

  /**
   * The {@link ReadParquetFile} class is a {@link PTransform} that reads from one or more Parquet
   * files and returns a {@link PCollection} of {@link GenericRecord}.
   */
  @AutoValue
  public abstract static class ReadParquetFile
      extends PTransform<PBegin, PCollection<GenericRecord>> {
    public static Builder newBuilder() {
      return new AutoValue_ParquetConverters_ReadParquetFile.Builder();
    }

    public abstract String schema();

    public abstract String inputFileSpec();

    @Override
    public PCollection<GenericRecord> expand(PBegin input) {
      return input.apply(
          "ReadParquetFile",
          ParquetIO.read(SchemaUtils.getAvroSchema(schema())).from(inputFileSpec()));
    }

    /** Builder for {@link ReadParquetFile}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setSchema(String schema);

      public abstract String schema();

      public abstract Builder setInputFileSpec(String inputFile);

      public abstract String inputFileSpec();

      public abstract ReadParquetFile autoBuild();

      public Builder withInputFileSpec(String inputFileSpec) {
        checkArgument(
            inputFileSpec != null, "withInputFileSpec(inputFileSpec) called with null input.");
        return setInputFileSpec(inputFileSpec);
      }

      public Builder withSchema(String schema) {
        checkArgument(schema != null, "withSchema(schema) called with null input.");
        return setSchema(schema);
      }

      public ReadParquetFile build() {
        checkNotNull(inputFileSpec(), "provide a Parquet file to read from.");
        checkNotNull(schema(), "schema needs to be provided while reading a Parquet file.");

        return autoBuild();
      }
    }
  }

  /**
   * The {@link WriteParquetFile} class is a {@link PTransform} that writes one or more Parquet
   * files.
   */
  @AutoValue
  public abstract static class WriteParquetFile
      extends PTransform<PCollection<GenericRecord>, POutput> {
    private static final String PARQUET_SUFFIX = ".parquet";

    public static Builder newBuilder() {
      return new AutoValue_ParquetConverters_WriteParquetFile.Builder();
    }

    public abstract String schema();

    public abstract String outputFile();

    public abstract String outputFilePrefix();

    public abstract Integer numShards();

    @Override
    public POutput expand(PCollection<GenericRecord> input) {
      return input.apply(
          "WriteParquetFile(s)",
          FileIO.<GenericRecord>write()
              .via(ParquetIO.sink(SchemaUtils.getAvroSchema(schema())))
              .to(outputFile())
              .withNumShards(numShards())
              .withPrefix(outputFilePrefix())
              .withSuffix(PARQUET_SUFFIX));
    }

    /** Builder for {@link WriteParquetFile}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setSchema(String schema);

      public abstract String schema();

      public abstract Builder setOutputFile(String outputFile);

      public abstract String outputFile();

      public abstract Builder setOutputFilePrefix(String outputFilePrefix);

      public abstract Builder setNumShards(Integer numShards);

      public abstract WriteParquetFile autoBuild();

      public Builder withOutputFile(String outputFile) {
        checkArgument(outputFile != null, "withOutputFile(outputFile) called with null input.");
        return setOutputFile(outputFile);
      }

      public Builder withSchema(String schema) {
        checkArgument(schema != null, "withSchema(schema) called with null input.");
        return setSchema(schema);
      }

      public WriteParquetFile build() {
        checkNotNull(schema(), "provide an Avro schema to read the Parquet file.");
        checkNotNull(outputFile(), "provide a location to write the output Parquet file to.");

        return autoBuild();
      }
    }
  }
}
