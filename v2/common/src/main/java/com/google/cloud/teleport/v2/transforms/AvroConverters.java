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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

/** Common transforms for Avro files. */
public class AvroConverters {

  /** Options for reading or writing Avro files. */
  public interface AvroOptions extends PipelineOptions {
    @Description("GCS bucket to read input file from (e.g. gs://mybucket/path/file).")
    String getInputFileSpec();

    void setInputFileSpec(String inputFileSpec);

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
   * The {@link ReadAvroFile} class is a {@link PTransform} that reads from one or more Avro files
   * and returns a {@link PCollection} of {@link GenericRecord>}.
   */
  @AutoValue
  public abstract static class ReadAvroFile extends PTransform<PBegin, PCollection<GenericRecord>> {
    public static Builder newBuilder() {
      return new AutoValue_AvroConverters_ReadAvroFile.Builder();
    }

    public abstract String schema();

    public abstract String inputFileSpec();

    @Override
    public PCollection<GenericRecord> expand(PBegin input) {
      return input.apply(
          "ReadAvroFile",
          AvroIO.readGenericRecords(SchemaUtils.getAvroSchema(schema())).from(inputFileSpec()));
    }

    /** Builder for {@link ReadAvroFile}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setSchema(String schema);

      public abstract String schema();

      public abstract Builder setInputFileSpec(String inputFile);

      public abstract String inputFileSpec();

      public abstract ReadAvroFile autoBuild();

      public Builder withSchema(String schema) {
        checkArgument(schema != null, "withSchema(schema) called with null input.");
        return setSchema(schema);
      }

      public Builder withInputFileSpec(String inputFileSpec) {
        checkArgument(
            inputFileSpec != null, "withInputFileSpec(inputFileSpec) called with null input.");
        return setInputFileSpec(inputFileSpec);
      }

      public ReadAvroFile build() {
        checkNotNull(inputFileSpec(), "provide an Avro file to read from.");
        checkNotNull(schema(), "provide an Avro schema to read an Avro file.");

        return autoBuild();
      }
    }
  }

  /** The {@link WriteAvroFile} class is a {@link PTransform} that writes one or more Avro files. */
  @AutoValue
  public abstract static class WriteAvroFile
      extends PTransform<PCollection<GenericRecord>, POutput> {
    private static final String AVRO_SUFFIX = ".avro";

    public static Builder newBuilder() {
      return new AutoValue_AvroConverters_WriteAvroFile.Builder();
    }

    public abstract String schema();

    public abstract String outputFile();

    public abstract String outputFilePrefix();

    public abstract Integer numShards();

    @Override
    public POutput expand(PCollection<GenericRecord> input) {
      return input.apply(
          "WriteAvroFile(s)",
          AvroIO.writeGenericRecords(SchemaUtils.getAvroSchema(schema()))
              .to(outputFile().concat(outputFilePrefix()))
              .withNumShards(numShards())
              .withSuffix(AVRO_SUFFIX));
    }

    /** Builder for {@link WriteAvroFile}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setSchema(String schema);

      public abstract String schema();

      public abstract Builder setOutputFile(String outputFile);

      public abstract String outputFile();

      public abstract Builder setOutputFilePrefix(String outputFilePrefix);

      public abstract Builder setNumShards(Integer numShards);

      public abstract WriteAvroFile autoBuild();

      public Builder withOutputFile(String outputFile) {
        checkArgument(outputFile != null, "withOutputFile(outputFile) called with null input.");
        return setOutputFile(outputFile);
      }

      public Builder withSchema(String schema) {
        checkArgument(schema != null, "withSchema(schema) called with null input.");
        return setSchema(schema);
      }

      public WriteAvroFile build() {
        checkNotNull(schema(), "provide an Avro schema to write an Avro file.");
        checkNotNull(outputFile(), "provide a location to write the output Avro file to.");

        return autoBuild();
      }
    }
  }
}
