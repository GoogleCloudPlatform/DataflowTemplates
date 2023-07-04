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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

/** Common transforms for Avro files. */
public class AvroConverters {

  /** Options for reading or writing Avro files. */
  public interface AvroOptions extends PipelineOptions {
    @TemplateParameter.GcsReadFile(
        order = 1,
        description = "The input filepattern to read from.",
        helpText = "Cloud storage file pattern glob to read from.",
        example = "gs://your-bucket/path/*.avro")
    String getInputFileSpec();

    void setInputFileSpec(String inputFileSpec);

    @TemplateParameter.GcsWriteFolder(
        order = 2,
        description = "Output Cloud Storage directory.",
        helpText =
            "Cloud storage directory for writing output files. This value must end in a slash.",
        example = "gs://your-bucket/path/")
    String getOutputBucket();

    void setOutputBucket(String outputBucket);

    @TemplateParameter.GcsReadFile(
        order = 3,
        description = "Path of the Avro schema file used for the conversion.",
        helpText = "Cloud storage path to the avro schema file.",
        example = "gs://your-bucket/your-path/schema.avsc")
    String getSchema();

    void setSchema(String schema);

    @TemplateParameter.Integer(
        order = 4,
        optional = true,
        description = "Maximum output shards",
        helpText =
            "The maximum number of output shards produced when writing. A higher number of "
                + "shards means higher throughput for writing to Cloud Storage, but potentially higher "
                + "data aggregation cost across shards when processing output Cloud Storage files. "
                + "Default value is decided by Dataflow.")
    @Default.Integer(0)
    Integer getNumShards();

    void setNumShards(Integer numShards);

    @TemplateParameter.Text(
        order = 5,
        optional = true,
        description = "Output file prefix.",
        helpText = "The prefix of the files to write to.")
    @Default.String("output")
    String getOutputFilePrefix();

    void setOutputFilePrefix(String outputFilePrefix);
  }

  /**
   * The {@link ReadAvroFile} class is a {@link PTransform} that reads from one or more Avro files
   * and returns a {@link PCollection} of {@link GenericRecord}.
   */
  @AutoValue
  public abstract static class ReadAvroFile extends PTransform<PBegin, PCollection<GenericRecord>> {
    public static Builder newBuilder() {
      return new AutoValue_AvroConverters_ReadAvroFile.Builder();
    }

    @Nullable
    public abstract String schema();

    @Nullable
    public abstract String serializedSchema();

    public abstract String inputFileSpec();

    @Override
    public PCollection<GenericRecord> expand(PBegin input) {
      return input.apply(
          "ReadAvroFile",
          AvroIO.readGenericRecords(
                  serializedSchema() != null
                      ? SchemaUtils.parseAvroSchema(serializedSchema())
                      : SchemaUtils.getAvroSchema(schema()))
              .from(inputFileSpec()));
    }

    /** Builder for {@link ReadAvroFile}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setSchema(String schema);

      public abstract String schema();

      public abstract Builder setSerializedSchema(String schema);

      public abstract String serializedSchema();

      public abstract Builder setInputFileSpec(String inputFile);

      public abstract String inputFileSpec();

      public abstract ReadAvroFile autoBuild();

      public Builder withSchema(String schema) {
        checkArgument(schema != null, "withSchema(schema) called with null input.");
        return setSchema(schema);
      }

      public Builder withSerializedSchema(String schema) {
        checkArgument(schema != null, "withSerializedSchema(schema) called with null input.");
        return setSerializedSchema(schema);
      }

      public Builder withInputFileSpec(String inputFileSpec) {
        checkArgument(
            inputFileSpec != null, "withInputFileSpec(inputFileSpec) called with null input.");
        return setInputFileSpec(inputFileSpec);
      }

      public ReadAvroFile build() {
        checkNotNull(inputFileSpec(), "provide an Avro file to read from.");
        if ((schema() == null) == (serializedSchema() == null)) {
          throw new IllegalArgumentException(
              "Either schema location or serialized schema must be set.");
        }

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

    @Nullable
    public abstract String schema();

    @Nullable
    public abstract String serializedSchema();

    public abstract String outputFile();

    public abstract String outputFilePrefix();

    public abstract Integer numShards();

    @Override
    public POutput expand(PCollection<GenericRecord> input) {
      return input.apply(
          "WriteAvroFile(s)",
          AvroIO.writeGenericRecords(
                  serializedSchema() != null
                      ? SchemaUtils.parseAvroSchema(serializedSchema())
                      : SchemaUtils.getAvroSchema(schema()))
              .to(outputFile().concat(outputFilePrefix()))
              .withNumShards(numShards())
              .withSuffix(AVRO_SUFFIX));
    }

    /** Builder for {@link WriteAvroFile}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setSchema(String schema);

      public abstract String schema();

      public abstract Builder setSerializedSchema(String schema);

      public abstract String serializedSchema();

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

      public Builder withSerializedSchema(String schema) {
        checkArgument(schema != null, "withSerializedSchema(schema) called with null input.");
        return setSerializedSchema(schema);
      }

      public WriteAvroFile build() {
        if ((schema() == null) == (serializedSchema() == null)) {
          throw new IllegalArgumentException(
              "Either schema location or serialized schema must be set.");
        }
        checkNotNull(outputFile(), "provide a location to write the output Avro file to.");

        return autoBuild();
      }
    }
  }
}
