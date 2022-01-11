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
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link WriteToGCSParquet} class is a {@link PTransform} that takes in {@link PCollection} of
 * KV records.The transform converts and writes these records to GCS in parquet file format.
 */
@AutoValue
public abstract class WriteToGCSParquet
    extends PTransform<PCollection<KV<String, String>>, WriteFilesResult<Void>> {

  @VisibleForTesting protected static final String DEFAULT_OUTPUT_FILE_PREFIX = "output";
  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(WriteToGCSParquet.class);

  public static WriteToGCSBuilder newBuilder() {
    return new AutoValue_WriteToGCSParquet.Builder();
  }

  public abstract String outputDirectory();

  public abstract String outputFilenamePrefix();

  public abstract Integer numShards();

  @Override
  public WriteFilesResult<Void> expand(PCollection<KV<String, String>> kafkaRecords) {
    return kafkaRecords
        /*
         * Converting KV<String, String> records to GenericRecord using DoFn and {@link
         * KeyValueToGenericRecordFn} class.
         */
        .apply("Create GenericRecord(s)", ParDo.of(new KeyValueToGenericRecordFn()))
        .setCoder(AvroCoder.of(GenericRecord.class, KeyValueToGenericRecordFn.SCHEMA))
        /*
         * Writing as parquet file using {@link FileIO} and {@link ParquetIO}.
         *
         * The {@link WindowedFilenamePolicy} class specifies the file path for writing the file.
         * The {@link withNumShards} option specifies the number of shards passed by the user.
         */
        .apply(
            "Writing as Parquet",
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(KeyValueToGenericRecordFn.SCHEMA))
                .to(outputDirectory())
                .withPrefix(outputFilenamePrefix())
                .withSuffix(
                    WriteToGCSUtility.FILE_SUFFIX_MAP.get(WriteToGCSUtility.FileFormat.PARQUET))
                .withNumShards(numShards()));
  }

  /**
   * The {@link WriteToGCSParquetOptions} interface provides the custom execution options passed by
   * the executor at the command-line.
   */
  public interface WriteToGCSParquetOptions extends PipelineOptions {

    @Description("The directory to output files to. Must end with a slash. ")
    String getOutputDirectory();

    void setOutputDirectory(String outputDirectory);

    @Description(
        "The filename prefix of the files to write to. Default file prefix is set to \"output\". ")
    String getOutputFilenamePrefix();

    void setOutputFilenamePrefix(String outputFilenamePrefix);

    @Description(
        "The maximum number of output shards produced when writing. Default number is runner"
            + " defined. ")
    @Default.Integer(0)
    Integer getNumShards();

    void setNumShards(Integer numShards);
  }

  /** Builder for {@link WriteToGCSParquet}. */
  @AutoValue.Builder
  public abstract static class WriteToGCSBuilder {

    abstract WriteToGCSBuilder setOutputDirectory(String outputDirectory);

    abstract String outputDirectory();

    abstract WriteToGCSBuilder setOutputFilenamePrefix(String outputFilenamePrefix);

    abstract WriteToGCSBuilder setNumShards(Integer numShards);

    abstract WriteToGCSParquet autoBuild();

    public WriteToGCSBuilder withOutputDirectory(String outputDirectory) {
      checkArgument(
          outputDirectory != null, "withOutputDirectory(outputDirectory) called with null input.");
      return setOutputDirectory(outputDirectory);
    }

    public WriteToGCSBuilder withOutputFilenamePrefix(String outputFilenamePrefix) {
      if (outputFilenamePrefix == null) {
        LOG.info("Defaulting output filename prefix to: {}", DEFAULT_OUTPUT_FILE_PREFIX);
        outputFilenamePrefix = DEFAULT_OUTPUT_FILE_PREFIX;
      }
      return setOutputFilenamePrefix(outputFilenamePrefix);
    }

    public WriteToGCSParquet build() {
      checkNotNull(outputDirectory(), "Provide output directory to write to. ");

      return autoBuild();
    }
  }
}
