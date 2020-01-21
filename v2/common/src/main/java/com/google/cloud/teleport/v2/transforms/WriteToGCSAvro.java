/*
 * Copyright (C) 2019 Google Inc.
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
package com.google.cloud.teleport.v2.transforms;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link WriteToGCSAvro} class is a {@link PTransform} that takes in {@link PCollection} of KV
 * records. The transform converts and writes these records to GCS in avro file format.
 */
@AutoValue
public abstract class WriteToGCSAvro extends PTransform<PCollection<KV<String, String>>, PDone> {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(WriteToGCSAvro.class);

  @VisibleForTesting
  protected static final String DEFAULT_OUTPUT_FILE_PREFIX = "output";

  /**
   * The {@link WriteToGCSAvroOptions} interface provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface WriteToGCSAvroOptions extends PipelineOptions {

    @Description("The directory to output files to. Must end with a slash. ")
    String getOutputDirectory();

    void setOutputDirectory(String outputDirectory);

    @Description(
        "The filename prefix of the files to write to. Default file prefix is set to \"output\". ")
    String getOutputFilenamePrefix();

    void setOutputFilenamePrefix(String outputFilenamePrefix);

    @Description(
        "The maximum number of output shards produced when writing. Default number is runner defined. ")
    @Default.Integer(0)
    Integer getNumShards();

    void setNumShards(Integer numShards);
  }

  public static WriteToGCSBuilder newBuilder() {
    return new AutoValue_WriteToGCSAvro.Builder();
  }

  public abstract String outputDirectory();

  public abstract String outputFilenamePrefix();

  public abstract String tempLocation();

  public abstract Integer numShards();

  @Override
  public PDone expand(PCollection<KV<String, String>> kafkaRecords) {
    return kafkaRecords
        /*
         * Converting KV<String, String> records to GenericRecord using DoFn and {@link
         * KeyValueToGenericRecordFn} class.
         */
        .apply("Create GenericRecord(s)", ParDo.of(new KeyValueToGenericRecordFn()))
        .setCoder(AvroCoder.of(GenericRecord.class, KeyValueToGenericRecordFn.SCHEMA))
        /*
         * Writing as avro file using {@link AvroIO}.
         *
         * The {@link WindowedFilenamePolicy} class specifies the file path for writing the file.
         * The {@link withNumShards} option specifies the number of shards passed by the user.
         * The {@link withTempDirectory} option sets the base directory used to generate temporary files.
         */
        .apply(
            "Writing as Avro",
            AvroIO.writeGenericRecords(KeyValueToGenericRecordFn.SCHEMA)
                .to(
                    new WindowedFilenamePolicy(
                        outputDirectory(),
                        outputFilenamePrefix(),
                        WriteToGCSUtility.SHARD_TEMPLATE,
                        WriteToGCSUtility.FILE_SUFFIX_MAP.get(WriteToGCSUtility.FileFormat.AVRO)))
                .withTempDirectory(
                    FileBasedSink.convertToFileResourceIfPossible(tempLocation())
                        .getCurrentDirectory())
                .withWindowedWrites()
                .withNumShards(numShards()));
  }

  /** Builder for {@link WriteToGCSAvro}. */
  @AutoValue.Builder
  public abstract static class WriteToGCSBuilder {

    abstract WriteToGCSBuilder setOutputDirectory(String outputDirectory);

    abstract String outputDirectory();

    abstract WriteToGCSBuilder setTempLocation(String tempLocation);

    abstract String tempLocation();

    abstract WriteToGCSBuilder setOutputFilenamePrefix(String outputFilenamePrefix);

    abstract WriteToGCSBuilder setNumShards(Integer numShards);

    abstract WriteToGCSAvro autoBuild();

    public WriteToGCSBuilder withOutputDirectory(String outputDirectory) {
      checkArgument(
          outputDirectory != null, "withOutputDirectory(outputDirectory) called with null input.");
      return setOutputDirectory(outputDirectory);
    }

    public WriteToGCSBuilder withTempLocation(String tempLocation) {
      checkArgument(
          tempLocation != null, "withTempLocation(tempLocation) called with null input. ");
      return setTempLocation(tempLocation);
    }

    public WriteToGCSBuilder withOutputFilenamePrefix(String outputFilenamePrefix) {
      if (outputFilenamePrefix == null) {
        LOG.info("Defaulting output filename prefix to: {}", DEFAULT_OUTPUT_FILE_PREFIX);
        outputFilenamePrefix = DEFAULT_OUTPUT_FILE_PREFIX;
      }
      return setOutputFilenamePrefix(outputFilenamePrefix);
    }

    public WriteToGCSAvro build() {
      checkNotNull(outputDirectory(), "Provide output directory to write to. ");
      checkNotNull(tempLocation(), "Temporary directory needs to be provided. ");

      return autoBuild();
    }
  }
}
