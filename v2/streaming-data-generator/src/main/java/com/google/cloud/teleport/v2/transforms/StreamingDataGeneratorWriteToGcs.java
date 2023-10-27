/*
 * Copyright (C) 2020 Google LLC
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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator;
import com.google.cloud.teleport.v2.utils.GCSUtils;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Instant;

/**
 * A {@link PTransform} converts fakeMessages to either JSON, Avro or Parquet records based on
 * output encoding option and loads to GCS at the end of each window interval.
 */
@AutoValue
public abstract class StreamingDataGeneratorWriteToGcs
    extends PTransform<PCollection<byte[]>, PDone> {
  private static final String SHARD_TEMPLATE = "W-SS-of-NN";

  abstract StreamingDataGenerator.StreamingDataGeneratorOptions getPipelineOptions();

  public static Builder builder(StreamingDataGenerator.StreamingDataGeneratorOptions options) {
    return new AutoValue_StreamingDataGeneratorWriteToGcs.Builder().setPipelineOptions(options);
  }

  /** Builder for {@link StreamingDataGeneratorWriteToGcs}. */
  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setPipelineOptions(StreamingDataGenerator.StreamingDataGeneratorOptions value);

    public abstract StreamingDataGeneratorWriteToGcs build();
  }

  /** A {@link DoFn} that converts UTF8 Bytes to JSON Object. */
  private static class Utf8BytesToJsonFn extends DoFn<byte[], String> {
    @ProcessElement
    public void processElement(
        @Element byte[] element,
        @Timestamp Instant timestamp,
        OutputReceiver<String> receiver,
        ProcessContext context) {
      // parsing as json for formatting
      receiver.output(
          JsonParser.parseString(new String(element, StandardCharsets.UTF_8)).toString());
    }
  }

  /** A {@link DoFn} that converts UTF8 Bytes to Avro Generic Record. */
  private static class Utf8BytesToAvroGenericRecord extends DoFn<byte[], GenericRecord> {

    private final String schema;
    private Schema avroSchema = null;
    private DatumReader<GenericRecord> genericDatumReader = null;

    public Utf8BytesToAvroGenericRecord(String schema) {
      this.schema = schema;
    }

    @Setup
    public void setup() throws IOException {
      this.avroSchema = SchemaUtils.parseAvroSchema(this.schema);
      this.genericDatumReader = new GenericDatumReader<>(avroSchema);
    }

    @ProcessElement
    public void processElement(
        @Element byte[] element,
        @Timestamp Instant timestamp,
        OutputReceiver<GenericRecord> receiver,
        ProcessContext context)
        throws IOException {
      // Decode the json value using json Decoder from UTF8 encoded bytes
      JsonDecoder jsonDecoder =
          DecoderFactory.get()
              .jsonDecoder(this.avroSchema, new String(element, StandardCharsets.UTF_8));
      receiver.output(this.genericDatumReader.read(null, jsonDecoder));
    }
  }

  private WindowedFilenamePolicy getFileNamePolicy() {
    return WindowedFilenamePolicy.writeWindowedFiles()
        .withOutputDirectory(getPipelineOptions().getOutputDirectory())
        .withOutputFilenamePrefix(getPipelineOptions().getOutputFilenamePrefix())
        .withShardTemplate(SHARD_TEMPLATE)
        .withSuffix(getPipelineOptions().getOutputType().getFileExtension());
  }

  /** Converts the fake messages in bytes to json format and writes to a text file. */
  private void writeAsJson(PCollection<byte[]> fakeMessages) {
    fakeMessages
        .apply("Convert to JSON", ParDo.of(new Utf8BytesToJsonFn()))
        .apply(
            "Write output",
            TextIO.write()
                .to(getFileNamePolicy())
                .withTempDirectory(
                    FileBasedSink.convertToFileResourceIfPossible(
                            getPipelineOptions().getTempLocation())
                        .getCurrentDirectory())
                .withWindowedWrites()
                .withNumShards(getPipelineOptions().getNumShards()));
  }

  /** Converts the fake messages in bytes to json format and writes to a text file. */
  private void writeAsAvro(PCollection<GenericRecord> genericRecords, Schema avroSchema) {
    genericRecords.apply(
        "Write Avro output",
        AvroIO.writeGenericRecords(avroSchema)
            .to(getFileNamePolicy())
            .withTempDirectory(
                FileBasedSink.convertToFileResourceIfPossible(
                        getPipelineOptions().getTempLocation())
                    .getCurrentDirectory())
            .withWindowedWrites()
            .withNumShards(getPipelineOptions().getNumShards()));
  }

  /** Converts the fake messages in bytes to json format and writes to a text file. */
  private void writeAsParquet(PCollection<GenericRecord> genericRecords, Schema avroSchema) {
    genericRecords.apply(
        "Write Parquet output",
        FileIO.<GenericRecord>write()
            .via(ParquetIO.sink(avroSchema))
            .to(getPipelineOptions().getOutputDirectory())
            .withPrefix(getPipelineOptions().getOutputFilenamePrefix())
            .withSuffix(getPipelineOptions().getOutputType().getFileExtension())
            .withNumShards(getPipelineOptions().getNumShards()));
  }

  @Override
  public PDone expand(PCollection<byte[]> fakeMessages) {
    // Refer
    // https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/io/WriteFiles.java#L316
    checkArgument(
        getPipelineOptions().getNumShards() > 0,
        "--numShards parameter should be set to 1 or higher number while writing unbounded"
            + " PCollection to GCS");
    switch (getPipelineOptions().getOutputType()) {
      case JSON:
        writeAsJson(fakeMessages);
        break;
      case AVRO:
      case PARQUET:
        checkNotNull(
            getPipelineOptions().getAvroSchemaLocation(),
            String.format(
                "Missing required value for --avroSchemaLocation for %s output type",
                getPipelineOptions().getOutputType()));
        String schema = GCSUtils.getGcsFileAsString(getPipelineOptions().getAvroSchemaLocation());
        Schema avroSchema = SchemaUtils.parseAvroSchema(schema);
        PCollection<GenericRecord> genericRecords =
            fakeMessages
                .apply(
                    "Convert to GenericRecord", ParDo.of(new Utf8BytesToAvroGenericRecord(schema)))
                .setCoder(AvroGenericCoder.of(avroSchema));

        if (getPipelineOptions().getOutputType().equals(StreamingDataGenerator.OutputType.AVRO)) {
          writeAsAvro(genericRecords, avroSchema);
        } else {
          writeAsParquet(genericRecords, avroSchema);
        }
        break;
    }

    return PDone.in(fakeMessages.getPipeline());
  }
}
