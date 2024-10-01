/*
 * Copyright (C) 2024 Google LLC
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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.gson.JsonParser;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Instant;

/** A {@link PTransform} that converts generatedMessages to write to Spanner table. */
public final class StreamingDataGeneratorWriteToKafka {

  private StreamingDataGeneratorWriteToKafka() {}

  /** Creates Kafka message with JSON (UTF-8) encoded Payload. */
  @VisibleForTesting
  public static class JsonKafkaMessageFn extends DoFn<byte[], String> {

    @ProcessElement
    public void processElement(
        @Element byte[] element,
        @Timestamp Instant timestamp,
        OutputReceiver<String> receiver,
        ProcessContext context)
        throws IOException {

      receiver.output(
          JsonParser.parseString(new String(element, StandardCharsets.UTF_8)).toString());
    }
  }

  /**
   * Creates KafkaMessage with Binary Avro encoded Payload.KafkaMessage attributes are populated if
   * user provided message schema contains attributes property.
   */
  @VisibleForTesting
  public static class AvroKafkaMessageFn extends DoFn<byte[], String> {
    private final String avroSchemaLocation;
    private Schema schemaObj = null;
    private DatumReader<GenericRecord> genericDatumReader = null;
    private DatumWriter<GenericRecord> genericDatumWriter = null;

    public AvroKafkaMessageFn(String avroSchemaLocation) {
      this.avroSchemaLocation = avroSchemaLocation;
    }

    @Setup
    public void setup() {
      this.schemaObj = SchemaUtils.getAvroSchema(avroSchemaLocation);
      this.genericDatumReader = new GenericDatumReader<>(schemaObj);
      this.genericDatumWriter = new GenericDatumWriter<>(schemaObj);
    }

    @ProcessElement
    public void processElement(
        @Element byte[] element,
        @Timestamp Instant timestamp,
        OutputReceiver<String> receiver,
        ProcessContext context)
        throws IOException {

      receiver.output(utf8ToBinary(element));
    }

    private String utf8ToBinary(byte[] payload) throws IOException {
      String contents = new String(payload, StandardCharsets.UTF_8);

      // Decode the json value using json Decoder from UTF8 encoded bytes
      JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schemaObj, contents);
      GenericRecord genericRecord = this.genericDatumReader.read(null, jsonDecoder);

      // Encode the json value into Avro Binary encoded value
      try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
        BinaryEncoder binaryEncoder =
            EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        genericDatumWriter.write(genericRecord, binaryEncoder);
        binaryEncoder.flush(); // writes Generic record content to byteArrayOutputStream
        return Arrays.toString(byteArrayOutputStream.toByteArray());
      }
    }
  }

  /**
   * A {@link PTransform} converts generatedMessages to either JSON encoded or Avro encoded Kafka
   * messages based on Pipeline options and publishes to Kafka.
   */
  @AutoValue
  public abstract static class Writer extends PTransform<PCollection<byte[]>, PDone> {

    abstract StreamingDataGenerator.StreamingDataGeneratorOptions getPipelineOptions();

    public static StreamingDataGeneratorWriteToKafka.Writer.Builder builder(
        StreamingDataGenerator.StreamingDataGeneratorOptions options) {
      return new AutoValue_StreamingDataGeneratorWriteToKafka_Writer.Builder()
          .setPipelineOptions(options);
    }

    /** Builder for {@link StreamingDataGeneratorWriteToKafka.Writer}. */
    @AutoValue.Builder
    public abstract static class Builder {
      abstract StreamingDataGeneratorWriteToKafka.Writer.Builder setPipelineOptions(
          StreamingDataGenerator.StreamingDataGeneratorOptions value);

      public abstract StreamingDataGeneratorWriteToKafka.Writer build();
    }

    @Override
    public PDone expand(PCollection<byte[]> generatedMessages) {
      PCollection<String> kafkaMessages = null;
      StreamingDataGenerator.StreamingDataGeneratorOptions options = getPipelineOptions();
      switch (options.getOutputType()) {
        case JSON:
          kafkaMessages =
              generatedMessages.apply(
                  "Generate JSON Kafka Messages",
                  ParDo.of(new StreamingDataGeneratorWriteToKafka.JsonKafkaMessageFn()));
          break;
        case AVRO:
          checkNotNull(
              options.getAvroSchemaLocation(),
              String.format(
                  "Missing required value for --avroSchemaLocation for %s output type",
                  options.getOutputType()));
          kafkaMessages =
              generatedMessages.apply(
                  "Generate Avro Kafka Messages",
                  ParDo.of(
                      new StreamingDataGeneratorWriteToKafka.AvroKafkaMessageFn(
                          options.getAvroSchemaLocation())));
          break;
        default:
          throw new IllegalArgumentException(
              String.format(
                  "Invalid output type %s.Supported Output types for %s sink are: %s",
                  options.getOutputType(),
                  options.getSinkType(),
                  Joiner.on(",")
                      .join(
                          StreamingDataGenerator.OutputType.JSON.name(),
                          StreamingDataGenerator.OutputType.AVRO.name())));
      }

      return kafkaMessages.apply(
          "writeSuccessMessages",
          KafkaIO.<Void, String>write()
              .withBootstrapServers(getPipelineOptions().getBootstrapServer())
              .withTopic(getPipelineOptions().getKafkaTopic())
              .withValueSerializer(StringSerializer.class)
              .values());
    }
  }
}
