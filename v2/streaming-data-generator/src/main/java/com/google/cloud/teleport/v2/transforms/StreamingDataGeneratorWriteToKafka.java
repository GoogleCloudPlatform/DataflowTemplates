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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
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
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.ProducerRecordCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * A {@link PTransform} that converts generated messages to Kafka records and publishes them to a
 * Kafka topic.
 *
 * <p>By default the whole generated message is written as the Kafka record value, without a record
 * key or headers. To also populate the record key and headers, wrap the message schema in an
 * envelope with a mandatory {@code payload} member and optional {@code key} and {@code headers}
 * members:
 *
 * <pre>{@code
 * {
 *   "payload": {
 *     "accountId": "{{put("accountId", long(100000000000, 999999999999))}}",
 *     "amount": {{float(-500.0, 500.0, "%.2f")}}
 *   },
 *   "key": "{{get("accountId")}}",
 *   "headers": {
 *     "source": "streaming-data-generator",
 *     "correlationId": "{{uuid()}}"
 *   }
 * }
 * }</pre>
 *
 * <p>Only {@code payload} is written as the record value (JSON or Avro encoded, so an Avro schema
 * must describe the payload only). {@code key} becomes the record key, serialized as a UTF-8 string
 * (primitives in their textual form, objects and arrays as compact JSON). Every member of {@code
 * headers} becomes a record header holding the UTF-8 encoded textual form of its value; members
 * with a {@code null} value are skipped. A generated message is treated as an envelope only when it
 * is a JSON object that has a {@code payload} member and no members other than {@code payload},
 * {@code key} and {@code headers}.
 */
public final class StreamingDataGeneratorWriteToKafka {

  private static final Pattern ManagedKafkaRegex =
      Pattern.compile("bootstrap\\..*\\.managedkafka\\..*\\.cloud\\.goog.*");

  /** Envelope member holding the message payload, written as the Kafka record value. */
  public static final String PAYLOAD_FIELD = "payload";

  /** Envelope member holding the Kafka record key. */
  public static final String KEY_FIELD = "key";

  /** Envelope member holding the Kafka record headers. */
  public static final String HEADERS_FIELD = "headers";

  private static final Set<String> ENVELOPE_FIELDS =
      Set.of(PAYLOAD_FIELD, KEY_FIELD, HEADERS_FIELD);

  private StreamingDataGeneratorWriteToKafka() {}

  /**
   * Returns the coder of the records handed to {@link KafkaIO}. The key coder must be nullable
   * because messages without an envelope (or without a {@code key} member) are written keyless.
   */
  public static ProducerRecordCoder<String, byte[]> recordCoder() {
    return ProducerRecordCoder.of(NullableCoder.of(StringUtf8Coder.of()), ByteArrayCoder.of());
  }

  /**
   * Splits a generated message into Kafka record key, headers and payload (see the envelope format
   * in the class documentation) and lets subclasses encode the payload into the record value.
   */
  private abstract static class BaseKafkaMessageFn
      extends DoFn<byte[], ProducerRecord<String, byte[]>> {

    private final String topic;

    BaseKafkaMessageFn(String topic) {
      this.topic = checkNotNull(topic, "topic cannot be null");
    }

    /** Encodes the payload, i.e. the generated message without the envelope, as record value. */
    protected abstract byte[] encodePayload(JsonElement payload) throws IOException;

    // Must stay final: as this class is not public, javac would otherwise add a synthetic bridge
    // copy of this method to each public subclass. The copy keeps @ProcessElement but has erased
    // parameter types, so Beam rejects its raw OutputReceiver.
    @ProcessElement
    public final void processElement(
        @Element byte[] element, OutputReceiver<ProducerRecord<String, byte[]>> receiver)
        throws IOException {
      JsonElement message = JsonParser.parseString(new String(element, StandardCharsets.UTF_8));

      JsonElement payload = message;
      String key = null;
      List<Header> headers = new ArrayList<>();
      if (isEnvelope(message)) {
        JsonObject envelope = message.getAsJsonObject();
        payload = envelope.get(PAYLOAD_FIELD);
        key = toKey(envelope.get(KEY_FIELD));
        headers = toHeaders(envelope.get(HEADERS_FIELD));
      }

      receiver.output(
          new ProducerRecord<>(
              topic,
              /* partition= */ null,
              /* timestamp= */ null,
              key,
              encodePayload(payload),
              headers));
    }

    private static boolean isEnvelope(JsonElement message) {
      if (!message.isJsonObject()) {
        return false;
      }
      JsonObject object = message.getAsJsonObject();
      return object.has(PAYLOAD_FIELD) && ENVELOPE_FIELDS.containsAll(object.keySet());
    }

    /**
     * Converts the {@code key} member to the record key. Primitives are used in their textual form,
     * so the key may be generated quoted or unquoted; objects and arrays are used as compact JSON.
     */
    private static @Nullable String toKey(@Nullable JsonElement key) {
      if (key == null || key.isJsonNull()) {
        return null;
      }
      return key.isJsonPrimitive() ? key.getAsString() : key.toString();
    }

    /**
     * Converts the {@code headers} member to record headers. Every member becomes one header whose
     * value is the UTF-8 encoded textual form of the member value (compact JSON for objects and
     * arrays). Members with a {@code null} value are skipped.
     */
    private static List<Header> toHeaders(@Nullable JsonElement headers) {
      List<Header> result = new ArrayList<>();
      if (headers == null || headers.isJsonNull()) {
        return result;
      }
      checkArgument(
          headers.isJsonObject(),
          "The '%s' member of the generated message must be a JSON object but was: %s",
          HEADERS_FIELD,
          headers);
      for (Map.Entry<String, JsonElement> header : headers.getAsJsonObject().entrySet()) {
        JsonElement value = header.getValue();
        if (value.isJsonNull()) {
          continue;
        }
        String text = value.isJsonPrimitive() ? value.getAsString() : value.toString();
        result.add(new RecordHeader(header.getKey(), text.getBytes(StandardCharsets.UTF_8)));
      }
      return result;
    }
  }

  /** Creates Kafka records with a JSON (UTF-8) encoded value. */
  @VisibleForTesting
  public static class JsonKafkaMessageFn extends BaseKafkaMessageFn {

    public JsonKafkaMessageFn(String topic) {
      super(topic);
    }

    @Override
    protected byte[] encodePayload(JsonElement payload) {
      return payload.toString().getBytes(StandardCharsets.UTF_8);
    }
  }

  /**
   * Creates Kafka records with a binary Avro encoded value. When the envelope format is used, the
   * Avro schema describes the {@code payload} member only.
   */
  @VisibleForTesting
  public static class AvroKafkaMessageFn extends BaseKafkaMessageFn {
    private final String avroSchemaLocation;
    private transient Schema schemaObj = null;
    private transient DatumReader<GenericRecord> genericDatumReader = null;
    private transient DatumWriter<GenericRecord> genericDatumWriter = null;

    public AvroKafkaMessageFn(String topic, String avroSchemaLocation) {
      super(topic);
      this.avroSchemaLocation = avroSchemaLocation;
    }

    @Setup
    public void setup() {
      this.schemaObj = SchemaUtils.getAvroSchema(avroSchemaLocation);
      this.genericDatumReader = new GenericDatumReader<>(schemaObj);
      this.genericDatumWriter = new GenericDatumWriter<>(schemaObj);
    }

    @Override
    protected byte[] encodePayload(JsonElement payload) throws IOException {
      // Decode the json value using json Decoder
      JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schemaObj, payload.toString());
      GenericRecord genericRecord = this.genericDatumReader.read(null, jsonDecoder);

      // Encode the json value into Avro Binary encoded value
      try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
        BinaryEncoder binaryEncoder =
            EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        genericDatumWriter.write(genericRecord, binaryEncoder);
        binaryEncoder.flush(); // writes Generic record content to byteArrayOutputStream
        return byteArrayOutputStream.toByteArray();
      }
    }
  }

  /**
   * A {@link PTransform} converts generatedMessages to either JSON encoded or Avro encoded Kafka
   * records (with optional key and headers) based on Pipeline options and publishes to Kafka.
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
      PCollection<ProducerRecord<String, byte[]>> kafkaRecords = null;
      StreamingDataGenerator.StreamingDataGeneratorOptions options = getPipelineOptions();
      String topic = options.getKafkaTopic();
      switch (options.getOutputType()) {
        case JSON:
          kafkaRecords =
              generatedMessages.apply(
                  "Generate JSON Kafka Messages",
                  ParDo.of(new StreamingDataGeneratorWriteToKafka.JsonKafkaMessageFn(topic)));
          break;
        case AVRO:
          checkNotNull(
              options.getAvroSchemaLocation(),
              String.format(
                  "Missing required value for --avroSchemaLocation for %s output type",
                  options.getOutputType()));
          kafkaRecords =
              generatedMessages.apply(
                  "Generate Avro Kafka Messages",
                  ParDo.of(
                      new StreamingDataGeneratorWriteToKafka.AvroKafkaMessageFn(
                          topic, options.getAvroSchemaLocation())));
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
      kafkaRecords.setCoder(recordCoder());

      // Record keys and headers can only be written through KafkaIO.WriteRecords, which lacks
      // withGCPApplicationDefaultCredentials(). Configure KafkaIO.Write as before and reuse its
      // fully configured WriteRecords transform.
      KafkaIO.Write<String, byte[]> writeTransform =
          KafkaIO.<String, byte[]>write()
              .withBootstrapServers(options.getBootstrapServer())
              .withTopic(topic)
              .withKeySerializer(StringSerializer.class)
              .withValueSerializer(ByteArraySerializer.class)
              .withProducerConfigUpdates(
                  Map.of(
                      "linger.ms", 100,
                      "batch.size", 64000));
      if (ManagedKafkaRegex.matcher(options.getBootstrapServer()).matches()) {
        writeTransform = writeTransform.withGCPApplicationDefaultCredentials();
      }
      return kafkaRecords.apply("writeSuccessMessages", writeTransform.getWriteRecordsTransform());
    }
  }
}
