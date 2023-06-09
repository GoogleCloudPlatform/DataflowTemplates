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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
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
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;

/**
 * Writes fake messages to Google Cloud PubSub message. If attributes exists inside message
 * attributes are stripped out of the message and loaded into attributes map. If attributes does not
 * exists then an creates an empty map.
 */
public final class StreamingDataGeneratorWriteToPubSub {

  private StreamingDataGeneratorWriteToPubSub() {}

  /** Wrapper holding fake json message as Google Cloud PubSub Message. */
  @AutoValue
  abstract static class FakePubSubMessage {
    abstract byte[] getPayload();

    abstract ImmutableMap<String, String> getAttributes();

    public static FakePubSubMessage create(
        byte[] payload, ImmutableMap<String, String> attributes) {
      return new AutoValue_StreamingDataGeneratorWriteToPubSub_FakePubSubMessage(
          payload, attributes);
    }
  }

  /**
   * Loads schema, checks for presence of attributes pattern. If provided schema has attributes,
   * populates the attributes inside PubSub Message.
   */
  private abstract static class FakePubSubMessageFn extends DoFn<byte[], PubsubMessage> {

    private ObjectMapper mapper;
    private TypeReference<HashMap<String, String>> hashMapRef;
    private static final Pattern ATTRIBUTE_PATTERN =
        Pattern.compile("^\\{\\s*\"?payload\"?:.+\"?attributes\"?:.+");
    private static final int PUBSUB_ATTRIBUTE_VALUE_MAX_LENGTH = 1024;
    private boolean hasAttributes;

    protected void initialize(String schema) {
      hasAttributes =
          ATTRIBUTE_PATTERN
              .matcher(schema.replace("\n", "").replace("\r", "").replace("\t", ""))
              .find();
      if (hasAttributes) {
        mapper = new ObjectMapper();
        hashMapRef = new TypeReference<HashMap<String, String>>() {};
      }
    }

    /**
     * Processes the fake message in bytes to a canonical {@link FakePubSubMessage} instance. If
     * user provided fake message schema has attributes support then attributes are stripped out of
     * the fake message and loaded into attributes map. If provided schema only has payload without
     * attributes then message is created with an empty attributes map.
     */
    protected FakePubSubMessage getFakePubSubMessage(byte[] message)
        throws JsonSyntaxException, IOException {
      if (hasAttributes) {
        JsonObject jsonObject =
            JsonParser.parseString(new String(message, StandardCharsets.UTF_8)).getAsJsonObject();
        byte[] payload = jsonObject.get("payload").toString().getBytes();
        Map<String, String> attributeValues =
            mapper.readValue(jsonObject.get("attributes").toString(), hashMapRef);
        ImmutableMap.Builder<String, String> attributes =
            new ImmutableMap.Builder<String, String>();
        attributeValues.forEach(
            (key, value) -> {
              attributes.put(
                  key,
                  value.substring(0, Math.min(value.length(), PUBSUB_ATTRIBUTE_VALUE_MAX_LENGTH)));
            });
        return FakePubSubMessage.create(payload, attributes.build());
      }
      return FakePubSubMessage.create(message, ImmutableMap.of());
    }
  }

  /**
   * Creates PubsubMessage with JSON (UTF8) encoded Payload. PubsubMessage attributes are populated
   * if user provided message schema contains attributes property.
   */
  @VisibleForTesting
  public static class JsonPubSubMessageFn extends FakePubSubMessageFn {
    private final String schema;

    public JsonPubSubMessageFn(String schema) {
      this.schema = schema;
    }

    @Setup
    public void setup() throws IOException {
      initialize(schema);
    }

    @ProcessElement
    public void processElement(
        @Element byte[] fakeMessage,
        @Timestamp Instant timestamp,
        OutputReceiver<PubsubMessage> receiver,
        ProcessContext context)
        throws IOException {

      FakePubSubMessage fakePubSubMessage = getFakePubSubMessage(fakeMessage);
      receiver.output(
          new PubsubMessage(fakePubSubMessage.getPayload(), fakePubSubMessage.getAttributes()));
    }
  }

  /**
   * Creates PubsubMessage with Binary Avro encoded Payload.PubsubMessage attributes are populated
   * if user provided message schema contains attributes property.
   */
  @VisibleForTesting
  public static class AvroPubSubMessageFn extends FakePubSubMessageFn {
    private final String schema;
    private final String avroSchemaLocation;
    private Schema schemaObj = null;
    private DatumReader<GenericRecord> genericDatumReader = null;
    private DatumWriter<GenericRecord> genericDatumWriter = null;

    public AvroPubSubMessageFn(String schema, String avroSchemaLocation) {
      this.schema = schema;
      this.avroSchemaLocation = avroSchemaLocation;
    }

    @Setup
    public void setup() {
      initialize(schema);
      this.schemaObj = SchemaUtils.getAvroSchema(avroSchemaLocation);
      this.genericDatumReader = new GenericDatumReader<>(schemaObj);
      this.genericDatumWriter = new GenericDatumWriter<>(schemaObj);
    }

    @ProcessElement
    public void processElement(
        @Element byte[] fakeMessage,
        @Timestamp Instant timestamp,
        OutputReceiver<PubsubMessage> receiver,
        ProcessContext context)
        throws IOException {

      FakePubSubMessage fakePubSubMessage = getFakePubSubMessage(fakeMessage);
      receiver.output(
          new PubsubMessage(
              utf8ToBinary(fakePubSubMessage.getPayload()), fakePubSubMessage.getAttributes()));
    }

    private byte[] utf8ToBinary(byte[] payload) throws IOException {
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
        return byteArrayOutputStream.toByteArray();
      }
    }
  }

  /**
   * A {@link PTransform} converts generatedMessages to either JSON encoded or Avro encoded PubSub
   * messages based on Pipeline options and publishes to Google Cloud PubSub.
   */
  @AutoValue
  public abstract static class Writer extends PTransform<PCollection<byte[]>, PDone> {

    abstract StreamingDataGenerator.StreamingDataGeneratorOptions getPipelineOptions();

    abstract String getSchema();

    public static Builder builder(
        StreamingDataGenerator.StreamingDataGeneratorOptions options, String schema) {
      return new AutoValue_StreamingDataGeneratorWriteToPubSub_Writer.Builder()
          .setPipelineOptions(options)
          .setSchema(schema);
    }

    /** Builder for {@link StreamingDataGeneratorWriteToPubSub.Writer}. */
    @AutoValue.Builder
    public abstract static class Builder {
      abstract Builder setPipelineOptions(
          StreamingDataGenerator.StreamingDataGeneratorOptions value);

      abstract Builder setSchema(String schema);

      public abstract Writer build();
    }

    @Override
    public PDone expand(PCollection<byte[]> generatedMessages) {
      PCollection<PubsubMessage> pubsubMessages = null;
      StreamingDataGenerator.StreamingDataGeneratorOptions options = getPipelineOptions();
      switch (options.getOutputType()) {
        case JSON:
          pubsubMessages =
              generatedMessages.apply(
                  "Generate JSON PubSub Messages", ParDo.of(new JsonPubSubMessageFn(getSchema())));
          break;
        case AVRO:
          checkNotNull(
              options.getAvroSchemaLocation(),
              String.format(
                  "Missing required value for --avroSchemaLocation for %s output type",
                  options.getOutputType()));
          pubsubMessages =
              generatedMessages.apply(
                  "Generate Avro PubSub Messages",
                  ParDo.of(new AvroPubSubMessageFn(getSchema(), options.getAvroSchemaLocation())));
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

      return pubsubMessages.apply(
          "Write messages", PubsubIO.writeMessages().to(getPipelineOptions().getTopic()));
    }
  }
}
