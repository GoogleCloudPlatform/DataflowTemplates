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
package com.google.cloud.teleport.v2.templates;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.teleport.v2.transforms.StreamingDataGeneratorWriteToBigQuery;
import com.google.cloud.teleport.v2.transforms.StreamingDataGeneratorWriteToGcs;
import com.google.cloud.teleport.v2.transforms.StreamingDataGeneratorWriteToKafka;
import com.google.cloud.teleport.v2.transforms.StreamingDataGeneratorWriteToPubSub;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test cases for the {@link StreamingDataGenerator} class. */
@RunWith(JUnit4.class)
public class StreamingDataGeneratorTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String KAFKA_TOPIC = "test-topic";

  /** Tests Creation of PubSub Sink with missing PubSub topic name. */
  @Test
  public void testCreatingPubSubSink_withTopicNameMissing_throwsException() {
    StreamingDataGenerator.StreamingDataGeneratorOptions options =
        getPipelineOptions(new String[] {});
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        String.format(
            "Missing required value --topic for %s sink type", options.getSinkType().name()));
    StreamingDataGenerator.createSink(options, getSimpleSchema());
  }

  /** Tests Creation of PubSub Sink based on Pipeline options. */
  @Test
  public void testCreatingPubSubSink_returnsValidSinkType() {
    StreamingDataGenerator.StreamingDataGeneratorOptions options =
        getPipelineOptions(new String[] {"--topic=projects/demoproject/topics/testtopic"});
    assertTrue(
        StreamingDataGenerator.createSink(options, getSimpleSchema())
            instanceof StreamingDataGeneratorWriteToPubSub.Writer);
  }

  /** Tests Creation of BigQuery Sink based on Pipeline options. */
  @Test
  public void testCreatingBQSink_returnsValidSinkType() {
    StreamingDataGenerator.StreamingDataGeneratorOptions options =
        getPipelineOptions(
            new String[] {"--sinkType=" + StreamingDataGenerator.SinkType.BIGQUERY.name()});
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        String.format(
            "Missing required value --outputTableSpec in format <project>:<dataset>.<table_name>"
                + " for %s sink type",
            options.getSinkType().name()));
    StreamingDataGenerator.createSink(options, getSimpleSchema());

    options =
        getPipelineOptions(
            new String[] {
              "--sinkType=" + StreamingDataGenerator.SinkType.BIGQUERY.name(),
              "--outputTableSpec=testproject:testds:demotable"
            });
    assertTrue(
        StreamingDataGenerator.createSink(options, getSimpleSchema())
            instanceof StreamingDataGeneratorWriteToBigQuery);
  }

  /** Tests Creation of GCS Sink based on Pipeline options. */
  @Test
  public void testCreatingGCSSink_returnsValidSinkType() {
    StreamingDataGenerator.StreamingDataGeneratorOptions options =
        getPipelineOptions(
            new String[] {"--sinkType=" + StreamingDataGenerator.SinkType.GCS.name()});
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        String.format(
            "Missing required value --outputDirectory in format gs:// for %s sink type",
            options.getSinkType().name()));
    StreamingDataGenerator.createSink(options, getSimpleSchema());

    options =
        getPipelineOptions(
            new String[] {
              "--sinkType=" + StreamingDataGenerator.SinkType.BIGQUERY.name(),
              "--outputDirectory=gs://demobucket/testprefix/"
            });
    assertTrue(
        StreamingDataGenerator.createSink(options, getSimpleSchema())
            instanceof StreamingDataGeneratorWriteToGcs);
  }

  /** Tests generation of fake Json data message without attributes. */
  @Test
  public void testMessageGenerator_returnsFakeMessage() throws IOException {
    // Arrange
    String schema = getSimpleSchema();

    // Act
    PCollection<byte[]> results = generateJsonMessage(schema);

    // Assert
    PAssert.that(results)
        .satisfies(
            input -> {
              PubsubMessage message = new PubsubMessage(input.iterator().next(), new HashMap<>());

              assertNotNull(message.getPayload());
              assertTrue(message.getAttributeMap().isEmpty());

              return null;
            });

    pipeline.run();
  }

  /** Tests generation of fake Json data message with attributes. */
  @Test
  public void testJsonMessageGenerator_WithAttributes_returnsFakeMessageContainingAttributes()
      throws IOException {
    // Arrange
    String schema =
        "{\n"
            + "\t\"payload\": {\n"
            + "\t\t\"eventId\": \"{{put(\"eventId\",uuid())}}\",\n"
            + "\t\t\"eventTime\": {{put(\"eventTime\", timestamp())}},\n"
            + "\t\t\"username\": \"{{put(\"username \", username())}}\",\n"
            + "\t\t\"ipv4\": \"{{ipv4()}}\",\n"
            + "\t\t\"country\": \"{{country()}}\",\n"
            + "\t\t\"score\": {{ integer(0, 100) }},\n"
            + "\t\t\"teamavg\": {{put(\"teamavg\",float(100000, 10000000,\"%.7f\"))}},\n"
            + "\t\t\"completed\": {{bool()}}\n"
            + "\t},\n"
            + "\t\"attributes\": {\n"
            + "\t\t\"eventId\": \"{{get(\"eventId\")}}\",\n"
            + "\t\t\"eventTime\": {{get(\"eventTime\")}},\n"
            + "\t\t\"appId\": {{ integer(1, 10) }},\n"
            + "\t\t\"teamavg\": {{get(\"teamavg\")}}\n"
            + "\t}\n"
            + "}";

    // Act
    PCollection<PubsubMessage> results =
        pipeline
            .apply("CreateInput", Create.of(0L))
            .apply(
                "GenerateMessage", ParDo.of(new StreamingDataGenerator.MessageGeneratorFn(schema)))
            .apply(
                "Generate JSON PubSub Messages",
                ParDo.of(new StreamingDataGeneratorWriteToPubSub.JsonPubSubMessageFn(schema)));

    // Assert
    PAssert.that(results)
        .satisfies(
            input -> {
              PubsubMessage message = input.iterator().next();

              assertNotNull(message);
              assertNotNull(message.getPayload());
              assertEquals(4, message.getAttributeMap().size());

              return null;
            });

    pipeline.run();
  }

  /**
   * Tests generation of fake Json data message with attributes using schema with space indentation.
   */
  @Test
  public void
      testJsonMessageGenerator_WithAttributes_WithSpaceIndentation_returnsFakeMessageContainingAttributes()
          throws IOException {
    // Arrange
    String schema =
        "{\n"
            + "  \"payload\": {\n"
            + "    \"eventId\": \"{{put(\"eventId\",uuid())}}\",\n"
            + "    \"eventTime\": {{put(\"eventTime\", timestamp())}},\n"
            + "    \"username\": \"{{put(\"username \", username())}}\",\n"
            + "    \"ipv4\": \"{{ipv4()}}\",\n"
            + "    \"country\": \"{{country()}}\",\n"
            + "    \"score\": {{ integer(0, 100) }},\n"
            + "    \"teamavg\": {{put(\"teamavg\",float(100000, 10000000,\"%.7f\"))}},\n"
            + "   \"completed\": {{bool()}}\n"
            + "  },\n"
            + "  \"attributes\": {\n"
            + "    \"eventId\": \"{{get(\"eventId\")}}\",\n"
            + "    \"eventTime\": {{get(\"eventTime\")}},\n"
            + "    \"appId\": {{ integer(1, 10) }},\n"
            + "    \"teamavg\": {{get(\"teamavg\")}}\n"
            + "  }\n"
            + "}";
    File file = tempFolder.newFile();
    writeToFile(file.getAbsolutePath(), schema);

    // Act
    PCollection<PubsubMessage> results =
        pipeline
            .apply("CreateInput", Create.of(0L))
            .apply(
                "GenerateMessage", ParDo.of(new StreamingDataGenerator.MessageGeneratorFn(schema)))
            .apply(
                "Generate JSON PubSub Messages",
                ParDo.of(new StreamingDataGeneratorWriteToPubSub.JsonPubSubMessageFn(schema)));

    // Assert
    PAssert.that(results)
        .satisfies(
            input -> {
              PubsubMessage message = input.iterator().next();

              assertNotNull(message);
              assertNotNull(message.getPayload());
              assertEquals(4, message.getAttributeMap().size());

              return null;
            });

    pipeline.run();
  }

  /**
   * Verifies the generation of messages does not fail with invalid schema (but not recommended).
   */
  @Test
  public void testMessageGenerator_WithInvalidSchema_returnsSameSchema() throws IOException {
    // Arrange
    String schema = "{\"name: \"Invalid\"";

    // Act
    PCollection<byte[]> results = generateJsonMessage(schema);

    // Assert
    PAssert.that(results)
        .satisfies(
            input -> {
              PubsubMessage message = new PubsubMessage(input.iterator().next(), new HashMap<>());

              assertEquals(schema, new String(message.getPayload()));

              return null;
            });

    pipeline.run();
  }

  /** Tests generation of fake Avro data message. */
  @Test
  public void testAvroMessageGenerator_returnsAvroMessage() throws IOException {
    // Arrange
    String schema = getSimpleSchema();
    String avroSchema = getSimpleAvroSchema();
    File avroSchemaFile = tempFolder.newFile();
    writeToFile(avroSchemaFile.getAbsolutePath(), avroSchema);

    // Act
    PCollection<PubsubMessage> results =
        pipeline
            .apply("CreateInput", Create.of(0L))
            .apply(
                "GenerateMessage", ParDo.of(new StreamingDataGenerator.MessageGeneratorFn(schema)))
            .apply(
                "Generate JSON PubSub Messages",
                ParDo.of(
                    new StreamingDataGeneratorWriteToPubSub.AvroPubSubMessageFn(
                        schema, avroSchemaFile.getAbsolutePath())));

    // Assert
    PAssert.that(results)
        .satisfies(
            input -> {
              PubsubMessage message = input.iterator().next();
              try {
                GenericRecord record =
                    getGenericRecord(message.getPayload(), new Schema.Parser().parse(avroSchema));
                assertNotNull(message);
                assertEquals("John", record.get("username").toString());
                assertNotNull(message.getAttributeMap());
              } catch (IOException e) {
                fail("Exception while decoding avro message:" + e.getMessage());
              }
              return null;
            });

    pipeline.run();
  }

  /** Tests Creation of Kafka Sink with missing bootstrap server. */
  @Test
  public void testCreatingKafkaSink_withBootstrapServerMissing_throwsException() {
    StreamingDataGenerator.StreamingDataGeneratorOptions options =
        getPipelineOptions(
            new String[] {
              "--sinkType=" + StreamingDataGenerator.SinkType.KAFKA.name(),
              "--kafkaTopic=" + KAFKA_TOPIC
            });
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        String.format(
            "Missing required value --bootstrapServer for %s sink type",
            options.getSinkType().name()));
    StreamingDataGenerator.createSink(options, getSimpleSchema());
  }

  /** Tests Creation of Kafka Sink based on Pipeline options. */
  @Test
  public void testCreatingKafkaSink_returnsValidSinkType() {
    StreamingDataGenerator.StreamingDataGeneratorOptions options =
        getPipelineOptions(
            new String[] {
              "--sinkType=" + StreamingDataGenerator.SinkType.KAFKA.name(),
              "--bootstrapServer=localhost:9092",
              "--kafkaTopic=" + KAFKA_TOPIC
            });
    assertTrue(
        StreamingDataGenerator.createSink(options, getSimpleSchema())
            instanceof StreamingDataGeneratorWriteToKafka.Writer);
  }

  /** Tests generation of Kafka records from a schema without envelope: no key, no headers. */
  @Test
  public void testJsonKafkaMessageFn_withoutEnvelope_returnsKeylessRecord() {
    // Arrange
    String schema = getSimpleSchema();

    // Act
    PCollection<ProducerRecord<String, byte[]>> results = generateJsonKafkaRecords(schema);

    // Assert
    PAssert.that(results)
        .satisfies(
            input -> {
              ProducerRecord<String, byte[]> record = input.iterator().next();

              assertEquals(KAFKA_TOPIC, record.topic());
              assertNull(record.key());
              assertEquals(0, record.headers().toArray().length);
              JsonObject payload = parseJson(record.value());
              assertEquals(4, payload.size());
              assertEquals("John", payload.get("username").getAsString());

              return null;
            });

    pipeline.run();
  }

  /** Tests generation of Kafka records from an envelope schema: key and headers are populated. */
  @Test
  public void testJsonKafkaMessageFn_withEnvelope_returnsRecordWithKeyAndHeaders() {
    // Arrange
    String schema =
        "{\n"
            + "  \"payload\": {\n"
            + "    \"id\": \"{{put(\"id\", uuid())}}\",\n"
            + "    \"eventTime\": {{timestamp()}},\n"
            + "    \"username\": \"John\",\n"
            + "    \"score\": {{integer(0,100)}}\n"
            + "  },\n"
            + "  \"key\": \"{{get(\"id\")}}\",\n"
            + "  \"headers\": {\n"
            + "    \"source\": \"streaming-data-generator\",\n"
            + "    \"id\": \"{{get(\"id\")}}\",\n"
            + "    \"version\": 2,\n"
            + "    \"tags\": [\"a\", \"b\"],\n"
            + "    \"ignored\": null\n"
            + "  }\n"
            + "}";

    // Act
    PCollection<ProducerRecord<String, byte[]>> results = generateJsonKafkaRecords(schema);

    // Assert
    PAssert.that(results)
        .satisfies(
            input -> {
              ProducerRecord<String, byte[]> record = input.iterator().next();

              JsonObject payload = parseJson(record.value());
              assertEquals(4, payload.size());
              assertEquals("John", payload.get("username").getAsString());
              assertEquals(payload.get("id").getAsString(), record.key());
              assertEquals(4, record.headers().toArray().length);
              assertEquals("streaming-data-generator", headerValue(record, "source"));
              assertEquals(record.key(), headerValue(record, "id"));
              assertEquals("2", headerValue(record, "version"));
              assertEquals("[\"a\",\"b\"]", headerValue(record, "tags"));
              assertNull(record.headers().lastHeader("ignored"));

              return null;
            });

    pipeline.run();
  }

  /** Tests generation of Kafka records from an envelope schema with only a key. */
  @Test
  public void testJsonKafkaMessageFn_withEnvelopeKeyOnly_returnsRecordWithKey() {
    // Arrange: an unquoted (numeric) key is used in its textual form
    String schema = "{\"payload\": " + getSimpleSchema() + ", \"key\": 7}";

    // Act
    PCollection<ProducerRecord<String, byte[]>> results = generateJsonKafkaRecords(schema);

    // Assert
    PAssert.that(results)
        .satisfies(
            input -> {
              ProducerRecord<String, byte[]> record = input.iterator().next();

              assertEquals("7", record.key());
              assertEquals(0, record.headers().toArray().length);
              assertEquals("John", parseJson(record.value()).get("username").getAsString());

              return null;
            });

    pipeline.run();
  }

  /** Tests that a message with a payload member but other non-envelope members is kept as is. */
  @Test
  public void testJsonKafkaMessageFn_withPayloadFieldButNoEnvelope_returnsWholeMessage() {
    // Arrange
    String schema = "{\"payload\": \"{{uuid()}}\", \"key\": \"abc\", \"other\": 1}";

    // Act
    PCollection<ProducerRecord<String, byte[]>> results = generateJsonKafkaRecords(schema);

    // Assert
    PAssert.that(results)
        .satisfies(
            input -> {
              ProducerRecord<String, byte[]> record = input.iterator().next();

              assertNull(record.key());
              JsonObject payload = parseJson(record.value());
              assertEquals(3, payload.size());
              assertEquals("abc", payload.get("key").getAsString());

              return null;
            });

    pipeline.run();
  }

  /** Tests that an envelope whose headers member is not a JSON object is rejected. */
  @Test
  public void testJsonKafkaMessageFn_withInvalidHeaders_throwsException() {
    // Arrange
    String schema =
        "{\"payload\": " + getSimpleSchema() + ", \"headers\": [\"not\", \"an\", \"object\"]}";
    generateJsonKafkaRecords(schema);

    // Assert
    expectedException.expect(Pipeline.PipelineExecutionException.class);
    expectedException.expectCause(instanceOf(IllegalArgumentException.class));
    expectedException.expectMessage("must be a JSON object");

    // Act
    pipeline.run();
  }

  /** Tests generation of Avro Kafka records from an envelope schema. */
  @Test
  public void testAvroKafkaMessageFn_withEnvelope_returnsAvroRecordWithKeyAndHeaders()
      throws IOException {
    // Arrange
    String schema =
        "{\n"
            + "  \"payload\": {\n"
            + "    \"id\": \"{{put(\"id\", uuid())}}\",\n"
            + "    \"eventTime\": {{timestamp()}},\n"
            + "    \"username\": \"John\",\n"
            + "    \"score\": {{integer(0,100)}}\n"
            + "  },\n"
            + "  \"key\": \"{{get(\"id\")}}\",\n"
            + "  \"headers\": {\"encoding\": \"avro\"}\n"
            + "}";
    String avroSchema = getSimpleAvroSchema();
    File avroSchemaFile = tempFolder.newFile();
    writeToFile(avroSchemaFile.getAbsolutePath(), avroSchema);

    // Act
    PCollection<ProducerRecord<String, byte[]>> results =
        pipeline
            .apply("CreateInput", Create.of(0L))
            .apply(
                "GenerateMessage", ParDo.of(new StreamingDataGenerator.MessageGeneratorFn(schema)))
            .apply(
                "Generate Avro Kafka Messages",
                ParDo.of(
                    new StreamingDataGeneratorWriteToKafka.AvroKafkaMessageFn(
                        KAFKA_TOPIC, avroSchemaFile.getAbsolutePath())))
            .setCoder(StreamingDataGeneratorWriteToKafka.recordCoder());

    // Assert
    PAssert.that(results)
        .satisfies(
            input -> {
              ProducerRecord<String, byte[]> record = input.iterator().next();
              try {
                GenericRecord payload =
                    getGenericRecord(record.value(), new Schema.Parser().parse(avroSchema));
                assertEquals("John", payload.get("username").toString());
                assertEquals(payload.get("id").toString(), record.key());
                assertEquals("avro", headerValue(record, "encoding"));
              } catch (IOException e) {
                fail("Exception while decoding avro message:" + e.getMessage());
              }
              return null;
            });

    pipeline.run();
  }

  /** Helper method to return message schema. */
  private static String getSimpleSchema() {
    return "{"
        + "\"id\": \"{{uuid()}}\", "
        + "\"eventTime\": {{timestamp()}}, "
        + "\"username\": \"John\", "
        + "\"score\": {{integer(0,100)}}"
        + "}";
  }

  /** Helper method to return the Avro schema matching {@link #getSimpleSchema()}. */
  private static String getSimpleAvroSchema() {
    return "{\n"
        + "  \"type\" : \"record\",\n"
        + "  \"name\" : \"SampleEvent\",\n"
        + "  \"namespace\" : \"com.streamingdatagenerator.avro\",\n"
        + "  \"fields\" : [\n"
        + "    {\n"
        + "      \"name\": \"id\",\n"
        + "      \"type\": \"string\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"eventTime\",\n"
        + "      \"type\": \"long\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"username\",\n"
        + "      \"type\": \"string\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"score\",\n"
        + "      \"type\": \"int\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";
  }

  /** Helper method to run pipeline to generate Json Message. */
  private PCollection<byte[]> generateJsonMessage(String schema) {
    PCollection<byte[]> results =
        pipeline
            .apply("CreateInput", Create.of(0L))
            .apply(
                "GenerateMessage", ParDo.of(new StreamingDataGenerator.MessageGeneratorFn(schema)));
    return results;
  }

  /** Helper method to run pipeline to generate JSON encoded Kafka records. */
  private PCollection<ProducerRecord<String, byte[]>> generateJsonKafkaRecords(String schema) {
    return pipeline
        .apply("CreateInput", Create.of(0L))
        .apply("GenerateMessage", ParDo.of(new StreamingDataGenerator.MessageGeneratorFn(schema)))
        .apply(
            "Generate JSON Kafka Messages",
            ParDo.of(new StreamingDataGeneratorWriteToKafka.JsonKafkaMessageFn(KAFKA_TOPIC)))
        .setCoder(StreamingDataGeneratorWriteToKafka.recordCoder());
  }

  /** Helper method to parse a UTF-8 encoded JSON object. */
  private static JsonObject parseJson(byte[] payload) {
    return JsonParser.parseString(new String(payload, StandardCharsets.UTF_8)).getAsJsonObject();
  }

  /** Helper method to read a Kafka record header as UTF-8 string. */
  private static String headerValue(ProducerRecord<String, byte[]> record, String name) {
    Header header = record.headers().lastHeader(name);
    assertNotNull("Missing header " + name, header);
    return new String(header.value(), StandardCharsets.UTF_8);
  }

  /**
   * Helper method to deserialize Binary Avro encoded data.
   *
   * @param data Binary Avro encoded data.
   * @param schema Avro Schema object
   * @return Deserialized Generic record.
   * @throws IOException If an error occurs while reading encoded data.
   */
  private static GenericRecord getGenericRecord(byte[] data, Schema schema) throws IOException {
    DatumReader<GenericRecord> genericDatumReader = new GenericDatumReader<>(schema);
    Decoder binarydecoder =
        DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(data), null);
    return genericDatumReader.read(null, binarydecoder);
  }

  /**
   * Helper method providing pipeline options.
   *
   * @param args list of pipeline arguments.
   */
  private static StreamingDataGenerator.StreamingDataGeneratorOptions getPipelineOptions(
      String[] args) {
    return PipelineOptionsFactory.fromArgs(args)
        .as(StreamingDataGenerator.StreamingDataGeneratorOptions.class);
  }

  /**
   * Helper to generate files for testing.
   *
   * @param filePath The path to the file to write.
   * @param fileContents The content to write.
   * @return The file written.
   * @throws IOException If an error occurs while creating or writing the file.
   */
  private static ResourceId writeToFile(String filePath, String fileContents) throws IOException {

    ResourceId resourceId = FileSystems.matchNewResource(filePath, false);

    // Write the file contents to the channel and close.
    try (ReadableByteChannel readChannel =
        Channels.newChannel(new ByteArrayInputStream(fileContents.getBytes()))) {
      try (WritableByteChannel writeChannel = FileSystems.create(resourceId, MimeTypes.TEXT)) {
        ByteStreams.copy(readChannel, writeChannel);
      }
    }

    return resourceId;
  }
}
