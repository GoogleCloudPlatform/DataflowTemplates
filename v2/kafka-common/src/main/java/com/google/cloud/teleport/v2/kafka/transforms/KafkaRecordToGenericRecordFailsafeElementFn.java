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
package com.google.cloud.teleport.v2.kafka.transforms;

import com.google.cloud.teleport.v2.kafka.utils.FileAwareSchemaRegistryFactoryFn;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.values.TupleTag;

public class KafkaRecordToGenericRecordFailsafeElementFn
    extends DoFn<
        KafkaRecord<byte[], byte[]>, FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>>
    implements Serializable {

  private transient KafkaAvroDeserializer kafkaDeserializer;
  private transient BinaryAvroDeserializer binaryDeserializer;
  private transient SchemaRegistryClient schemaRegistryClient;

  // Flexible options for schema and encoding configuration
  private Schema schema;
  private final String topicName = "fake_topic";
  private String schemaRegistryConnectionUrl;
  private Map<String, Object> schemaRegistrySslConfig;
  private String messageFormat; // "AVRO_BINARY_ENCODING" or "AVRO_CONFLUENT_WIRE_FORMAT"
  private static final int DEFAULT_CACHE_CAPACITY = 1000;
  private BadRecordRouter badRecordRouter;
  private TupleTag<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>>
      successGenericRecordTag;

  // Constructors for different configurations
  public KafkaRecordToGenericRecordFailsafeElementFn(
      String schemaRegistryConnectionUrl,
      Map<String, Object> schemaRegistrySslConfig,
      BadRecordRouter badRecordRouter,
      TupleTag<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>>
          successGenericRecordTag) {
    this.schemaRegistryConnectionUrl = schemaRegistryConnectionUrl;
    this.schemaRegistrySslConfig = schemaRegistrySslConfig;
    this.badRecordRouter = badRecordRouter;
    this.successGenericRecordTag = successGenericRecordTag;
  }

  public KafkaRecordToGenericRecordFailsafeElementFn(
      Schema schema,
      String messageFormat,
      BadRecordRouter badRecordRouter,
      TupleTag<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>>
          successGenericRecordTag) {
    this.schema = schema;
    this.messageFormat = messageFormat;
    this.badRecordRouter = badRecordRouter;
    this.successGenericRecordTag = successGenericRecordTag;
  }

  @Setup
  public void setup() throws IOException, RestClientException {
    // Unified setup logic
    if (schemaRegistryConnectionUrl != null && !schemaRegistryConnectionUrl.isBlank()) {
      FileAwareSchemaRegistryFactoryFn processor = new FileAwareSchemaRegistryFactoryFn();
      this.schemaRegistryClient =
          new CachedSchemaRegistryClient(
              this.schemaRegistryConnectionUrl,
              DEFAULT_CACHE_CAPACITY,
              processor.apply(this.schemaRegistrySslConfig));
      this.kafkaDeserializer = new KafkaAvroDeserializer(this.schemaRegistryClient);
    } else if (schema != null && messageFormat.equals("AVRO_BINARY_ENCODING")) {
      this.binaryDeserializer = new BinaryAvroDeserializer(schema);
    } else if (schema != null && messageFormat.equals("AVRO_CONFLUENT_WIRE_FORMAT")) {
      this.schemaRegistryClient = new MockSchemaRegistryClient();
      this.schemaRegistryClient.register(topicName, schema, 1, 1);
      this.kafkaDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    } else {
      throw new IllegalArgumentException(
          "Either a Schema Registry URL, or an Avro schema with wire format is needed.");
    }
  }

  @ProcessElement
  public void processElement(ProcessContext context, MultiOutputReceiver o) throws Exception {
    KafkaRecord<byte[], byte[]> element = context.element();
    GenericRecord result = null;
    try {
      // Deserialization based on configuration
      if (messageFormat != null && messageFormat.equals("AVRO_BINARY_ENCODING")) {
        result =
            binaryDeserializer.deserialize(
                element.getTopic(), element.getHeaders(), element.getKV().getValue());
      } else { // Assume Confluent wire format or regular Avro with schema registry
        result =
            (GenericRecord)
                kafkaDeserializer.deserialize(
                    element.getTopic(), element.getHeaders(), element.getKV().getValue());
        // Output the failsafe element with the successful tag.
      }
      o.get(successGenericRecordTag).output(FailsafeElement.of(element, result));
    } catch (Exception e) {
      KafkaRecordCoder<byte[], byte[]> coder =
          KafkaRecordCoder.of(
              NullableCoder.of(ByteArrayCoder.of()), NullableCoder.of(ByteArrayCoder.of()));
      badRecordRouter.route(o, element, coder, e, e.toString());
    }
  }
}
