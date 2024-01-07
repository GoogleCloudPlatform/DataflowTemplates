/*
 * Copyright (C) 2023 Google LLC
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

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

public class SchemaKafkaAvroDeserializer extends AbstractKafkaAvroDeserializer
    implements Deserializer<GenericRecord> {
  private boolean isKey;
  private Schema schema;

  public SchemaKafkaAvroDeserializer() {}

  public SchemaKafkaAvroDeserializer(Schema schema) {
    this.schema = schema;
  }

  public SchemaKafkaAvroDeserializer(Schema schema, Map<String, ?> props) {
    // Here we use a mock schema since we always want to return the same schema instance.
    this.schemaRegistry = new MockSchemaRegistryClient();
    try {
      this.schemaRegistry.register("subject", schema);
    } catch (Exception e) {
      throw new RuntimeException("Error registering schema", e);
    }
  }

  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    this.configure(new KafkaAvroDeserializerConfig(configs));
  }

  public GenericRecord deserialize(String s, byte[] bytes) {
    return (GenericRecord) this.deserialize(bytes);
  }

  public GenericRecord deserialize(String s, byte[] bytes, Schema readerSchema) {
    return (GenericRecord) this.deserialize(bytes, readerSchema);
  }

  public void close() {}
}
