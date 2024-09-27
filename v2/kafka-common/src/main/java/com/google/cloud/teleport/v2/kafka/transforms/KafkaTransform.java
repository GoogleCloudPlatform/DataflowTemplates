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
package com.google.cloud.teleport.v2.kafka.transforms;

import com.google.cloud.teleport.v2.kafka.utils.FileAwareConsumerFactoryFn;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.kafka.DeserializerProvider;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Different transformations over the processed data in the pipeline. */
public class KafkaTransform {

  /**
   * Configures Kafka consumer that reads String.
   *
   * @param bootstrapServers Kafka servers to read from
   * @param topicsList Kafka topics to read from
   * @param config configuration for the Kafka consumer
   * @return PCollection of Kafka Key & Value Pair deserialized in string format
   */
  public static KafkaIO.Read<String, String> readStringFromKafka(
      String bootstrapServers,
      List<String> topicsList,
      Map<String, Object> config,
      @Nullable Map<String, Object> sslConfig,
      boolean enableCommitOffsets) {
    KafkaIO.Read<String, String> kafkaRecords =
        KafkaIO.<String, String>read()
            .withBootstrapServers(bootstrapServers)
            .withTopics(topicsList)
            .withKeyDeserializerAndCoder(
                StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
            .withValueDeserializerAndCoder(
                StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
            .withConsumerConfigUpdates(config)
            .withConsumerFactoryFn(new FileAwareConsumerFactoryFn());
    if (enableCommitOffsets) {
      kafkaRecords = kafkaRecords.commitOffsetsInFinalize();
    }
    return kafkaRecords;
    // topic, partition, source offset
  }

  /**
   * Configures Kafka consumer that reads Avro GenericRecord.
   *
   * @param bootstrapServers Kafka servers to read from
   * @param topicsList Kafka topics to read from
   * @param config configuration for the Kafka consumer
   * @return PCollection of Kafka Key & Value Pair deserialized in string format
   */
  public static PTransform<PBegin, PCollection<KV<byte[], GenericRecord>>> readAvroFromKafka(
      String bootstrapServers,
      List<String> topicsList,
      Map<String, Object> config,
      String avroSchema,
      @Nullable Map<String, Object> sslConfig) {
    KafkaIO.Read<byte[], GenericRecord> kafkaRecords =
        KafkaIO.<byte[], GenericRecord>read()
            .withBootstrapServers(bootstrapServers)
            .withTopics(topicsList)
            .withKeyDeserializerAndCoder(
                ByteArrayDeserializer.class, NullableCoder.of(ByteArrayCoder.of()))
            .withValueDeserializer(new KafkaSchemaDeserializerProvider(avroSchema))
            .withConsumerConfigUpdates(config)
            .withConsumerFactoryFn(new FileAwareConsumerFactoryFn());
    return kafkaRecords.withoutMetadata();
  }

  /**
   * Configures Kafka consumer that reads bytes.
   *
   * @param bootstrapServers Kafka servers to read from
   * @param topicsList Kafka topics to read from
   * @param config configuration for the Kafka consumer
   * @return PCollection of Kafka Key & Value Pair deserialized in string format
   */
  public static KafkaIO.Read<byte[], byte[]> readBytesFromKafka(
      String bootstrapServers,
      List<String> topicsList,
      Map<String, Object> config,
      boolean enableCommitOffsets) {

    KafkaIO.Read<byte[], byte[]> kafkaRecords =
        KafkaIO.<byte[], byte[]>read()
            .withBootstrapServers(bootstrapServers)
            .withTopics(topicsList)
            .withKeyDeserializerAndCoder(
                ByteArrayDeserializer.class, NullableCoder.of(ByteArrayCoder.of()))
            .withValueDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of())
            .withConsumerConfigUpdates(config)
            .withConsumerFactoryFn(new FileAwareConsumerFactoryFn());

    if (enableCommitOffsets) {
      kafkaRecords = kafkaRecords.commitOffsetsInFinalize();
    }
    return kafkaRecords;
  }

  /**
   * The {@link MessageToFailsafeElementFn} wraps an Kafka Message with the {@link FailsafeElement}
   * class so errors can be recovered from and the original message can be output to a error records
   * table.
   */
  public static class MessageToFailsafeElementFn
      extends DoFn<KV<String, String>, FailsafeElement<KV<String, String>, String>> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      KV<String, String> message = context.element();
      context.output(FailsafeElement.of(message, message.getValue()));
    }
  }

  static class KafkaSchemaDeserializerProvider implements DeserializerProvider<GenericRecord> {

    private String avroSchemaPath;
    private transient Schema avroSchema;

    public KafkaSchemaDeserializerProvider(String avroSchemaPath) {
      this.avroSchemaPath = avroSchemaPath;
    }

    @Override
    public Deserializer<GenericRecord> getDeserializer(Map<String, ?> configs, boolean isKey) {
      return new SchemaKafkaAvroDeserializer(getAvroSchema(), configs);
    }

    @Override
    public Coder<GenericRecord> getCoder(CoderRegistry coderRegistry) {
      return NullableCoder.of(AvroCoder.of(getAvroSchema()));
    }

    protected synchronized Schema getAvroSchema() {
      if (this.avroSchema == null) {
        this.avroSchema = SchemaUtils.getAvroSchema(avroSchemaPath);
      }
      return this.avroSchema;
    }
  }
}
