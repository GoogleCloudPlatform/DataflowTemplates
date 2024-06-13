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

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.coders.GenericRecordCoder;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * The {@link AvroDynamicTransform} class is a {@link PTransform} which transforms incoming Kafka
 * Message objects into GenericRecords using Schema Registry.
 */
public class AvroDynamicTransform
    extends PTransform<
        PCollection<KafkaRecord<byte[], byte[]>>,
        PCollection<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>>> {
  private String schemaRegistryConnectionUrl;

  private AvroDynamicTransform(String schemaRegistryConnectionUrl) {
    this.schemaRegistryConnectionUrl = schemaRegistryConnectionUrl;
  }

  public static AvroDynamicTransform of(String schemaRegistryConnectionUrl) {
    return new AvroDynamicTransform(schemaRegistryConnectionUrl);
  }

  public PCollection<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>> expand(
      PCollection<KafkaRecord<byte[], byte[]>> kafkaRecords) {
    PCollection<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>> genericRecords;
    genericRecords =
        kafkaRecords
            .apply(
                "ConvertKafkaRecordsToGenericRecordsWrappedinFailsafeElement",
                ParDo.of(
                    new KafkaRecordToGenericRecordFailsafeElementFn(
                        this.schemaRegistryConnectionUrl)))
            .setCoder(
                FailsafeElementCoder.of(
                    KafkaRecordCoder.of(NullableCoder.of(ByteArrayCoder.of()), ByteArrayCoder.of()),
                    GenericRecordCoder.of()));
    return genericRecords;
  }
}
