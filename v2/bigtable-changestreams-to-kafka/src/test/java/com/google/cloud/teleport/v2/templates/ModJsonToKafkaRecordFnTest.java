/*
 * Copyright (C) 2026 Google LLC
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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.bigtable.ChangelogEntryMessage;
import com.google.cloud.teleport.bigtable.ChangelogEntryMessageBase64;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters.MessageFormatConstants;
import com.google.cloud.teleport.v2.templates.BigtableChangeStreamsToKafka.ModJsonToKafkaRecordFn;
import com.google.cloud.teleport.v2.templates.model.BigtableSource;
import com.google.cloud.teleport.v2.templates.model.Mod;
import com.google.cloud.teleport.v2.templates.model.TestChangeStreamMutation;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ModJsonToKafkaRecordFn}. */
@RunWith(JUnit4.class)
public class ModJsonToKafkaRecordFnTest {
  private static final String TOPIC = "topic";
  private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://mod-json-to-kafka-record-fn-test";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testConvertsModJsonToBinaryAvroKafkaRecord() throws Exception {
    PCollectionTuple output =
        convert(setCellModJson(), MessageFormatConstants.AVRO_BINARY_ENCODING, new HashMap<>());

    PAssert.that(output.get(ModJsonToKafkaRecordFn.KAFKA_RECORDS))
        .satisfies(
            records -> {
              KV<byte[], byte[]> record = Iterables.getOnlyElement(records);
              assertThat(record.getKey()).isEqualTo(bytes("row"));
              GenericRecord value =
                  decode(
                      ChangelogEntryMessage.getClassSchema(),
                      DecoderFactory.get().binaryDecoder(record.getValue(), null));
              assertSetCellWithByteFields(value);
              return null;
            });
    PAssert.that(output.get(ModJsonToKafkaRecordFn.FAILED_MOD_JSON)).empty();
    pipeline.run();
  }

  @Test
  public void testConvertsModJsonToJsonKafkaRecordWithBase64EncodedFields() throws Exception {
    PCollectionTuple output =
        convert(setCellModJson(), MessageFormatConstants.JSON, new HashMap<>());

    PAssert.that(output.get(ModJsonToKafkaRecordFn.KAFKA_RECORDS))
        .satisfies(
            records -> {
              KV<byte[], byte[]> record = Iterables.getOnlyElement(records);
              assertThat(record.getKey()).isEqualTo(bytes("row"));
              GenericRecord value;
              try {
                value =
                    decode(
                        ChangelogEntryMessageBase64.getClassSchema(),
                        DecoderFactory.get()
                            .jsonDecoder(
                                ChangelogEntryMessageBase64.getClassSchema(),
                                new String(record.getValue(), StandardCharsets.UTF_8)));
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
              assertThat(value.get("modType").toString()).isEqualTo("SET_CELL");
              assertThat(value.get("columnFamily").toString()).isEqualTo("cf");
              assertThat(value.get("rowKey").toString()).isEqualTo(base64("row"));
              assertThat(value.get("column").toString()).isEqualTo(base64("q"));
              assertThat(value.get("value").toString()).isEqualTo(base64("value"));
              return null;
            });
    PAssert.that(output.get(ModJsonToKafkaRecordFn.FAILED_MOD_JSON)).empty();
    pipeline.run();
  }

  @Test
  public void testConvertsModJsonToConfluentWireFormatKafkaRecord() throws Exception {
    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
    PCollectionTuple output =
        convert(setCellModJson(), MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT, config);

    PAssert.that(output.get(ModJsonToKafkaRecordFn.KAFKA_RECORDS))
        .satisfies(
            records -> {
              KV<byte[], byte[]> record = Iterables.getOnlyElement(records);
              assertThat(record.getKey()).isEqualTo(bytes("row"));
              try (KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer()) {
                deserializer.configure(config, false);
                GenericRecord value =
                    (GenericRecord) deserializer.deserialize(TOPIC, record.getValue());
                assertSetCellWithByteFields(value);
              }
              return null;
            });
    PAssert.that(output.get(ModJsonToKafkaRecordFn.FAILED_MOD_JSON)).empty();
    pipeline.run();
  }

  @Test
  public void testSendsInvalidModJsonToFailedOutput() {
    String invalidModJson = "not a mod";
    PCollectionTuple output =
        convert(invalidModJson, MessageFormatConstants.AVRO_BINARY_ENCODING, new HashMap<>());

    PAssert.that(output.get(ModJsonToKafkaRecordFn.KAFKA_RECORDS)).empty();
    PAssert.that(output.get(ModJsonToKafkaRecordFn.FAILED_MOD_JSON))
        .satisfies(
            failures -> {
              FailsafeElement<String, String> failure = Iterables.getOnlyElement(failures);
              assertThat(failure.getOriginalPayload()).isEqualTo(invalidModJson);
              assertThat(failure.getPayload()).isEqualTo(invalidModJson);
              assertThat(failure.getErrorMessage()).isNotEmpty();
              assertThat(failure.getStacktrace()).isNotEmpty();
              return null;
            });
    pipeline.run();
  }

  /**
   * Applies {@link ModJsonToKafkaRecordFn} configured for the given message format to the given Mod
   * JSON. The coder of the Kafka records output is deliberately left for Beam to infer, as in the
   * template.
   */
  private PCollectionTuple convert(
      String modJson, String messageFormat, Map<String, Object> valueSerializerConfig) {
    PCollectionTuple output =
        pipeline
            .apply(
                Create.of(FailsafeElement.of(modJson, modJson))
                    .withCoder(BigtableChangeStreamsToKafka.FAILSAFE_ELEMENT_CODER))
            .apply(
                ParDo.of(new ModJsonToKafkaRecordFn(TOPIC, messageFormat, valueSerializerConfig))
                    .withOutputTags(
                        ModJsonToKafkaRecordFn.KAFKA_RECORDS,
                        TupleTagList.of(ModJsonToKafkaRecordFn.FAILED_MOD_JSON)));
    output
        .get(ModJsonToKafkaRecordFn.FAILED_MOD_JSON)
        .setCoder(BigtableChangeStreamsToKafka.FAILSAFE_ELEMENT_CODER);
    return output;
  }

  private static void assertSetCellWithByteFields(GenericRecord value) {
    assertThat(value.get("modType").toString()).isEqualTo("SET_CELL");
    assertThat(value.get("columnFamily").toString()).isEqualTo("cf");
    assertThat(value.get("rowKey")).isEqualTo(ByteBuffer.wrap(bytes("row")));
    assertThat(value.get("column")).isEqualTo(ByteBuffer.wrap(bytes("q")));
    assertThat(value.get("value")).isEqualTo(ByteBuffer.wrap(bytes("value")));
  }

  private static GenericRecord decode(Schema schema, Decoder decoder) {
    try {
      return new GenericDatumReader<GenericRecord>(schema).read(null, decoder);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String setCellModJson() throws Exception {
    SetCell setCell =
        SetCell.create("cf", ByteString.copyFromUtf8("q"), 1000L, ByteString.copyFromUtf8("value"));
    BigtableSource source = new BigtableSource("instance", "table", "UTF-8", "", "");
    return new Mod(source, new TestChangeStreamMutation(setCell), setCell).toJson();
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static String base64(String value) {
    return Base64.getEncoder().encodeToString(bytes(value));
  }
}
