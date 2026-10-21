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
package com.google.cloud.teleport.v2.templates.model;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.Range.TimestampRange;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.v2.kafka.transforms.BinaryAvroSerializer;
import com.google.cloud.teleport.v2.kafka.transforms.JsonAvroSerializer;
import com.google.cloud.teleport.v2.templates.schemautils.KafkaUtils;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ModSerializationTest {
  private static final String TOPIC = "topic";

  @Parameter(0)
  public String name;

  @Parameter(1)
  public Entry entry;

  @Parameter(2)
  public boolean json;

  @Parameters(name = "{0}")
  public static List<Object[]> parameters() {
    ByteString qualifier = ByteString.copyFromUtf8("q");
    List<Object[]> parameters = new ArrayList<>();
    for (boolean json : new boolean[] {false, true}) {
      String format = json ? "JSON" : "AVRO";
      parameters.add(
          new Object[] {
            "SetCell-" + format,
            SetCell.create("cf", qualifier, 1000L, ByteString.copyFromUtf8("value")),
            json
          });
      parameters.add(
          new Object[] {
            "DeleteCells-" + format,
            DeleteCells.create("cf", qualifier, TimestampRange.create(1000L, 2000L)),
            json
          });
      parameters.add(new Object[] {"DeleteFamily-" + format, DeleteFamily.create("cf"), json});
    }
    return parameters;
  }

  @Test
  public void testEntryCanBeWrittenToKafka() throws Exception {
    BigtableSource source = new BigtableSource("instance", "table", "UTF-8", "", "");
    TestChangeStreamMutation mutation = new TestChangeStreamMutation(entry);
    Mod mod;
    if (entry instanceof SetCell setCell) {
      mod = new Mod(source, mutation, setCell);
    } else if (entry instanceof DeleteCells deleteCells) {
      mod = new Mod(source, mutation, deleteCells);
    } else if (entry instanceof DeleteFamily deleteFamily) {
      mod = new Mod(source, mutation, deleteFamily);
    } else {
      throw new IllegalArgumentException("Unsupported Entry kind");
    }

    String changeJson = Mod.fromJson(mod.toJson()).getChangeJson();
    KafkaUtils kafkaUtils = new KafkaUtils("UTF-8");
    // Simpler version of the record and serializer selection logic from WriteToKafkaFn.
    // KafkaAvroSerializer is missing from this test, because it differs from
    // BinaryAvroSerializer mostly by using external schema  registry - it is tested
    // in the integration test instead.
    ProducerRecord<byte[], GenericRecord> record =
        json
            ? kafkaUtils.getProducerRecordWithBase64EncodedFields(changeJson, TOPIC)
            : kafkaUtils.getProducerRecord(changeJson, TOPIC);

    byte[] serialized;
    try (Serializer<GenericRecord> serializer =
        json ? new JsonAvroSerializer() : new BinaryAvroSerializer()) {
      serialized = serializer.serialize(TOPIC, record.value());
    }
    assertThat(serialized).isNotEmpty();
  }
}
