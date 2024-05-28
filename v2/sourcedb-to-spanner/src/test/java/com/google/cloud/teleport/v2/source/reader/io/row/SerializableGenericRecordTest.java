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
package com.google.cloud.teleport.v2.source.reader.io.row;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaTestUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link SerializableGenericRecord}. */
@RunWith(MockitoJUnitRunner.class)
public class SerializableGenericRecordTest {

  @Test
  public void testGetterSetter() {
    final String testTableFirst = "testTable-1";
    final String testTableSecond = "testTable-2";
    var schema = SchemaTestUtils.generateTestTableSchema(testTableFirst);
    var record =
        new GenericRecordBuilder(schema.avroSchema())
            .set("read_timestamp", 1L)
            .set(
                "payload",
                (new GenericRecordBuilder(schema.getAvroPayload()))
                    .set("firstName", "abc")
                    .set("lastName", "def")
                    .build())
            .build();
    SerializableGenericRecord serializableGenericRecord = new SerializableGenericRecord(record);
    assertThat(serializableGenericRecord.getRecord()).isEqualTo(record);
  }

  @Test
  public void testSerde() throws IOException, ClassNotFoundException {
    var schema = SchemaTestUtils.generateTestTableSchema("testTable");
    GenericRecord genericRecord =
        (new GenericRecordBuilder(schema.avroSchema()))
            .set("read_timestamp", 1L)
            .set(
                "payload",
                (new GenericRecordBuilder(schema.getAvroPayload()))
                    .set("firstName", "abc")
                    .set("lastName", "def")
                    .build())
            .build();
    SerializableGenericRecord record = new SerializableGenericRecord(genericRecord);

    /* Serialize */
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream outputStream = new ObjectOutputStream(bos);
    outputStream.writeObject(record);
    outputStream.flush();

    /* Deserialize */
    ByteArrayInputStream ios = new ByteArrayInputStream(bos.toByteArray());
    ObjectInputStream inputStream = new ObjectInputStream(ios);
    SerializableGenericRecord deSerializedRecord =
        (SerializableGenericRecord) inputStream.readObject();

    assertThat(record).isEqualTo(deSerializedRecord);
  }

  @Test
  public void testEquality() {
    var schema = SchemaTestUtils.generateTestTableSchema("testTable");
    GenericRecord genericRecordLeft =
        (new GenericRecordBuilder(schema.avroSchema()))
            .set("read_timestamp", 1L)
            .set(
                "payload",
                (new GenericRecordBuilder(schema.getAvroPayload()))
                    .set("firstName", "abc")
                    .set("lastName", "def")
                    .build())
            .build();
    GenericRecord genericRecordRight =
        (new GenericRecordBuilder(schema.avroSchema()))
            .set("read_timestamp", genericRecordLeft.get("read_timestamp"))
            .set("payload", genericRecordLeft.get("payload"))
            .build();
    SerializableGenericRecord recordLeft = new SerializableGenericRecord(genericRecordLeft);
    SerializableGenericRecord recordRight = new SerializableGenericRecord(genericRecordRight);
    assertThat(recordLeft).isEqualTo(recordRight);
    assertThat(recordLeft.hashCode()).isEqualTo(recordRight.hashCode());
    recordRight.getRecord().put(0, 4L);
    assertThat(recordLeft).isEqualTo(recordLeft);
    assertThat(recordLeft).isNotEqualTo(recordRight);
    assertThat(recordLeft.hashCode()).isNotEqualTo(recordRight.hashCode());
    assertThat(recordLeft).isNotEqualTo(123L);
  }
}
