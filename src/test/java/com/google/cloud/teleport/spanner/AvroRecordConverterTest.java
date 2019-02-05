/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.spanner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.junit.Test;

/** Unit tests for {@link AvroRecordConverter}. */
public class AvroRecordConverterTest {

  @Test
  public void integerArray() {
    Schema schema =
        SchemaBuilder.record("record").fields()
            .requiredLong("id")
            .name("arrayofint")
                .type()
                .nullable()
                .array()
                .items()
                    .longType()
                .noDefault()
            .endRecord();

    List<Integer> intArray = Arrays.asList(1, 2, 3, null);
    List<Long> longArray = Arrays.asList(1L, 2L, 3L, null);
    List<Utf8> stringArray = Arrays.asList(new Utf8("1"), new Utf8("2"), new Utf8("3"), null);

    // Null field
    GenericRecord avroRecord =
        new GenericRecordBuilder(schema).set("id", 0).set("arrayofint", null).build();
    Optional<List<Long>> result =
        AvroRecordConverter.readInt64Array(avroRecord, Schema.Type.INT, "arrayofint");
    assertFalse(result.isPresent());

    // Convert from int to Int64.
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set("arrayofint", intArray).build();
    result = AvroRecordConverter.readInt64Array(avroRecord, Schema.Type.INT, "arrayofint");
    assertArrayEquals(longArray.toArray(), result.get().toArray());

    // Convert from long to Int64.
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set("arrayofint", longArray).build();
    result = AvroRecordConverter.readInt64Array(avroRecord, Schema.Type.LONG, "arrayofint");
    assertArrayEquals(longArray.toArray(), result.get().toArray());

    // Convert from String to Int64.
    avroRecord =
        new GenericRecordBuilder(schema).set("id", 0).set("arrayofint", stringArray).build();
    result = AvroRecordConverter.readInt64Array(avroRecord, Schema.Type.STRING, "arrayofint");
    assertArrayEquals(longArray.toArray(), result.get().toArray());
  }
}
