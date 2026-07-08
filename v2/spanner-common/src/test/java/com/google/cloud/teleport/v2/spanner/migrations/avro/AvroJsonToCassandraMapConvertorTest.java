/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.spanner.migrations.avro;

import static com.google.cloud.teleport.v2.spanner.migrations.avro.AvroTestingHelper.createIntervalNanosRecord;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.spanner.ddl.annotations.cassandra.CassandraAnnotations;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.AvroTypeConvertorException;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.List;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kerby.util.Hex;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AvroJsonToCassandraMapConvertorTest {

  private static final Schema JSON_TYPE =
      new LogicalType(GenericRecordTypeConvertor.CustomAvroTypes.JSON)
          .addToSchema(SchemaBuilder.builder().stringType());

  Object getMapJson(Object key, Object value) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.add(
        (key == null) ? "null" : key.toString(),
        new JsonPrimitive((value == null) ? "null" : value.toString()));

    Schema payloadSchema =
        SchemaBuilder.record("payload")
            .fields()
            .name("mapCol")
            .type(JSON_TYPE)
            .noDefault()
            .endRecord();

    GenericRecord payload =
        new GenericRecordBuilder(payloadSchema).set("mapCol", jsonObject.toString()).build();
    return payload.get("mapCol");
  }

  @Test
  public void testHandleJsonToMapBasic() {
    assertThat(
            AvroJsonToCassandraMapConvertor.handleJsonToMap(
                getMapJson(42, 42),
                CassandraAnnotations.fromColumnOptions(
                    List.of("cassandra_type='map<int, int>'"), "testCol"),
                "testCol",
                JSON_TYPE))
        .isEqualTo("{\"42\":\"42\"}");

    /* Nulls */
    assertThat(
            AvroJsonToCassandraMapConvertor.handleJsonToMap(
                null,
                CassandraAnnotations.fromColumnOptions(
                    List.of("cassandra_type='map<int, int>'"), "testCol"),
                "testCol",
                JSON_TYPE))
        .isEqualTo("NULL");
    assertThat(
            AvroJsonToCassandraMapConvertor.handleJsonToMap(
                getMapJson(null, null),
                CassandraAnnotations.fromColumnOptions(
                    List.of("cassandra_type='map<sting, string>'"), "testCol"),
                "testCol",
                JSON_TYPE))
        .isEqualTo("{\"null\":\"null\"}");

    /* Pass through for not maps */
    assertThat(
            AvroJsonToCassandraMapConvertor.handleJsonToMap(
                42,
                CassandraAnnotations.fromColumnOptions(List.of("cassandra_type='int'"), "testCol"),
                "testCol",
                JSON_TYPE))
        .isEqualTo("42");
  }

  @Test
  public void testTypeMappings() {
    /* Null */
    assertThat(AvroJsonToCassandraMapConvertor.mapKeyOrValue(null, "BOOLEAN")).isEqualTo("NULL");
    /* Boolean */
    assertThat(AvroJsonToCassandraMapConvertor.mapKeyOrValue(true, "BOOLEAN")).isEqualTo("true");
    /* Case mixed-up on purpose for test */
    assertThat(AvroJsonToCassandraMapConvertor.mapKeyOrValue("FaLSE", "BOOLEAN"))
        .isEqualTo("false");
    /* Blob */
    assertThat(
            AvroJsonToCassandraMapConvertor.mapKeyOrValue(Hex.encode("Google".getBytes()), "BLOB"))
        .isEqualTo("R29vZ2xl");
    /* Float, Double */
    assertThat(AvroJsonToCassandraMapConvertor.mapKeyOrValue(3.14, "FLOAT")).isEqualTo("3.14");
    assertThat(AvroJsonToCassandraMapConvertor.mapKeyOrValue(Float.NaN, "FLOAT")).isEqualTo("NaN");
    assertThat(AvroJsonToCassandraMapConvertor.mapKeyOrValue(Float.POSITIVE_INFINITY, "FLOAT"))
        .isEqualTo("Infinity");
    assertThat(AvroJsonToCassandraMapConvertor.mapKeyOrValue(Float.NEGATIVE_INFINITY, "FLOAT"))
        .isEqualTo("-Infinity");
    assertThat(AvroJsonToCassandraMapConvertor.mapKeyOrValue(4.28, "DOUBLE")).isEqualTo("4.28");
    /* Date */
    assertThat(AvroJsonToCassandraMapConvertor.mapKeyOrValue(10, "DATE")).isEqualTo("1970-01-11");
    assertThat(AvroJsonToCassandraMapConvertor.mapKeyOrValue(-10, "DATE")).isEqualTo("1969-12-22");
    /* Time */
    assertThat(AvroJsonToCassandraMapConvertor.mapKeyOrValue(68400000000000L, "TIME"))
        .isEqualTo("68400000000000");
    /* Timestamp */
    assertThat(AvroJsonToCassandraMapConvertor.mapKeyOrValue(1742372396581000L, "TIMESTAMP"))
        .isEqualTo("2025-03-19T08:19:56.581Z");

    /* Duration */
    assertThat(
            AvroJsonToCassandraMapConvertor.mapKeyOrValue(
                createIntervalNanosRecord(1L, 2L, 3L, 4L, 5L, 6L, 789000000L).toString(),
                "DURATION"))
        .isEqualTo("P1Y2M3DT4H5M6.789S");
    assertThat(
            AvroJsonToCassandraMapConvertor.mapKeyOrValue(
                createIntervalNanosRecord(0L, 0L, 0L, 0L, 0L, 0L, 0L).toString(), "DURATION"))
        .isEqualTo("P0D");
    assertThat(
            AvroJsonToCassandraMapConvertor.mapKeyOrValue(
                createIntervalNanosRecord(null, 0L, 0L, 0L, 0L, 0L, 0L).toString(), "DURATION"))
        .isEqualTo("P0D");
    /* UUID */
    /* Case of input mixed-up for the purpose of test */
    assertThat(
            AvroJsonToCassandraMapConvertor.mapKeyOrValue(
                "B430e5b6-4f06-4dd9-85f7-df10b77fc382", "UUID"))
        .isEqualTo("b430e5b6-4f06-4dd9-85f7-df10b77fc382");
    /* Inet */
    assertThat(AvroJsonToCassandraMapConvertor.mapKeyOrValue("10.37.41.15", "inet"))
        .isEqualTo("10.37.41.15");

    assertThrows(
        AvroTypeConvertorException.class,
        () -> {
          assertThat(AvroJsonToCassandraMapConvertor.mapKeyOrValue("XYZ", "BLOB"))
              .isEqualTo("R29vZ2xl");
        });
  }
}
