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
package com.google.cloud.teleport.v2.transforms;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.utils.StructHelper.ValueHelper.NullTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.extensions.avro.io.AvroSource.DatumReaderFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(Enclosed.class)
public final class AvroToStructFnTest {

  @RunWith(JUnit4.class)
  public static final class UnitTests {

    @Test
    public void testApply_castsAllPrimitiveDataTypes() {
      Schema inputSchema =
          Schema.createRecord(
              ImmutableList.<Field>builder()
                  .add(new Field("boolTrue", Schema.create(Schema.Type.BOOLEAN)))
                  .add(new Field("boolFalse", Schema.create(Schema.Type.BOOLEAN)))
                  .add(new Field("bytes", Schema.create(Schema.Type.BYTES)))
                  .add(new Field("double", Schema.create(Schema.Type.DOUBLE)))
                  .add(new Field("fixed", Schema.createFixed("fixed", "doc", "namespace", 4)))
                  .add(new Field("float", Schema.create(Schema.Type.FLOAT)))
                  .add(new Field("int", Schema.create(Schema.Type.INT)))
                  .add(new Field("long", Schema.create(Schema.Type.LONG)))
                  .add(new Field("string", Schema.create(Schema.Type.STRING)))
                  .build());
      GenericRecord input =
          new GenericRecordBuilder(inputSchema)
              .set("boolTrue", Boolean.TRUE)
              .set("boolFalse", Boolean.FALSE)
              .set("bytes", ByteArray.fromBase64("Tml0bw=="))
              .set("double", 7.0)
              .set("fixed", ByteArray.fromBase64("Tml0bw=="))
              .set("float", 7.0F)
              .set("int", 7)
              .set("long", 7L)
              .set("string", "text")
              .build();
      Struct expectedOutput =
          Struct.newBuilder()
              .set("boolTrue")
              .to(Boolean.TRUE)
              .set("boolFalse")
              .to(Boolean.FALSE)
              .set("bytes")
              .to(ByteArray.fromBase64("Tml0bw=="))
              .set("double")
              .to(7.0)
              .set("fixed")
              .to(ByteArray.fromBase64("Tml0bw=="))
              .set("float")
              .to(7.0F)
              .set("int")
              .to(7)
              .set("long")
              .to(7L)
              .set("string")
              .to("text")
              .build();

      Struct output = AvroToStructFn.create().apply(input);

      assertThat(output).isEqualTo(expectedOutput);
    }

    @Test
    public void testApply_castsAllLogicalDataTypes() {
      Schema inputSchema =
          Schema.createRecord(
              ImmutableList.<Field>builder()
                  .add(
                      new Field(
                          "date",
                          new Schema.Parser()
                              .parse("{\"type\": \"int\", \"logicalType\": \"date\"}")))
                  .add(
                      new Field(
                          "decimal",
                          new Schema.Parser()
                              .parse(
                                  "{\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 7, \"scale\": 6}")))
                  .add(
                      new Field(
                          "localTimestampMillis",
                          new Schema.Parser()
                              .parse(
                                  "{\"type\": \"long\", \"logicalType\": \"local-timestamp-millis\"}")))
                  .add(
                      new Field(
                          "timestampMillis",
                          new Schema.Parser()
                              .parse(
                                  "{\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}")))
                  .add(
                      new Field(
                          "localTimestampMicros",
                          new Schema.Parser()
                              .parse(
                                  "{\"type\": \"long\", \"logicalType\": \"local-timestamp-micros\"}")))
                  .add(
                      new Field(
                          "timestampMicros",
                          new Schema.Parser()
                              .parse(
                                  "{\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}")))
                  .build());
      GenericRecord input =
          new GenericRecordBuilder(inputSchema)
              .set("date", 7499L) // Days since epoch.
              .set("decimal", ByteArray.copyFrom(new Conversions.DecimalConversion().toBytes(
                  BigDecimal.valueOf(3141592L, 6),
                  new Schema.Parser().parse("{\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 7, \"scale\": 6}"),
                  LogicalTypes.fromSchema(new Schema.Parser().parse("{\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 7, \"scale\": 6}"))))) // 3141592
              .set("localTimestampMillis", 647917261000L)
              .set("timestampMillis", 647917261000L)
              .set("localTimestampMicros", 647917261000000L)
              .set("timestampMicros", 647917261000000L)
              .build();
      Struct expectedOutput =
          Struct.newBuilder()
              .set("date")
              .to(Date.fromYearMonthDay(1990, 7, 14))
              .set("decimal")
              .to(BigDecimal.valueOf(3141592, 6))
              .set("localTimestampMillis")
              .to(Timestamp.ofTimeMicroseconds(647917261000000L))
              .set("timestampMillis")
              .to(Timestamp.ofTimeMicroseconds(647917261000000L))
              .set("localTimestampMicros")
              .to(Timestamp.ofTimeMicroseconds(647917261000000L))
              .set("timestampMicros")
              .to(Timestamp.ofTimeMicroseconds(647917261000000L))
              .build();

      Struct output = AvroToStructFn.create().apply(input);

      assertThat(output).isEqualTo(expectedOutput);
    }

    @Test
    public void testApply_castsAllNullableDataTypes() {
      Schema inputSchema =
          Schema.createRecord(
              ImmutableList.<Field>builder()
                  .add(
                      new Field(
                          "nullableNullBoolean",
                          Schema.createUnion(
                              Schema.create(Schema.Type.BOOLEAN), Schema.create(Schema.Type.NULL))))
                  .add(
                      new Field(
                          "nullableTrueBoolean",
                          Schema.createUnion(
                              Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BOOLEAN))))
                  .add(
                      new Field(
                          "nullableFalseBoolean",
                          Schema.createUnion(
                              Schema.create(Schema.Type.BOOLEAN), Schema.create(Schema.Type.NULL))))
                  .add(
                      new Field(
                          "nullableNullDouble",
                          Schema.createUnion(
                              Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE))))
                  .add(
                      new Field(
                          "nullableDouble",
                          Schema.createUnion(
                              Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.DOUBLE))))
                  .add(
                      new Field(
                          "nullableFloat",
                          Schema.createUnion(
                              Schema.create(Schema.Type.FLOAT), Schema.create(Schema.Type.NULL))))
                  .add(
                      new Field(
                          "nullableNullFloat",
                          Schema.createUnion(
                              Schema.create(Schema.Type.FLOAT), Schema.create(Schema.Type.NULL))))
                  .add(
                      new Field(
                          "nullableNullInt",
                          Schema.createUnion(
                              Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT))))
                  .add(
                      new Field(
                          "nullableInt",
                          Schema.createUnion(
                              Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT))))
                  .add(
                      new Field(
                          "nullableNullLong",
                          Schema.createUnion(
                              Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.NULL))))
                  .add(
                      new Field(
                          "nullableLong",
                          Schema.createUnion(
                              Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.NULL))))
                  .add(
                      new Field(
                          "nullableNullString",
                          Schema.createUnion(
                              Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))))
                  .add(
                      new Field(
                          "nullableString",
                          Schema.createUnion(
                              Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING))))
                  .build());
      GenericRecord input =
          new GenericRecordBuilder(inputSchema)
              .set("nullableNullBoolean", NullTypes.NULL_BOOLEAN)
              .set("nullableTrueBoolean", Boolean.TRUE)
              .set("nullableFalseBoolean", Boolean.FALSE)
              .set("nullableNullDouble", NullTypes.NULL_FLOAT64)
              .set("nullableDouble", 7.0)
              .set("nullableFloat", NullTypes.NULL_FLOAT32)
              .set("nullableNullFloat", 7.0F)
              .set("nullableNullInt", NullTypes.NULL_INT64)
              .set("nullableInt", 7)
              .set("nullableNullLong", NullTypes.NULL_INT64)
              .set("nullableLong", 7L)
              .set("nullableNullString", NullTypes.NULL_STRING)
              .set("nullableString", "text")
              .build();
      Struct expectedOutput =
          Struct.newBuilder()
              .set("nullableNullBoolean")
              .to(NullTypes.NULL_BOOLEAN)
              .set("nullableTrueBoolean")
              .to(Boolean.TRUE)
              .set("nullableFalseBoolean")
              .to(Boolean.FALSE)
              .set("nullableNullDouble")
              .to(NullTypes.NULL_FLOAT64)
              .set("nullableDouble")
              .to(7.0)
              .set("nullableFloat")
              .to(NullTypes.NULL_FLOAT32)
              .set("nullableNullFloat")
              .to(7.0F)
              .set("nullableNullInt")
              .to(NullTypes.NULL_INT64)
              .set("nullableInt")
              .to(7)
              .set("nullableNullLong")
              .to(NullTypes.NULL_INT64)
              .set("nullableLong")
              .to(7L)
              .set("nullableNullString")
              .to(NullTypes.NULL_STRING)
              .set("nullableString")
              .to("text")
              .build();

      Struct output = AvroToStructFn.create().apply(input);

      assertThat(output).isEqualTo(expectedOutput);
    }

    @Test
    public void testApply_throwsForUnsupportedDataTypes() {
      Schema inputSchema =
          Schema.createRecord(
              ImmutableList.<Field>builder()
                  .add(new Field("array", Schema.createArray(Schema.create(Schema.Type.STRING))))
                  .build());
      GenericRecord input =
          new GenericRecordBuilder(inputSchema)
              .set("array", ImmutableList.of("arrayValue"))
              .build();

      UnsupportedOperationException thrown =
          assertThrows(
              UnsupportedOperationException.class, () -> AvroToStructFn.create().apply(input));

      assertThat(thrown).hasMessageThat().contains("Avro field type ARRAY is not supported.");
    }

    @Test
    public void testApply_throwsForUnsupportedUnionTypes_tripleUnion() {
      Schema inputSchema =
          Schema.createRecord(
              ImmutableList.<Field>builder()
                  .add(
                      new Field(
                          "tripleUnion",
                          Schema.createUnion(
                              Schema.create(Schema.Type.BOOLEAN),
                              Schema.create(Schema.Type.NULL),
                              Schema.create(Schema.Type.LONG))))
                  .build());
      GenericRecord input =
          new GenericRecordBuilder(inputSchema).set("tripleUnion", Boolean.TRUE).build();

      UnsupportedOperationException thrown =
          assertThrows(
              UnsupportedOperationException.class, () -> AvroToStructFn.create().apply(input));

      assertThat(thrown)
          .hasMessageThat()
          .contains(
              "UNION is only supported for nullable fields. Got: [\"boolean\", \"null\", \"long\"]");
    }

    @Test
    public void testApply_throwsForUnsupportedUnionTypes_doubleUnionNonNull() {
      Schema inputSchema =
          Schema.createRecord(
              ImmutableList.<Field>builder()
                  .add(
                      new Field(
                          "nonNullableUnion",
                          Schema.createUnion(
                              Schema.create(Schema.Type.BOOLEAN),
                              Schema.create(Schema.Type.DOUBLE))))
                  .build());
      GenericRecord input =
          new GenericRecordBuilder(inputSchema).set("nonNullableUnion", Boolean.TRUE).build();

      UnsupportedOperationException thrown =
          assertThrows(
              UnsupportedOperationException.class, () -> AvroToStructFn.create().apply(input));

      assertThat(thrown)
          .hasMessageThat()
          .contains("UNION is only supported for nullable fields. Got: [\"boolean\", \"double\"].");
    }
  }

  @RunWith(JUnit4.class)
  public static final class ITTests {
    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    private static final String RESOURCES_DIR = "AvroToStructFnTest/";

    @Test
    public void testIntegration_withAvroIO() {
      String testFile = Resources.getResource(RESOURCES_DIR + "test_avro_file.avro").getPath();
      Struct expectedRow1 =
          Struct.newBuilder()
              .set("bool")
              .to(Boolean.TRUE)
              .set("int")
              .to(7)
              .set("string")
              .to("abc")
              .build();
      Struct expectedRow2 =
          Struct.newBuilder()
              .set("bool")
              .to(Boolean.FALSE)
              .set("int")
              .to(9)
              .set("string")
              .to("xyz")
              .build();

      PCollection<Struct> output =
          pipeline.apply(AvroIO.parseGenericRecords(AvroToStructFn.create()).from(testFile));

      PAssert.that(output).containsInAnyOrder(expectedRow1, expectedRow2);
      pipeline.run().waitUntilFinish();
    }
  }
}
