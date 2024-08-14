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

import com.google.cloud.spanner.Struct;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import kotlin.reflect.jvm.internal.calls.CallerImpl.Constructor;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public final class AvroToStructFnTest {

  @RunWith(Parameterized.class)
  public static final class ParameterizedTests {

    private final String testName;

    private final ImmutableList<GenericRecord> input;
    private final ImmutableList<Struct> expectedOutputs;
    public ParameterizedTests(
        String testName,
        ImmutableList<GenericRecord> input,
        ImmutableList<Struct> expectedOutputs) {
      this.testName = testName;
      this.input = input;
      this.expectedOutputs = expectedOutputs;
    }

    @Test
    public void apply_success() {
      List<Struct> output =
          input.stream().map(AvroToStructFn.create()::apply).collect(Collectors.toList());
      assertThat(output).containsExactlyElementsIn(expectedOutputs);
    }

    @Parameters(name = "{0}")
    public static ImmutableList<Object[]> testParameters() {
      return ImmutableList.<Object[]>builder()
          .add(
              new Object[] {
                  /*testName=*/"castsBooleans",
                  /*input=*/ImmutableList.of(
                    new GenericRecordBuilder(
                            Schema.createRecord(
                                ImmutableList.<Field>builder()
                                    .add(
                                        new Field(
                                            "fieldNameTrue", Schema.create(Schema.Type.BOOLEAN)))
                                    .add(
                                        new Field(
                                            "fieldNameFalse", Schema.create(Schema.Type.BOOLEAN)))
                                    .build()))
                        .set("fieldNameTrue", Boolean.TRUE)
                        .set("fieldNameFalse", Boolean.FALSE)
                        .build()),
                /*expectedOutput=*/ImmutableList.of(
                    Struct.newBuilder()
                        .set("fieldNameTrue")
                        .to(Boolean.TRUE)
                        .set("fieldNameFalse")
                        .to(Boolean.FALSE)
                        .build())
              })
          .add(
              new Object[] {
                /*testName=*/"castsDoubles",
                ImmutableList.of(
                    new GenericRecordBuilder(
                            Schema.createRecord(
                                ImmutableList.<Field>builder()
                                    .add(
                                        new Field("fieldDouble", Schema.create(Schema.Type.DOUBLE)))
                                    .build()))
                        .set("fieldDouble", 7.0)
                        .build()),
                ImmutableList.of(Struct.newBuilder().set("fieldDouble").to(7.0).build())
              })
          .build();
    }
  }

  @RunWith(JUnit4.class)
  public static final class UnitTests {
    // TODO: migrate to parameterized testing.

    @Test
    public void testApply_castsBooleans() {
      String fieldNameTrue = "fieldTrue";
      String fieldNameFalse = "fieldFalse";
      Schema inputSchema =
          Schema.createRecord(
              ImmutableList.<Field>builder()
                  .add(new Field(fieldNameTrue, Schema.create(Schema.Type.BOOLEAN)))
                  .add(new Field(fieldNameFalse, Schema.create(Schema.Type.BOOLEAN)))
                  .build());
      GenericRecord input =
          new GenericRecordBuilder(inputSchema)
              .set(fieldNameTrue, Boolean.TRUE)
              .set(fieldNameFalse, Boolean.FALSE)
              .build();
      Struct expectedOutput =
          Struct.newBuilder()
              .set(fieldNameTrue)
              .to(Boolean.TRUE)
              .set(fieldNameFalse)
              .to(Boolean.FALSE)
              .build();

      Struct output = AvroToStructFn.create().apply(input);

      assertThat(output).isEqualTo(expectedOutput);
    }

    @Test
    public void testApply_castsDouble() {
      String fieldName = "fieldDouble";
      Double fieldValue = 7.0;
      Schema inputSchema =
          Schema.createRecord(
              ImmutableList.<Field>builder()
                  .add(new Field(fieldName, Schema.create(Schema.Type.DOUBLE)))
                  .build());
      GenericRecord input =
          new GenericRecordBuilder(inputSchema).set(fieldName, fieldValue).build();
      Struct expectedOutput = Struct.newBuilder().set(fieldName).to(fieldValue).build();

      Struct output = AvroToStructFn.create().apply(input);

      assertThat(output).isEqualTo(expectedOutput);
    }

    @Test
    public void testApply_castsFloat() {
      String fieldName = "fieldFloat";
      Float fieldValue = 7.0F;
      Schema inputSchema =
          Schema.createRecord(
              ImmutableList.<Field>builder()
                  .add(new Field(fieldName, Schema.create(Schema.Type.FLOAT)))
                  .build());
      GenericRecord input =
          new GenericRecordBuilder(inputSchema).set(fieldName, fieldValue).build();
      Struct expectedOutput = Struct.newBuilder().set(fieldName).to(fieldValue).build();

      Struct output = AvroToStructFn.create().apply(input);

      assertThat(output).isEqualTo(expectedOutput);
    }

    @Test
    public void testApply_castsInt() {
      String fieldName = "fieldInt";
      Integer fieldValue = 7;
      Schema inputSchema =
          Schema.createRecord(
              ImmutableList.<Field>builder()
                  .add(new Field(fieldName, Schema.create(Schema.Type.INT)))
                  .build());
      GenericRecord input =
          new GenericRecordBuilder(inputSchema).set(fieldName, fieldValue).build();
      Struct expectedOutput = Struct.newBuilder().set(fieldName).to(fieldValue).build();

      Struct output = AvroToStructFn.create().apply(input);

      assertThat(output).isEqualTo(expectedOutput);
    }

    @Test
    public void testApply_castsLong() {
      String fieldName = "fieldLong";
      Long fieldValue = 7L;
      Schema inputSchema =
          Schema.createRecord(
              ImmutableList.<Field>builder()
                  .add(new Field(fieldName, Schema.create(Schema.Type.LONG)))
                  .build());
      GenericRecord input =
          new GenericRecordBuilder(inputSchema).set(fieldName, fieldValue).build();
      Struct expectedOutput = Struct.newBuilder().set(fieldName).to(fieldValue).build();

      Struct output = AvroToStructFn.create().apply(input);

      assertThat(output).isEqualTo(expectedOutput);
    }

    @Test
    public void testApply_castsString() {
      String fieldName = "fieldString";
      String fieldValue = "textValue";
      Schema inputSchema =
          Schema.createRecord(
              ImmutableList.<Field>builder()
                  .add(new Field(fieldName, Schema.create(Schema.Type.STRING)))
                  .build());
      GenericRecord input =
          new GenericRecordBuilder(inputSchema).set(fieldName, fieldValue).build();
      Struct expectedOutput = Struct.newBuilder().set(fieldName).to(fieldValue).build();

      Struct output = AvroToStructFn.create().apply(input);

      assertThat(output).isEqualTo(expectedOutput);
    }

    @Test
    public void testApply_castsMultipleFields() {
      Schema inputSchema =
          Schema.createRecord(
              ImmutableList.<Field>builder()
                  .add(new Field("fieldBoolean", Schema.create(Schema.Type.BOOLEAN)))
                  .add(new Field("fieldDouble", Schema.create(Schema.Type.DOUBLE)))
                  .add(new Field("fieldFloat", Schema.create(Schema.Type.FLOAT)))
                  .add(new Field("fieldInt", Schema.create(Schema.Type.INT)))
                  .add(new Field("fieldLong", Schema.create(Schema.Type.LONG)))
                  .add(new Field("fieldString", Schema.create(Schema.Type.STRING)))
                  .add(new Field("extraFieldString", Schema.create(Schema.Type.STRING)))
                  .build());
      GenericRecord input =
          new GenericRecordBuilder(inputSchema)
              .set("fieldBoolean", Boolean.TRUE)
              .set("fieldDouble", 7.0)
              .set("fieldFloat", 7.0F)
              .set("fieldInt", 7)
              .set("fieldLong", 7L)
              .set("fieldString", "text")
              .set("extraFieldString", "otherText")
              .build();
      Struct expectedOutput =
          Struct.newBuilder()
              .set("fieldBoolean")
              .to(Boolean.TRUE)
              .set("fieldDouble")
              .to(7.0)
              .set("fieldFloat")
              .to(7.0F)
              .set("fieldInt")
              .to(7)
              .set("fieldLong")
              .to(7L)
              .set("fieldString")
              .to("text")
              .set("extraFieldString")
              .to("otherText")
              .build();

      Struct output = AvroToStructFn.create().apply(input);

      assertThat(output).isEqualTo(expectedOutput);
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
