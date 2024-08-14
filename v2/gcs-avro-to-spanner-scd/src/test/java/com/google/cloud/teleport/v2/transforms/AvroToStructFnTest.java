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

@RunWith(Enclosed.class)
public final class AvroToStructFnTest {

  @RunWith(JUnit4.class)
  public static final class UnitTests {

    @Test
    public void testApply_castsAllDataTypes() {
      Schema inputSchema =
          Schema.createRecord(
              ImmutableList.<Field>builder()
                  .add(new Field("boolTrue", Schema.create(Schema.Type.BOOLEAN)))
                  .add(new Field("boolFalse", Schema.create(Schema.Type.BOOLEAN)))
                  .add(new Field("double", Schema.create(Schema.Type.DOUBLE)))
                  .add(new Field("float", Schema.create(Schema.Type.FLOAT)))
                  .add(new Field("int", Schema.create(Schema.Type.INT)))
                  .add(new Field("long", Schema.create(Schema.Type.LONG)))
                  .add(new Field("string", Schema.create(Schema.Type.STRING)))
                  .build());
      GenericRecord input =
          new GenericRecordBuilder(inputSchema)
              .set("boolTrue", Boolean.TRUE)
              .set("boolFalse", Boolean.FALSE)
              .set("double", 7.0)
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
              .set("double")
              .to(7.0)
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
