/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.common.io.Resources;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test cases for the {@link AvroConverters} class. */
public class AvroConvertersTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public ExpectedException expectedException = ExpectedException.none();

  private static final String RESOURCES_DIR = "AvroConvertersTest/";

  private static final String FAKE_DIR = "FakeDirectory/";

  private static final String AVRO_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "test_avro_file.avro").getPath();

  private static final String SCHEMA_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "test_schema.json").getPath();

  /** Tests if {@link AvroConverters.ReadAvroFile} reads an Avro file correctly. */
  @Test
  public void testReadAvroFile() {
    Schema schema = SchemaUtils.getAvroSchema(SCHEMA_FILE_PATH);

    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put("id", "007");
    genericRecord.put("state", "CA");
    genericRecord.put("price", 26.23);

    PCollection<GenericRecord> pCollection =
        pipeline.apply(
            "ReadAvroFile",
            AvroConverters.ReadAvroFile.newBuilder()
                .withInputFileSpec(AVRO_FILE_PATH)
                .withSchema(SCHEMA_FILE_PATH)
                .build());

    PAssert.that(pCollection).containsInAnyOrder(genericRecord);

    pipeline.run();
  }

  /**
   * Test whether {@link AvroConverters.ReadAvroFile} throws an exception if no Avro schema is
   * provided.
   */
  @Test
  public void testReadWithoutSchema() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("withSchema(schema) called with null input.");

    pipeline.apply(
        AvroConverters.ReadAvroFile.newBuilder()
            .withInputFileSpec(AVRO_FILE_PATH)
            .withSchema(null)
            .build());

    pipeline.run();
  }

  /**
   * Test whether {@link AvroConverters.ReadAvroFile} throws an exception if no input Avro file is
   * provided.
   */
  @Test
  public void testReadWithoutInputFile() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("withInputFileSpec(inputFileSpec) called with null input.");

    pipeline.apply(
        AvroConverters.ReadAvroFile.newBuilder()
            .withInputFileSpec(null)
            .withSchema(SCHEMA_FILE_PATH)
            .build());

    pipeline.run();
  }

  /**
   * Test whether {@link AvroConverters.WriteAvroFile} throws an exception if no Avro schema is
   * provided.
   */
  @Test
  public void testWriteWithoutSchema() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("withSchema(schema) called with null input.");

    pipeline
        .apply(
            AvroConverters.ReadAvroFile.newBuilder()
                .withInputFileSpec(AVRO_FILE_PATH)
                .withSchema(SCHEMA_FILE_PATH)
                .build())
        .apply(
            AvroConverters.WriteAvroFile.newBuilder()
                .withOutputFile(FAKE_DIR)
                .withSchema(null)
                .build());

    pipeline.run();
  }

  /**
   * Test whether {@link AvroConverters.WriteAvroFile} throws an exception if no output location is
   * provided.
   */
  @Test
  public void testWriteWithoutInputFile() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("withOutputFile(outputFile) called with null input.");

    pipeline
        .apply(
            AvroConverters.ReadAvroFile.newBuilder()
                .withInputFileSpec(AVRO_FILE_PATH)
                .withSchema(SCHEMA_FILE_PATH)
                .build())
        .apply(
            AvroConverters.WriteAvroFile.newBuilder()
                .withOutputFile(null)
                .withSchema(AVRO_FILE_PATH)
                .build());

    pipeline.run();
  }
}
