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
package com.google.cloud.teleport.v2.utils;

import static org.junit.Assert.assertEquals;

import com.google.common.io.Resources;
import org.apache.avro.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;

/** Test cases for the {@link SchemaUtils} class. */
public class SchemaUtilsTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final String RESOURCES_DIR = "SchemaUtilsTest/";

  private static final String AVRO_SCHEMA_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "avro_schema.json").getPath();

  /**
   * Test whether {@link SchemaUtils#getGcsFileAsString(String)} reads a file correctly as a String.
   */
  @Test
  public void testGetGcsFileAsString() {
    String expectedContent =
        "{\n"
            + "  \"type\" : \"record\",\n"
            + "  \"name\" : \"test_file\",\n"
            + "  \"namespace\" : \"com.test\",\n"
            + "  \"fields\" : [\n"
            + "    {\n"
            + "      \"name\": \"id\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"price\",\n"
            + "      \"type\": \"double\"\n"
            + "    }\n"
            + "  ]\n"
            + "}\n";
    String actualContent = SchemaUtils.getGcsFileAsString(AVRO_SCHEMA_FILE_PATH);

    assertEquals(expectedContent, actualContent);
  }

  /**
   * Test whether {@link SchemaUtils#getAvroSchema(String)} reads an Avro schema correctly and
   * returns a {@link Schema} object.
   */
  @Test
  public void testGetAvroSchema() {
    String avroSchema =
        "{\n"
            + "  \"type\" : \"record\",\n"
            + "  \"name\" : \"test_file\",\n"
            + "  \"namespace\" : \"com.test\",\n"
            + "  \"fields\" : [\n"
            + "    {\n"
            + "      \"name\": \"id\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"price\",\n"
            + "      \"type\": \"double\"\n"
            + "    }\n"
            + "  ]\n"
            + "}";
    Schema expectedSchema = new Schema.Parser().parse(avroSchema);
    Schema actualSchema = SchemaUtils.getAvroSchema(AVRO_SCHEMA_FILE_PATH);

    assertEquals(expectedSchema, actualSchema);
  }
}
