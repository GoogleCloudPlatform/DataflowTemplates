/*
 * Copyright (C) 2020 Google Inc.
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

import static com.google.common.truth.Truth.assertThat;

import org.apache.avro.Schema;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link SerializableSchemaSupplier}.
 */
@RunWith(JUnit4.class)
public class SerializableSchemaSupplierTest {

  private static final String AVRO_SCHEMA_STRING = "{\n"
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

  @Test
  public void schemaSupplierProvidesValidSchema() {
    Schema expectedSchema = new Schema.Parser().parse(AVRO_SCHEMA_STRING);

    Supplier<Schema> schemaSupplier = SerializableSchemaSupplier.of(expectedSchema);

    assertThat(schemaSupplier.get()).isEqualTo(expectedSchema);
  }
}
