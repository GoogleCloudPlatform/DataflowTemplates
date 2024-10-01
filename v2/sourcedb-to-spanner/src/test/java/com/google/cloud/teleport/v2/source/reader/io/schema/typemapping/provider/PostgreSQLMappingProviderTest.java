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
package com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;

public class PostgreSQLMappingProviderTest {
  @Test
  public void testPostgreSQLMappingProvider() {
    Long[] testMods = {1L, 1L};
    // We don't have a use case for arrays yet.
    Long[] testArrayBounds = {};
    ImmutableMap<String, String> stringifiedMapping =
        PostgreSQLMappingProvider.getMapping().entrySet().stream()
            .map(
                e ->
                    Map.entry(
                        e.getKey(),
                        e.getValue()
                            .getSchema(testMods, testArrayBounds)
                            .toString()
                            .replaceAll("\\s+", "")))
            .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    assertThat(stringifiedMapping).isEqualTo(expectedMapping());
  }

  private ImmutableMap<String, String> expectedMapping() {
    return ImmutableMap.<String, String>builder()
        .put("BIGINT", "\"long\"")
        .put("BIGSERIAL", "\"long\"")
        .put("BIT", "\"bytes\"")
        .put("BIT VARYING", "\"bytes\"")
        .put("BOOL", "\"boolean\"")
        .put("BOOLEAN", "\"boolean\"")
        .put("BYTEA", "\"bytes\"")
        .put("CHAR", "\"string\"")
        .put("CHARACTER", "\"string\"")
        .put("CHARACTER VARYING", "\"string\"")
        .put("CITEXT", "\"string\"")
        .put("DATE", "{\"type\":\"int\",\"logicalType\":\"date\"}")
        .put("DECIMAL", "{\"type\":\"string\",\"logicalType\":\"number\"}")
        .put("DOUBLE PRECISION", "\"double\"")
        .put("FLOAT4", "\"float\"")
        .put("FLOAT8", "\"double\"")
        .put("INT", "\"int\"")
        .put("INTEGER", "\"int\"")
        .put("INT2", "\"int\"")
        .put("INT4", "\"int\"")
        .put("INT8", "\"long\"")
        .put("JSON", "{\"type\":\"string\",\"logicalType\":\"json\"}")
        .put("JSONB", "{\"type\":\"string\",\"logicalType\":\"json\"}")
        .put("MONEY", "\"double\"")
        .put("NUMERIC", "{\"type\":\"string\",\"logicalType\":\"number\"}")
        .put("OID", "\"long\"")
        .put("REAL", "\"float\"")
        .put("SERIAL", "\"int\"")
        .put("SERIAL2", "\"int\"")
        .put("SERIAL4", "\"int\"")
        .put("SERIAL8", "\"long\"")
        .put("SMALLINT", "\"int\"")
        .put("SMALLSERIAL", "\"int\"")
        .put("TEXT", "\"string\"")
        .put("TIMESTAMP", "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}")
        .put(
            "TIMESTAMPTZ",
            "{\"type\":\"record\",\"name\":\"timestampTz\",\"fields\":[{\"name\":\"timestamp\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},{\"name\":\"offset\",\"type\":{\"type\":\"int\",\"logicalType\":\"time-millis\"}}]}")
        .put(
            "TIMESTAMP WITH TIME ZONE",
            "{\"type\":\"record\",\"name\":\"timestampTz\",\"fields\":[{\"name\":\"timestamp\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},{\"name\":\"offset\",\"type\":{\"type\":\"int\",\"logicalType\":\"time-millis\"}}]}")
        .put(
            "TIMESTAMP WITHOUT TIME ZONE",
            "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}")
        .put("UUID", "\"string\"")
        .put("VARBIT", "\"bytes\"")
        .put("VARCHAR", "\"string\"")
        .put("UNSUPPORTED", "{\"type\":\"null\",\"logicalType\":\"unsupported\"}")
        .build()
        .entrySet()
        .stream()
        .map(e -> Map.entry(e.getKey(), e.getValue().replaceAll("\\s+", "")))
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
