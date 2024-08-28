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
        .put("CHARACTER VARYING", "\"string\"")
        .put("DATE", "{\"type\":\"int\",\"logicalType\":\"date\"}")
        .put("INT8", "\"long\"")
        .put("TEXT", "\"string\"")
        .put("VARCHAR", "\"string\"")
        .build()
        .entrySet()
        .stream()
        .map(e -> Map.entry(e.getKey(), e.getValue().replaceAll("\\s+", "")))
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
