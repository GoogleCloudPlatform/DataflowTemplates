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
import java.util.Map.Entry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link MysqlMappingProvider}. */
@RunWith(MockitoJUnitRunner.class)
public class MysqlMappingProviderTest {
  @Test
  public void testMySqlMappingProvider() {
    Long[] testMods = {1L, 1L};
    // We don't have a use case for arrays yet.
    Long[] testArrayBounds = {};
    ImmutableMap<String, String> stringifiedMapping =
        MysqlMappingProvider.getMapping().entrySet().stream()
            .map(
                e ->
                    Map.entry(
                        e.getKey(),
                        e.getValue()
                            .getSchema(testMods, testArrayBounds)
                            .toString()
                            .replaceAll("\\s+", "")))
            .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));
    assertThat(stringifiedMapping).isEqualTo(expectedMapping());
  }

  private ImmutableMap<String, String> expectedMapping() {
    return ImmutableMap.<String, String>builder()
        .put("BIGINT", "\"long\"")
        .put("BIGINT UNSIGNED", "{\"type\":\"string\",\"logicalType\":\"number\"}")
        .put("BINARY", "\"string\"")
        .put("BIT", "\"long\"")
        .put("BLOB", "\"string\"")
        .put("BOOL", "\"int\"")
        .put("CHAR", "\"string\"")
        .put("DATE", "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}")
        .put(
            "DATETIME",
            "{\"type\":\"record\","
                + "\"name\":\"datetime\","
                + "\"fields\": ["
                + "{\"name\":\"date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
                + "{\"name\":\"time\",\"type\":{\"type\":\"long\",\"logicalType\":\"time-micros\"}}]}")
        .put(
            "DECIMAL",
            "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":1,\"scale\":1}")
        .put("DOUBLE", "\"double\"")
        .put("ENUM", "\"string\"")
        .put("FLOAT", "\"float\"")
        .put("INTEGER", "\"int\"")
        .put("INTEGER UNSIGNED", "\"long\"")
        .put("JSON", "{\"type\":\"string\",\"logicalType\":\"json\"}")
        .put("LONGBLOB", "\"string\"")
        .put("LONGTEXT", "\"string\"")
        .put("MEDIUMBLOB", "\"string\"")
        .put("MEDIUMINT", "\"int\"")
        .put("MEDIUMTEXT", "\"string\"")
        .put("SET", "\"string\"")
        .put("SMALLINT", "\"int\"")
        .put("TEXT", "\"string\"")
        .put("TIME", "{\"type\":\"long\",\"logicalType\":\"time-interval-micros\"}")
        .put("TIMESTAMP", "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}")
        .put("TINYBLOB", "\"string\"")
        .put("TINYINT", "\"int\"")
        .put("TINYTEXT", "\"string\"")
        .put("VARBINARY", "\"string\"")
        .put("VARCHAR", "\"string\"")
        .put("YEAR", "\"int\"")
        .put("UNSUPPORTED", "{\"type\":\"null\",\"logicalType\":\"unsupported\"}")
        .build()
        .entrySet()
        .stream()
        .map(e -> Map.entry(e.getKey(), e.getValue().replaceAll("\\s+", "")))
        .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));
  }
}
