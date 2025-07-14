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
package com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link UnifiedMappingProvider}. */
@RunWith(MockitoJUnitRunner.class)
public class UnifiedMappingProviderTest {
  @Test
  public void testUnifiedMappingProvider() {
    final Long[] decimalMods = {1L, 1L};
    final Long[] varCharMods = {10L};
    ImmutableMap<String, String> stringifiedMapping =
        simpleTypes().stream()
            .map(
                t ->
                    Map.entry(
                        t.toString(),
                        UnifiedMappingProvider.getMapping(t)
                            .getSchema(null, null)
                            .toString()
                            .replaceAll("\\s+", "")))
            .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));
    assertThat(stringifiedMapping).isEqualTo(simpleTypeExpectedMapping());
    assertThat(
            UnifiedMappingProvider.getMapping(UnifiedMappingProvider.Type.DECIMAL)
                .getSchema(decimalMods, null)
                .toString())
        .isEqualTo("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":1,\"scale\":1}");

    assertThat(
            UnifiedMappingProvider.getMapping(UnifiedMappingProvider.Type.VARCHAR)
                .getSchema(varCharMods, null)
                .toString())
        .isEqualTo("{\"type\":\"string\",\"logicalType\":\"varchar\",\"length\":10}");
    assertThat(
            UnifiedMappingProvider.getArrayMapping(UnifiedMappingProvider.Type.VARCHAR)
                .getSchema(varCharMods, null)
                .toString())
        .isEqualTo(
            "{\"type\":\"array\",\"items\":{\"type\":\"string\",\"logicalType\":\"varchar\",\"length\":10}}");
    assertThat(
            UnifiedMappingProvider.getArrayMapping(UnifiedMappingProvider.Type.UNSUPPORTED)
                .getSchema(varCharMods, null)
                .toString())
        .isEqualTo("{\"type\":\"null\",\"logicalType\":\"unsupported\"}");
  }

  private ImmutableList<UnifiedMappingProvider.Type> simpleTypes() {

    return ImmutableList.<UnifiedMappingProvider.Type>builder()
        .add(UnifiedMappingProvider.Type.BOOLEAN)
        .add(UnifiedMappingProvider.Type.BYTES)
        .add(UnifiedMappingProvider.Type.DOUBLE)
        .add(UnifiedMappingProvider.Type.DATE)
        .add(UnifiedMappingProvider.Type.DATETIME)
        .add(UnifiedMappingProvider.Type.FLOAT)
        .add(UnifiedMappingProvider.Type.INTEGER)
        .add(UnifiedMappingProvider.Type.INTERVAL)
        .add(UnifiedMappingProvider.Type.JSON)
        .add(UnifiedMappingProvider.Type.LONG)
        .add(UnifiedMappingProvider.Type.NUMBER)
        .add(UnifiedMappingProvider.Type.STRING)
        .add(UnifiedMappingProvider.Type.TIME)
        .add(UnifiedMappingProvider.Type.TIME_INTERVAL)
        .add(UnifiedMappingProvider.Type.TIMESTAMP)
        .add(UnifiedMappingProvider.Type.TIMESTAMP_WITH_TIME_ZONE)
        .add(UnifiedMappingProvider.Type.TIME_WITH_TIME_ZONE)
        .add(UnifiedMappingProvider.Type.UNSUPPORTED)
        .build();
  }

  private ImmutableMap<String, String> simpleTypeExpectedMapping() {
    return ImmutableMap.<String, String>builder()
        .put("BOOLEAN", "\"boolean\"")
        .put("BYTES", "\"bytes\"")
        .put("DOUBLE", "\"double\"")
        .put("DATE", "{\"type\":\"int\",\"logicalType\":\"date\"}")
        .put(
            "DATETIME",
            "{\"type\":\"record\","
                + "\"name\":\"datetime\","
                + "\"fields\": ["
                + "{\"name\":\"date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
                + "{\"name\":\"time\",\"type\":{\"type\":\"long\",\"logicalType\":\"time-micros\"}}]}")
        .put("FLOAT", "\"float\"")
        .put("INTEGER", "\"int\"")
        .put(
            "INTERVAL",
            "{\"type\":\"record\","
                + "\"name\":\"interval\","
                + "\"fields\":["
                + "{\"name\":\"months\",\"type\":\"int\"},"
                + "{\"name\":\"hours\",\"type\":\"int\"},"
                + "{\"name\":\"micros\",\"type\":\"long\"}]}")
        .put("JSON", "{\"type\":\"string\",\"logicalType\":\"json\"}")
        .put("LONG", "\"long\"")
        .put("NUMBER", "{\"type\":\"string\", \"logicalType\":\"number\"}")
        .put("STRING", "\"string\"")
        .put("TIME", "{\"type\":\"long\", \"logicalType\":\"time-micros\"}")
        .put("TIME_INTERVAL", "{\"type\":\"long\", \"logicalType\":\"time-interval-micros\"}")
        .put("TIMESTAMP", "{\"type\":\"long\", \"logicalType\":\"timestamp-micros\"}")
        .put(
            "TIMESTAMP_WITH_TIME_ZONE",
            "{\"type\":\"record\","
                + "\"name\":\"timestampTz\","
                + "\"fields\":[{"
                + "\"name\":\"timestamp\", \"type\":"
                + "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},"
                + "{\"name\":\"offset\",\"type\":"
                + "{\"type\":\"int\",\"logicalType\":\"time-millis\"}}]}")
        .put(
            "TIME_WITH_TIME_ZONE",
            "{\"type\":\"record\","
                + "\"name\":\"timeTz\","
                + "\"fields\":[{"
                + "\"name\":\"time\","
                + "\"type\":{\"type\":\"long\",\"logicalType\":\"time-micros\"}},"
                + "{\"name\":\"offset\", \"type\":"
                + "{\"type\":\"int\",\"logicalType\":\"time-millis\"}}]}")
        .put("UNSUPPORTED", "{\"type\":\"null\", \"logicalType\":\"unsupported\"}")
        .build()
        .entrySet()
        .stream()
        .map(e -> Map.entry(e.getKey(), e.getValue().replaceAll("\\s+", "")))
        .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));
  }
}
