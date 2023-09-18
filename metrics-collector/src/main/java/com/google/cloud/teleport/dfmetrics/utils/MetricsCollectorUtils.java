/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.dfmetrics.utils;

import com.google.common.base.CaseFormat;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class {@link MetricsCollectorUtils} represents various helper methods. */
public class MetricsCollectorUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsCollectorUtils.class);

  private MetricsCollectorUtils() {}

  /**
   * Sanitizes the Job name.
   *
   * @param prefix
   * @return
   */
  public static String sanitizeJobName(String prefix) {
    String convertedPrefix =
        CaseFormat.UPPER_CAMEL.converterTo(CaseFormat.LOWER_HYPHEN).convert(prefix);
    String formattedTimestamp =
        DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")
            .withZone(ZoneId.of("UTC"))
            .format(Instant.now());
    return String.format("%s-%s", convertedPrefix, formattedTimestamp);
  }

  /**
   * Serialize to json value.
   *
   * @param entries
   * @param indent
   * @return
   */
  public static String serializeToJson(Map<String, Object> entries, boolean indent) {
    Gson gson;
    if (indent) {
      gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
    } else {
      gson = new Gson();
    }

    return gson.toJson(entries);
  }

  /**
   * Print the Hashmap.
   *
   * @param map
   * @param <K>
   * @param <V>
   */
  public static <K, V> void printMap(Map<K, V> map) {
    for (Map.Entry entry : map.entrySet()) {
      System.out.printf("%s - %s\n", entry.getKey(), entry.getValue());
    }
  }

  /**
   * Cast the values in the map to appropriate types.
   *
   * @param map
   * @return
   */
  public static Map<String, Object> castValuesToAppropriateTypes(Map<String, Object> map) {
    // During Serialization Since JSON converts numerics to Double convert to appropriate type.
    for (String key : new String[] {"numWorkers", "maxWorkers", "disk_size_gb"}) {
      if (map.containsKey(key)) {
        map.put(key, ((Double) map.get(key)).intValue());
      }
    }
    return map;
  }
}
