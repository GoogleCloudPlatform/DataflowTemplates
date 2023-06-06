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
package com.google.cloud.teleport.it.datadog.matchers;

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatRecords;
import static com.google.cloud.teleport.it.datadog.DatadogResourceManagerUtils.datadogEventToMap;

import com.google.cloud.teleport.it.common.matchers.RecordsSubject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.datadog.DatadogEvent;

/** Assert utilities for Datadog tests. */
public class DatadogAsserts {

  /**
   * Convert Datadog {@link DatadogEvent} to a list of maps.
   *
   * @param events List of DatadogEvents to parse
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> datadogEventsToRecords(Collection<DatadogEvent> events) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (DatadogEvent event : events) {
        Map<String, Object> converted = datadogEventToMap(event);
        records.add(converted);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting DatadogEvents to Records", e);
    }
  }

  /**
   * Creates a {@link RecordsSubject} to assert information within a list of records.
   *
   * @param events List of DatadogEvents in Datadog {@link DatadogEvent} format to use in the
   *     comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatDatadogEvents(@Nullable Collection<DatadogEvent> events) {
    return assertThatRecords(datadogEventsToRecords(events));
  }
}
