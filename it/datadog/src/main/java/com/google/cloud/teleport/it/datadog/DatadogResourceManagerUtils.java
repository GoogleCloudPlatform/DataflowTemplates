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
package com.google.cloud.teleport.it.datadog;

import static com.google.cloud.teleport.it.common.utils.ResourceManagerUtils.generatePassword;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.io.datadog.DatadogEvent;

/** Utilities for {@link DatadogResourceManager} implementations. */
public final class DatadogResourceManagerUtils {

  static final String DEFAULT_DATADOG_INDEX = "main";

  // Datadog event metadata keys
  private static final String DATADOG_EVENT_KEY = "event";
  private static final String DATADOG_HOST_KEY = "host";
  private static final String DATADOG_INDEX_KEY = "index";
  private static final String DATADOG_TIME_KEY = "time";
  private static final String DATADOG_SOURCE_KEY = "source";
  private static final String DATADOG_SOURCE_TYPE_KEY = "sourcetype";

  private static final int MIN_PASSWORD_LENGTH = 8;
  private static final int MAX_PASSWORD_LENGTH = 20;

  private DatadogResourceManagerUtils() {}

  public static Map<String, Object> datadogEventToMap(DatadogEvent event) {
    Map<String, Object> eventMap = new HashMap<>();
    eventMap.put(DATADOG_EVENT_KEY, event.event());
    eventMap.put(DATADOG_HOST_KEY, event.host());
    eventMap.put(DATADOG_INDEX_KEY, event.index() != null ? event.index() : DEFAULT_DATADOG_INDEX);
    eventMap.put(DATADOG_SOURCE_KEY, event.source());
    eventMap.put(DATADOG_SOURCE_TYPE_KEY, event.sourceType());
    eventMap.put(DATADOG_TIME_KEY, event.time());

    return eventMap;
  }

  /**
   * Generates a secure, valid Datadog password.
   *
   * @return The generated password.
   */
  static String generateDatadogPassword() {
    int numLower = 2;
    int numUpper = 2;
    int numSpecial = 0;
    return generatePassword(
        MIN_PASSWORD_LENGTH,
        MAX_PASSWORD_LENGTH,
        numLower,
        numUpper,
        numSpecial,
        /* specialChars= */ null);
  }

  /**
   * Generates a secure, valid Datadog HEC authentication token.
   *
   * @return The generated password.
   */
  static String generateHecToken() {
    return UUID.randomUUID().toString();
  }
}
