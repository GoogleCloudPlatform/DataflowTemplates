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

import static com.google.cloud.teleport.it.datadog.DatadogResourceManagerUtils.datadogEntryToMap;
import static com.google.cloud.teleport.it.datadog.DatadogResourceManagerUtils.generateApiKey;
import static com.google.common.truth.Truth.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link com.google.cloud.teleport.it.datadog.DatadogResourceManager}. */
@RunWith(JUnit4.class)
public class DatadogResourceManagerUtilsTest {

  @Test
  public void testDatadogLogEntryToMapWithValuesSet() {
    DatadogLogEntry entry =
        DatadogLogEntry.newBuilder().withMessage("myEvent").withSource("mySource").build();

    Map<String, Object> expected = new HashMap<>();
    expected.put("message", "myEvent");
    expected.put("ddsource", "mySource");
    expected.put("hostname", null);
    expected.put("ddtags", null);
    expected.put("service", null);

    Map<String, Object> actual = datadogEntryToMap(entry);
    assertThat(actual).containsExactlyEntriesIn(expected);
  }

  @Test
  public void testGenerateApiKeyMeetsRequirements() {
    for (int i = 0; i < 10000; i++) {
      String password = generateApiKey();
      int lower = 0;
      int upper = 0;

      for (char c : password.toCharArray()) {
        String s = String.valueOf(c);
        lower += s.toLowerCase().equals(s) ? 1 : 0;
        upper += s.toUpperCase().equals(s) ? 1 : 0;
      }

      assertThat(lower).isAtLeast(1);
      assertThat(upper).isAtLeast(1);
    }
  }
}
