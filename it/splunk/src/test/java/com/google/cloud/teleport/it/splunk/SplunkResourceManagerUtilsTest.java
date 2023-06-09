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
package com.google.cloud.teleport.it.splunk;

import static com.google.cloud.teleport.it.splunk.SplunkResourceManagerUtils.DEFAULT_SPLUNK_INDEX;
import static com.google.cloud.teleport.it.splunk.SplunkResourceManagerUtils.generateHecToken;
import static com.google.cloud.teleport.it.splunk.SplunkResourceManagerUtils.generateSplunkPassword;
import static com.google.cloud.teleport.it.splunk.SplunkResourceManagerUtils.splunkEventToMap;
import static com.google.common.truth.Truth.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.splunk.SplunkEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link com.google.cloud.teleport.it.splunk.SplunkResourceManager}. */
@RunWith(JUnit4.class)
public class SplunkResourceManagerUtilsTest {

  @Test
  public void testSplunkEventToMapWithValuesSet() {
    SplunkEvent event =
        SplunkEvent.newBuilder()
            .withEvent("myEvent")
            .withSource("mySource")
            .withSourceType("mySourceType")
            .withIndex("myIndex")
            .withTime(123L)
            .create();

    Map<String, Object> expected = new HashMap<>();
    expected.put("event", "myEvent");
    expected.put("source", "mySource");
    expected.put("sourcetype", "mySourceType");
    expected.put("index", "myIndex");
    expected.put("time", 123L);
    expected.put("host", null);

    Map<String, Object> actual = splunkEventToMap(event);
    assertThat(actual).containsExactlyEntriesIn(expected);
  }

  @Test
  public void testSplunkEventToMapWithDefaultValueForIndex() {
    SplunkEvent event = SplunkEvent.newBuilder().withEvent("myEvent").create();

    Map<String, Object> expected = new HashMap<>();
    expected.put("event", "myEvent");
    expected.put("index", DEFAULT_SPLUNK_INDEX);
    expected.put("source", null);
    expected.put("sourcetype", null);
    expected.put("host", null);
    expected.put("time", null);

    assertThat(splunkEventToMap(event)).containsExactlyEntriesIn(expected);
  }

  @Test
  public void testGeneratePasswordMeetsRequirements() {
    for (int i = 0; i < 10000; i++) {
      String password = generateSplunkPassword();
      int lower = 0;
      int upper = 0;

      for (char c : password.toCharArray()) {
        String s = String.valueOf(c);
        lower += s.toLowerCase().equals(s) ? 1 : 0;
        upper += s.toUpperCase().equals(s) ? 1 : 0;
      }

      assertThat(lower).isAtLeast(2);
      assertThat(upper).isAtLeast(2);
    }
  }

  @Test
  public void testGenerateHecTokenMeetsRequirements() {
    for (int i = 0; i < 10000; i++) {
      String password = generateHecToken();
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
