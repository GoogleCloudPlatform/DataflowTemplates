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

import static com.google.cloud.teleport.it.datadog.matchers.DatadogAsserts.assertThatDatadogLogEntries;
import static com.google.cloud.teleport.it.datadog.matchers.DatadogAsserts.datadogEntriesToRecords;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.it.testcontainers.TestContainersIntegrationTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

/** Integration tests for Datadog Resource Managers. */
@Category(TestContainersIntegrationTest.class)
@RunWith(JUnit4.class)
public class DatadogResourceManagerIT {
  private static final String TEST_ID = "dummy-test";
  private static final int NUM_EVENTS = 100;

  private DatadogResourceManager datadogResourceManager;

  @Before
  public void setUp() {
    datadogResourceManager = DatadogResourceManager.builder(TEST_ID).build();
  }

  @Test
  public void testDefaultDatadogResourceManagerE2E() {
    // Arrange
    String source = RandomStringUtils.randomAlphabetic(1, 20);
    String host = RandomStringUtils.randomAlphabetic(1, 20);
    List<DatadogLogEntry> httpEventsSent = generateHttpEvents(source, host);

    datadogResourceManager.sendHttpEvents(httpEventsSent);

    List<DatadogLogEntry> httpEventsReceived = datadogResourceManager.getEntries();

    // Assert
    assertThatDatadogLogEntries(httpEventsReceived)
        .hasRecordsUnordered(datadogEntriesToRecords(httpEventsSent));
  }

  private static List<DatadogLogEntry> generateHttpEvents(String source, String hostname) {
    List<DatadogLogEntry> events = new ArrayList<>();
    for (int i = 0; i < NUM_EVENTS; i++) {
      String message = RandomStringUtils.randomAlphabetic(1, 20);
      events.add(
          DatadogLogEntry.newBuilder()
              .withMessage(message)
              .withSource(source)
              .withHostname(hostname)
              .build());
    }

    return events;
  }
}
