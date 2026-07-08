/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.gcp.logging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.Logging.EntryListOption;
import com.google.cloud.logging.Payload;
import com.google.cloud.logging.Severity;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/** Unit test for {@link LoggingClient}. */
public class LoggingClientTest {
  private Logging mockLoggingService;
  private Credentials mockCredentials;
  private Page<LogEntry> mockLogEntryPage;

  private LoggingClient loggingClient;

  @Before
  public void setUp() {
    mockLoggingService = mock(Logging.class);
    mockCredentials = mock(Credentials.class);
    mockLogEntryPage = mock(Page.class);
    loggingClient = LoggingClient.withLoggingServiceClient(mockLoggingService);
    when(mockLoggingService.listLogEntries(
            any(EntryListOption.class), any(EntryListOption.class), any(EntryListOption.class)))
        .thenReturn(mockLogEntryPage);
  }

  @Test
  public void testBuilderWithNullCredentials() {
    LoggingClient.Builder builder = LoggingClient.builder(null);
    builder.setProjectId("test-project-id");
    LoggingClient clientFromBuilder = builder.build();
    assertNotNull(clientFromBuilder);
  }

  @Test
  public void testWithLoggingServiceClient() {
    Logging localMockLoggingService = mock(Logging.class);
    LoggingClient client = LoggingClient.withLoggingServiceClient(localMockLoggingService);
    assertNotNull(client);
    when(localMockLoggingService.listLogEntries(
            any(EntryListOption.class), any(EntryListOption.class), any(EntryListOption.class)))
        .thenReturn(mockLogEntryPage);
    when(mockLogEntryPage.iterateAll()).thenReturn(Collections.emptyList());
    client.readLogs("test-filter", 10);
    verify(localMockLoggingService, times(1))
        .listLogEntries(
            any(EntryListOption.class), any(EntryListOption.class), any(EntryListOption.class));
  }

  @Test
  public void testReadLogs_returnsPayloads() {
    LogEntry entry1 = mock(LogEntry.class);
    Payload payload1 = mock(Payload.class);
    when(entry1.getPayload()).thenReturn(payload1);

    LogEntry entry2 = mock(LogEntry.class);
    Payload payload2 = mock(Payload.class);
    when(entry2.getPayload()).thenReturn(payload2);

    List<LogEntry> mockEntries = Arrays.asList(entry1, entry2);
    when(mockLogEntryPage.iterateAll()).thenReturn(mockEntries);

    List<Payload> result = loggingClient.readLogs("test-filter", 10);

    assertEquals(2, result.size());
    assertTrue(result.contains(payload1));
    assertTrue(result.contains(payload2));

    ArgumentCaptor<EntryListOption> filterCaptor = ArgumentCaptor.forClass(EntryListOption.class);
    ArgumentCaptor<EntryListOption> pageSizeCaptor = ArgumentCaptor.forClass(EntryListOption.class);
    ArgumentCaptor<EntryListOption> sortOrderCaptor =
        ArgumentCaptor.forClass(EntryListOption.class);

    verify(mockLoggingService)
        .listLogEntries(
            filterCaptor.capture(), pageSizeCaptor.capture(), sortOrderCaptor.capture());
  }

  @Test
  public void testReadLogs_respectsMaxEntries() {
    LogEntry entry1 = mock(LogEntry.class);
    LogEntry entry2 = mock(LogEntry.class);
    LogEntry entry3 = mock(LogEntry.class);
    when(entry1.getPayload()).thenReturn(mock(Payload.class));
    when(entry2.getPayload()).thenReturn(mock(Payload.class));
    when(entry3.getPayload()).thenReturn(mock(Payload.class));

    List<LogEntry> mockEntries = Arrays.asList(entry1, entry2, entry3);
    when(mockLogEntryPage.iterateAll()).thenReturn(mockEntries);

    List<Payload> result = loggingClient.readLogs("test-filter", 2);

    assertEquals(2, result.size());
    verify(mockLoggingService, times(1))
        .listLogEntries(
            any(EntryListOption.class), any(EntryListOption.class), any(EntryListOption.class));
  }

  @Test
  public void testReadLogs_noEntriesFound() {
    when(mockLogEntryPage.iterateAll()).thenReturn(Collections.emptyList());

    List<Payload> result = loggingClient.readLogs("test-filter", 10);

    assertTrue(result.isEmpty());
    verify(mockLoggingService, times(1))
        .listLogEntries(
            any(EntryListOption.class), any(EntryListOption.class), any(EntryListOption.class));
  }

  @Test
  public void testReadJobLogs_filterConstruction_withSeverityAndAdditionalFilter() {
    String jobId = "test-job-123";
    String additionalFilter = "AND textPayload:\"error\"";
    Severity minSeverity = Severity.WARNING;
    int maxEntries = 5;

    when(mockLogEntryPage.iterateAll()).thenReturn(Collections.emptyList());

    loggingClient.readJobLogs(jobId, additionalFilter, minSeverity, maxEntries);

    ArgumentCaptor<EntryListOption> filterOptionCaptor =
        ArgumentCaptor.forClass(EntryListOption.class);
    verify(mockLoggingService)
        .listLogEntries(
            filterOptionCaptor.capture(), any(EntryListOption.class), any(EntryListOption.class));

    String capturedFilterString = getFilterStringFromOption(filterOptionCaptor.getValue());

    assertTrue(capturedFilterString.contains("resource.type=\"dataflow_step\""));
    assertTrue(
        capturedFilterString.contains(
            "logName=(\"projects/cloud-teleport-testing/logs/dataflow.googleapis.com%2Fjob-message\" OR \"projects/cloud-teleport-testing/logs/dataflow.googleapis.com%2Flauncher\")"));
    assertTrue(capturedFilterString.contains("resource.labels.job_id=\"" + jobId + "\""));
    assertTrue(capturedFilterString.contains("severity >= \"" + minSeverity.name() + "\""));
    assertTrue(capturedFilterString.endsWith(additionalFilter));
  }

  @Test
  public void testReadJobLogs_filterConstruction_withoutSeverity() {
    String jobId = "test-job-456";
    String additionalFilter = "AND customField=\"value\"";
    int maxEntries = 3;

    when(mockLogEntryPage.iterateAll()).thenReturn(Collections.emptyList());

    loggingClient.readJobLogs(jobId, additionalFilter, null, maxEntries);

    ArgumentCaptor<EntryListOption> filterOptionCaptor =
        ArgumentCaptor.forClass(EntryListOption.class);
    verify(mockLoggingService)
        .listLogEntries(
            filterOptionCaptor.capture(), any(EntryListOption.class), any(EntryListOption.class));

    String capturedFilterString = getFilterStringFromOption(filterOptionCaptor.getValue());

    assertTrue(capturedFilterString.contains("resource.type=\"dataflow_step\""));
    assertTrue(
        capturedFilterString.contains(
            "logName=(\"projects/cloud-teleport-testing/logs/dataflow.googleapis.com%2Fjob-message\" OR \"projects/cloud-teleport-testing/logs/dataflow.googleapis.com%2Flauncher\")"));
    assertTrue(capturedFilterString.contains("resource.labels.job_id=\"" + jobId + "\""));
    assertTrue(capturedFilterString.endsWith(additionalFilter));
    assertFalse(capturedFilterString.contains("severity >="));
  }

  @Test
  public void testCleanupAll() {
    loggingClient.cleanupAll();
  }

  private String getFilterStringFromOption(EntryListOption option) {
    String optionStr = option.toString();
    if (optionStr.contains("value=")) {
      return optionStr.substring(
          optionStr.indexOf("value=") + "value=".length(), optionStr.length() - 1);
    } else if (optionStr.startsWith("FILTER(")) {
      return optionStr.substring("FILTER(".length(), optionStr.length() - 1);
    }
    System.err.println(
        "Warning: Could not reliably extract filter string from EntryListOption: " + optionStr);
    return optionStr;
  }
}
