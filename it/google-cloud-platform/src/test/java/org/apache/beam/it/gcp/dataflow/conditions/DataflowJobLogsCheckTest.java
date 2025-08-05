/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.gcp.dataflow.conditions;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.logging.Payload;
import com.google.cloud.logging.Severity;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.conditions.ConditionCheck.CheckResult;
import org.apache.beam.it.gcp.logging.LoggingClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DataflowJobLogsCheck}. */
@RunWith(JUnit4.class)
public class DataflowJobLogsCheckTest {

  private LoggingClient mockLoggingClient;
  private LaunchInfo mockLaunchInfo;

  private static final String JOB_ID = "test-job-2025-08-05";
  private static final String FILTER = "\"error message\"";

  @Before
  public void setUp() {
    mockLoggingClient = mock(LoggingClient.class);
    mockLaunchInfo = mock(LaunchInfo.class);
    when(mockLaunchInfo.jobId()).thenReturn(JOB_ID);
  }

  /** Helper method to generate a list of mock log payloads. */
  private List<Payload> createMockLogs(int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> mock(Payload.class))
        .collect(Collectors.toList());
  }

  @Test
  public void testCheck_succeedsWhenExactlyEnoughLogsAreFound() {
    int minLogs = 5;
    List<Payload> fakeLogs = createMockLogs(minLogs);
    when(mockLoggingClient.readJobLogs(anyString(), anyString(), any(Severity.class), anyInt()))
        .thenReturn(fakeLogs);

    DataflowJobLogsCheck check =
        DataflowJobLogsCheck.builder(mockLoggingClient)
            .setJobInfo(mockLaunchInfo)
            .setFilter(FILTER)
            .setMinSeverity(Severity.INFO)
            .setMinLogs(minLogs)
            .build();

    CheckResult result = check.check();

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.getMessage()).isEqualTo("Expected at least 5 logs and found 5");
    verify(mockLoggingClient).readJobLogs(JOB_ID, FILTER, Severity.INFO, minLogs + 2);
    assertNotNull(check.getDescription());
  }

  @Test
  public void testCheck_succeedsWhenMoreLogsAreFound() {
    int minLogs = 10;
    List<Payload> fakeLogs = createMockLogs(minLogs + 5); // 15 logs
    when(mockLoggingClient.readJobLogs(anyString(), anyString(), any(Severity.class), anyInt()))
        .thenReturn(fakeLogs);

    DataflowJobLogsCheck check =
        DataflowJobLogsCheck.builder(mockLoggingClient)
            .setJobInfo(mockLaunchInfo)
            .setFilter(FILTER)
            .setMinSeverity(Severity.ERROR)
            .setMinLogs(minLogs)
            .build();

    CheckResult result = check.check();

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.getMessage()).isEqualTo("Expected at least 10 logs and found 15");
    verify(mockLoggingClient).readJobLogs(JOB_ID, FILTER, Severity.ERROR, minLogs + 2);
  }

  @Test
  public void testCheck_failsWhenNotEnoughLogsAreFound() {
    int minLogs = 20;
    List<Payload> fakeLogs = createMockLogs(minLogs - 1); // 19 logs
    when(mockLoggingClient.readJobLogs(anyString(), anyString(), any(Severity.class), anyInt()))
        .thenReturn(fakeLogs);

    DataflowJobLogsCheck check =
        DataflowJobLogsCheck.builder(mockLoggingClient)
            .setJobInfo(mockLaunchInfo)
            .setFilter(FILTER)
            .setMinSeverity(Severity.WARNING)
            .setMinLogs(minLogs)
            .build();

    CheckResult result = check.check();

    assertThat(result.isSuccess()).isFalse();
    assertThat(result.getMessage()).isEqualTo("Expected 20 logs but has only 19");
    verify(mockLoggingClient).readJobLogs(JOB_ID, FILTER, Severity.WARNING, minLogs + 2);
  }

  @Test
  public void testCheck_succeedsWhenZeroLogsRequiredAndZeroFound() {
    int minLogs = 0;
    when(mockLoggingClient.readJobLogs(anyString(), anyString(), any(Severity.class), anyInt()))
        .thenReturn(Collections.emptyList());

    DataflowJobLogsCheck check =
        DataflowJobLogsCheck.builder(mockLoggingClient)
            .setJobInfo(mockLaunchInfo)
            .setFilter(FILTER)
            .setMinSeverity(Severity.DEFAULT)
            .setMinLogs(minLogs)
            .build();

    CheckResult result = check.check();

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.getMessage()).isEqualTo("Expected at least 0 logs and found 0");
    verify(mockLoggingClient).readJobLogs(JOB_ID, FILTER, Severity.DEFAULT, minLogs + 2);
  }
}
