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
package org.apache.beam.it.gcp.dataflow.conditions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.conditions.ConditionCheck;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link DataflowMetricCounterCheck}. */
@RunWith(JUnit4.class)
public class DataflowMetricCounterCheckTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private PipelineLauncher pipelineLauncher;
  @Mock private PipelineLauncher.LaunchInfo launchInfo;

  private static final String PROJECT = "test-project";
  private static final String REGION = "us-central1";
  private static final String JOB_ID = "test-job-id";
  private static final String COUNTER_NAME = "test-counter";

  @Before
  public void setUp() {
    when(launchInfo.projectId()).thenReturn(PROJECT);
    when(launchInfo.region()).thenReturn(REGION);
    when(launchInfo.jobId()).thenReturn(JOB_ID);
  }

  @Test
  public void testCheck_minCounterMet() throws IOException {
    when(pipelineLauncher.getMetric(PROJECT, REGION, JOB_ID, COUNTER_NAME)).thenReturn(10.0);
    DataflowMetricCounterCheck check =
        DataflowMetricCounterCheck.builder(pipelineLauncher, launchInfo)
            .setCounterName(COUNTER_NAME)
            .setMinCounterValue(5)
            .build();
    ConditionCheck.CheckResult result = check.check();
    assertTrue(result.isSuccess());
    assertEquals("Expected 'test-counter' to be at least 5 and was 10", result.getMessage());
  }

  @Test
  public void testCheck_minCounterNotMet() throws IOException {
    when(pipelineLauncher.getMetric(PROJECT, REGION, JOB_ID, COUNTER_NAME)).thenReturn(4.0);
    DataflowMetricCounterCheck check =
        DataflowMetricCounterCheck.builder(pipelineLauncher, launchInfo)
            .setCounterName(COUNTER_NAME)
            .setMinCounterValue(5)
            .build();
    ConditionCheck.CheckResult result = check.check();
    assertFalse(result.isSuccess());
    assertEquals("Expected 'test-counter' to be at least 5 but was 4", result.getMessage());
  }
}
