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
package org.apache.beam.it.gcp.dataflow;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.dataflow.AbstractPipelineLauncher.CAPACITY_RETRIES_PROPERTY;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMessage;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher.JobState;
import org.apache.beam.it.gcp.dataflow.AbstractPipelineLauncher.ActiveJob;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for the Dataflow capacity retry in {@link AbstractPipelineLauncher}. */
@RunWith(JUnit4.class)
public final class AbstractPipelineLauncherCapacityRetryTest {

  private static final String PROJECT = "test-project";
  private static final String REGION = "us-central1";
  private static final String CAPACITY_ERROR =
      "Due to a temporary capacity issue, Dataflow failed to schedule a backend for executing the"
          + " job. Retry your job submission at a later time.";

  private AbstractPipelineLauncher launcher;
  private Deque<String> jobIds;
  private int submissions;

  @Before
  public void setUp() throws Exception {
    System.clearProperty(CAPACITY_RETRIES_PROPERTY);
    launcher = spy(FlexTemplateClient.withDataflowClient(mock(Dataflow.class)));
    doNothing().when(launcher).sleepBeforeCapacityRetry(anyLong());
    jobIds = new ArrayDeque<>();
    submissions = 0;
  }

  @After
  public void tearDown() {
    System.clearProperty(CAPACITY_RETRIES_PROPERTY);
  }

  private Job submit() {
    submissions++;
    return new Job().setId(jobIds.pop());
  }

  private void stubState(String jobId, JobState state) throws IOException {
    doReturn(state).when(launcher).getJobStatus(PROJECT, REGION, jobId);
  }

  private void stubErrors(String jobId, String... texts) {
    List<JobMessage> messages = new ArrayList<>();
    for (String text : texts) {
      messages.add(new JobMessage().setMessageText(text));
    }
    doReturn(messages).when(launcher).listMessages(PROJECT, REGION, jobId, "JOB_MESSAGE_ERROR");
  }

  @Test
  public void disabledByDefault_capacityFailureIsNotRetried() throws IOException {
    jobIds.add("job-1");
    stubState("job-1", JobState.FAILED);
    stubErrors("job-1", CAPACITY_ERROR);

    assertThrows(
        RuntimeException.class,
        () -> launcher.submitAndWaitUntilActive(PROJECT, REGION, this::submit));

    assertThat(submissions).isEqualTo(1);
    // With retries disabled we must not even look at the job messages.
    verify(launcher, never()).listMessages(anyString(), anyString(), anyString(), anyString());
  }

  @Test
  public void enabled_capacityFailureIsRetriedUntilRunning() throws Exception {
    System.setProperty(CAPACITY_RETRIES_PROPERTY, "3");
    jobIds.addAll(Arrays.asList("job-1", "job-2", "job-3"));
    stubState("job-1", JobState.FAILED);
    stubErrors("job-1", CAPACITY_ERROR);
    stubState("job-2", JobState.FAILED);
    stubErrors("job-2", "some other error", CAPACITY_ERROR);
    stubState("job-3", JobState.RUNNING);

    ActiveJob activeJob = launcher.submitAndWaitUntilActive(PROJECT, REGION, this::submit);

    assertThat(activeJob.job.getId()).isEqualTo("job-3");
    assertThat(activeJob.state).isEqualTo(JobState.RUNNING);
    assertThat(submissions).isEqualTo(3);
    verify(launcher).sleepBeforeCapacityRetry(60);
    verify(launcher).sleepBeforeCapacityRetry(120);
  }

  @Test
  public void enabled_nonCapacityFailureIsNotRetried() throws IOException {
    System.setProperty(CAPACITY_RETRIES_PROPERTY, "3");
    jobIds.add("job-1");
    stubState("job-1", JobState.FAILED);
    stubErrors("job-1", "Workflow failed. Causes: some pipeline bug");

    assertThrows(
        RuntimeException.class,
        () -> launcher.submitAndWaitUntilActive(PROJECT, REGION, this::submit));

    assertThat(submissions).isEqualTo(1);
  }

  @Test
  public void enabled_givesUpAfterMaxRetries() throws Exception {
    System.setProperty(CAPACITY_RETRIES_PROPERTY, "2");
    jobIds.addAll(Arrays.asList("job-1", "job-2", "job-3"));
    for (String id : jobIds) {
      stubState(id, JobState.FAILED);
      stubErrors(id, CAPACITY_ERROR);
    }

    assertThrows(
        RuntimeException.class,
        () -> launcher.submitAndWaitUntilActive(PROJECT, REGION, this::submit));

    assertThat(submissions).isEqualTo(3);
    verify(launcher, times(2)).sleepBeforeCapacityRetry(anyLong());
  }

  @Test
  public void enabled_messageLookupFailureIsNotRetried() throws IOException {
    System.setProperty(CAPACITY_RETRIES_PROPERTY, "3");
    jobIds.add("job-1");
    stubState("job-1", JobState.FAILED);
    doReturn(null).when(launcher).listMessages(eq(PROJECT), eq(REGION), eq("job-1"), anyString());

    assertThrows(
        RuntimeException.class,
        () -> launcher.submitAndWaitUntilActive(PROJECT, REGION, this::submit));

    assertThat(submissions).isEqualTo(1);
  }

  @Test
  public void capacityRetries_parsesProperty() {
    assertThat(AbstractPipelineLauncher.capacityRetries()).isEqualTo(0);
    System.setProperty(CAPACITY_RETRIES_PROPERTY, "4");
    assertThat(AbstractPipelineLauncher.capacityRetries()).isEqualTo(4);
    System.setProperty(CAPACITY_RETRIES_PROPERTY, "-1");
    assertThat(AbstractPipelineLauncher.capacityRetries()).isEqualTo(0);
    System.setProperty(CAPACITY_RETRIES_PROPERTY, "abc");
    assertThat(AbstractPipelineLauncher.capacityRetries()).isEqualTo(0);
    System.setProperty(CAPACITY_RETRIES_PROPERTY, "");
    assertThat(AbstractPipelineLauncher.capacityRetries()).isEqualTo(0);
  }
}
