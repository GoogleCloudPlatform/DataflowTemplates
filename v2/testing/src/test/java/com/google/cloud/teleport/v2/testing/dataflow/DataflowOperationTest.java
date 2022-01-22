package com.google.cloud.teleport.v2.testing.dataflow;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.testing.dataflow.DataflowOperation.Config;
import com.google.cloud.teleport.v2.testing.dataflow.DataflowOperation.Result;
import com.google.cloud.teleport.v2.testing.dataflow.FlexTemplateClient.JobState;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class DataflowOperationTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock FlexTemplateClient client;

  private static final String PROJECT = "test-project";
  private static final String JOB_ID = "test-job-id";
  private static final Duration CHECK_AFTER = Duration.ofMillis(10);
  private static final Duration TIMEOUT_AFTER = Duration.ofMillis(100);

  private static final Config DEFAULT_CONFIG = Config.builder()
      .setProject(PROJECT)
      .setJobId(JOB_ID)
      .setCheckAfter(CHECK_AFTER)
      .setTimeoutAfter(TIMEOUT_AFTER)
      .build();

  @Captor public ArgumentCaptor<String> projectCaptor;
  @Captor public ArgumentCaptor<String> jobIdCaptor;

  @Test
  public void testWaitUntilDone() throws IOException {
    // Arrange
    when(client.getJobStatus(any(), any()))
        .thenReturn(JobState.QUEUED)
        .thenReturn(JobState.RUNNING)
        .thenReturn(JobState.CANCELLING)
        .thenReturn(JobState.CANCELLED);

    // Act
    Result result = DataflowOperation.waitUntilDone(client, DEFAULT_CONFIG);

    // Assert
    verify(client, times(4))
        .getJobStatus(projectCaptor.capture(), jobIdCaptor.capture());

    Set<String> allProjects = new HashSet<>(projectCaptor.getAllValues());
    Set<String> allJobIds = new HashSet<>(jobIdCaptor.getAllValues());

    assertThat(allProjects).containsExactly(PROJECT);
    assertThat(allJobIds).containsExactly(JOB_ID);
    assertThat(result).isEqualTo(Result.JOB_FINISHED);
  }

  @Test
  public void testWaitUntilDoneTimeout() throws IOException {
    when(client.getJobStatus(any(), any())).thenReturn(JobState.RUNNING);
    Result result = DataflowOperation.waitUntilDone(client, DEFAULT_CONFIG);
    assertThat(result).isEqualTo(Result.TIMEOUT);
  }

  @Test
  public void testWaitForCondition() throws IOException {
    AtomicInteger callCount = new AtomicInteger();
    int totalCalls = 3;
    Supplier<Boolean> checker = () -> callCount.incrementAndGet() >= totalCalls;
    when(client.getJobStatus(any(), any()))
        .thenReturn(JobState.RUNNING)
        .thenThrow(new IOException())
        .thenReturn(JobState.RUNNING);

    Result result = DataflowOperation.waitForCondition(client, DEFAULT_CONFIG, checker);

    verify(client, atMost(totalCalls)).getJobStatus(projectCaptor.capture(), jobIdCaptor.capture());
    assertThat(projectCaptor.getValue()).isEqualTo(PROJECT);
    assertThat(jobIdCaptor.getValue()).isEqualTo(JOB_ID);
    assertThat(result).isEqualTo(Result.CONDITION_MET);
  }

  @Test
  public void testWaitForConditionJobFinished() throws IOException {
    when(client.getJobStatus(any(), any()))
        .thenReturn(JobState.RUNNING)
        .thenReturn(JobState.CANCELLED);

    Result result = DataflowOperation.waitForCondition(client, DEFAULT_CONFIG, () -> false);

    assertThat(result).isEqualTo(Result.JOB_FINISHED);
  }

  @Test
  public void testWaitForConditionTimeout() throws IOException {
    when(client.getJobStatus(any(), any())).thenReturn(JobState.RUNNING);

    Result result = DataflowOperation.waitForCondition(client, DEFAULT_CONFIG, () -> false);

    assertThat(result).isEqualTo(Result.TIMEOUT);
  }

  @Test
  public void testFinishAfterCondition() throws IOException {
    // Arrange
    AtomicInteger callCount = new AtomicInteger();
    int totalCalls = 3;
    Supplier<Boolean> checker = () -> callCount.incrementAndGet() >= totalCalls;

    when(client.getJobStatus(any(), any()))
        .thenReturn(JobState.RUNNING)
        .thenThrow(new IOException())
        .thenReturn(JobState.RUNNING)
        .thenReturn(JobState.CANCELLING)
        .thenReturn(JobState.CANCELLED);
    doAnswer(invocation -> null).when(client).cancelJob(any(), any());

    // Act
    Result result = DataflowOperation.waitForConditionAndFinish(client, DEFAULT_CONFIG, checker);

    // Assert
    verify(client, atLeast(totalCalls))
        .getJobStatus(projectCaptor.capture(), jobIdCaptor.capture());
    verify(client).cancelJob(projectCaptor.capture(), jobIdCaptor.capture());

    Set<String> allProjects = new HashSet<>(projectCaptor.getAllValues());
    Set<String> allJobIds = new HashSet<>(jobIdCaptor.getAllValues());

    assertThat(allProjects).containsExactly(PROJECT);
    assertThat(allJobIds).containsExactly(JOB_ID);
    assertThat(result).isEqualTo(Result.CONDITION_MET);
  }

  @Test
  public void testFinishAfterConditionJobStopped() throws IOException {
    when(client.getJobStatus(any(), any()))
        .thenReturn(JobState.RUNNING)
        .thenReturn(JobState.CANCELLED);
    doAnswer(invocation -> null).when(client).cancelJob(projectCaptor.capture(), jobIdCaptor.capture());

    Result result = DataflowOperation.waitForCondition(client, DEFAULT_CONFIG, () -> false);

    verify(client, never()).cancelJob(any(), any());
    assertThat(result).isEqualTo(Result.JOB_FINISHED);
  }

  @Test
  public void testFinishAfterConditionTimeout() throws IOException {
    when(client.getJobStatus(any(), any()))
        .thenReturn(JobState.RUNNING);
    doAnswer(invocation -> null).when(client).cancelJob(projectCaptor.capture(), jobIdCaptor.capture());

    Result result = DataflowOperation.waitForCondition(client, DEFAULT_CONFIG, () -> false);

    verify(client, never()).cancelJob(any(), any());
    assertThat(result).isEqualTo(Result.TIMEOUT);
  }
}