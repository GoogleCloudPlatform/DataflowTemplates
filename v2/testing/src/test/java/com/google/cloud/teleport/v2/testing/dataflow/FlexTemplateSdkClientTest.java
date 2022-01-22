package com.google.cloud.teleport.v2.testing.dataflow;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.Dataflow.Projects;
import com.google.api.services.dataflow.Dataflow.Projects.Jobs.Get;
import com.google.api.services.dataflow.Dataflow.Projects.Jobs.Update;
import com.google.api.services.dataflow.Dataflow.Projects.Locations.FlexTemplates;
import com.google.api.services.dataflow.Dataflow.Projects.Locations.FlexTemplates.Launch;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.LaunchFlexTemplateParameter;
import com.google.api.services.dataflow.model.LaunchFlexTemplateRequest;
import com.google.api.services.dataflow.model.LaunchFlexTemplateResponse;
import com.google.auth.Credentials;
import com.google.cloud.teleport.v2.testing.dataflow.FlexTemplateClient.JobInfo;
import com.google.cloud.teleport.v2.testing.dataflow.FlexTemplateClient.JobState;
import com.google.cloud.teleport.v2.testing.dataflow.FlexTemplateClient.Options;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class FlexTemplateSdkClientTest{
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS) private Dataflow client;

  private static final String PROJECT = "test-project";
  private static final String REGION = "us-east1";
  private static final String JOB_ID = "test-job-id";
  private static final String JOB_NAME = "test-job";
  private static final String SPEC_PATH = "gs://test-bucket/test-dir/test-spec.json";

  private static final String PARAM_KEY = "key";
  private static final String PARAM_VALUE = "value";

  @Captor private ArgumentCaptor<String> projectCaptor;
  @Captor private ArgumentCaptor<String> regionCaptor;
  @Captor private ArgumentCaptor<String> jobIdCaptor;
  @Captor private ArgumentCaptor<LaunchFlexTemplateRequest> requestCaptor;
  @Captor private ArgumentCaptor<Job> jobCaptor;

  @Test
  public void testCreateWithCredentials() {
    Credentials credentials = mock(Credentials.class);
    FlexTemplateSdkClient.builder().setCredentials(credentials).build();
    // Lack of exception is all we really can test
  }

  @Test
  public void testCreateWithNullCredentials() {
    FlexTemplateSdkClient.builder().setCredentials(null).build();
    // Lack of exception is all we really can test
  }

  @Test
  public void testLaunchNewJob() throws IOException {
    // Arrange
    Launch launch = mock(Launch.class);
    Job job = new Job().setId(JOB_ID).setCurrentState(JobState.QUEUED.toString());
    LaunchFlexTemplateResponse response = new LaunchFlexTemplateResponse().setJob(job);

    Options options = new Options(JOB_NAME, SPEC_PATH)
        .setIsStreaming(true)
        .addParameter(PARAM_KEY, PARAM_VALUE);

    when(getFlexTemplates(client).launch(projectCaptor.capture(), regionCaptor.capture(), requestCaptor.capture())).thenReturn(launch);
    when(launch.execute()).thenReturn(response);

    // Act
    JobInfo actual = FlexTemplateSdkClient.withDataflowClient(client).launchNewJob(PROJECT, REGION, options);

    // Assert
    JobInfo expected = JobInfo.builder()
        .setJobId(JOB_ID)
        .setState(JobState.QUEUED)
        .build();

    LaunchFlexTemplateRequest expectedRequest = new LaunchFlexTemplateRequest()
        .setLaunchParameter(new LaunchFlexTemplateParameter()
            .setJobName(JOB_NAME)
            .setContainerSpecGcsPath(SPEC_PATH)
            .setParameters(ImmutableMap.of(PARAM_KEY, PARAM_VALUE, "isStreaming", "true")));

    assertThat(actual).isEqualTo(expected);
    assertThat(projectCaptor.getValue()).isEqualTo(PROJECT);
    assertThat(regionCaptor.getValue()).isEqualTo(REGION);
    assertThat(requestCaptor.getValue()).isEqualTo(expectedRequest);
  }

  @Test(expected = IOException.class)
  public void testLaunchNewJobThrowsException() throws IOException {
    when(getFlexTemplates(client).launch(any(), any(), any())).thenThrow(new IOException());
    FlexTemplateSdkClient.withDataflowClient(client).launchNewJob(PROJECT, REGION, new Options(JOB_NAME, SPEC_PATH));
  }

  @Test
  public void testGetJobStatus() throws IOException {
    Get get = mock(Get.class);
    Job job = new Job().setCurrentState(JobState.RUNNING.toString());
    when(getProjectJobs(client).get(projectCaptor.capture(), jobIdCaptor.capture())).thenReturn(get);
    when(get.execute()).thenReturn(job);

    JobState actual = FlexTemplateSdkClient.withDataflowClient(client).getJobStatus(PROJECT, JOB_ID);

    assertThat(actual).isEqualTo(JobState.RUNNING);
    assertThat(projectCaptor.getValue()).isEqualTo(PROJECT);
    assertThat(jobIdCaptor.getValue()).isEqualTo(JOB_ID);
  }

  @Test(expected = IOException.class)
  public void testGetJobThrowsException() throws IOException {
    when(getProjectJobs(client).get(any(), any())).thenThrow(new IOException());
    FlexTemplateSdkClient.withDataflowClient(client).getJobStatus(PROJECT, JOB_ID);
  }

  @Test
  public void testCancelJob() throws IOException {
    Update update = mock(Update.class);
    when(getProjectJobs(client).update(projectCaptor.capture(), jobIdCaptor.capture(),
        jobCaptor.capture())).thenReturn(update);
    when(update.execute()).thenReturn(new Job());

    FlexTemplateSdkClient.withDataflowClient(client).cancelJob(PROJECT, JOB_ID);

    assertThat(projectCaptor.getValue()).isEqualTo(PROJECT);
    assertThat(jobIdCaptor.getValue()).isEqualTo(JOB_ID);
    assertThat(jobCaptor.getValue().getCurrentState()).isEqualTo(JobState.CANCELLED.toString());
  }

  @Test(expected = IOException.class)
  public void testCancelJobThrowsException() throws IOException {
    when(getProjectJobs(client).update(any(), any(), any())).thenThrow(new IOException());
    FlexTemplateSdkClient.withDataflowClient(client).cancelJob(PROJECT, JOB_ID);
  }

  private static Projects.Jobs getProjectJobs(Dataflow client) {
    return client.projects().jobs();
  }

  private static FlexTemplates getFlexTemplates(Dataflow client) {
    return client.projects().locations().flexTemplates();
  }
}