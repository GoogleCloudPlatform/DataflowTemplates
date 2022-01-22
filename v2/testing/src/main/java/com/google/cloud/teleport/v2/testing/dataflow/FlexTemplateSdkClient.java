package com.google.cloud.teleport.v2.testing.dataflow;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.LaunchFlexTemplateParameter;
import com.google.api.services.dataflow.model.LaunchFlexTemplateRequest;
import com.google.api.services.dataflow.model.LaunchFlexTemplateResponse;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import java.io.IOException;
import java.time.Duration;

/** Client for interacting with Dataflow Flex Templates using the Dataflow SDK. */
public final class FlexTemplateSdkClient implements FlexTemplateClient {

  private final Dataflow client;

  // TODO(zhoufek): Let users set this in options.
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

  private FlexTemplateSdkClient(Builder builder) {
    HttpRequestInitializer initializer = builder.getCredentials() != null
        ? new HttpCredentialsAdapter(builder.getCredentials())
        : request -> {
          request.setConnectTimeout((int) REQUEST_TIMEOUT.toMillis());
          request.setReadTimeout((int) REQUEST_TIMEOUT.toMillis());
          request.setWriteTimeout((int) REQUEST_TIMEOUT.toMillis());
        };

    this.client = new Dataflow(
        Utils.getDefaultTransport(),
        Utils.getDefaultJsonFactory(),
        initializer);
  }

  private FlexTemplateSdkClient(Dataflow dataflow) {
    this.client = dataflow;
  }

  public static FlexTemplateSdkClient withDataflowClient(Dataflow dataflow) {
    return new FlexTemplateSdkClient(dataflow);
  }

  public static Builder builder() {
    return new Builder();
  }

  public JobInfo launchNewJob(String project, String region, Options options) throws IOException {
    LaunchFlexTemplateRequest request = new LaunchFlexTemplateRequest();
    LaunchFlexTemplateParameter parameter = new LaunchFlexTemplateParameter()
        .setJobName(options.jobName())
        .setParameters(options.parameters())
        .setContainerSpecGcsPath(options.specPath());
    LaunchFlexTemplateResponse response = client.projects()
        .locations()
        .flexTemplates()
        .launch(project, region, request.setLaunchParameter(parameter))
        .execute();
    Job job = response.getJob();
    return JobInfo.builder()
        .setJobId(job.getId())
        .setState(JobState.parse(job.getCurrentState()))
        .build();
  }

  public JobState getJobStatus(String project, String jobId) throws IOException {
    Job job = client.projects().jobs().get(project, jobId).execute();
    return JobState.parse(job.getCurrentState());
  }

  public void cancelJob(String project, String jobId) throws IOException {
    Job job = new Job().setCurrentState(JobState.CANCELLED.toString());
    client.projects().jobs().update(project, jobId, job).execute();
  }

  public static final class Builder {
    private Credentials credentials;

    private Builder() {}

    public Credentials getCredentials() {
      return credentials;
    }

    public Builder setCredentials(Credentials value) {
      credentials = value;
      return this;
    }

    public FlexTemplateSdkClient build() {
      return new FlexTemplateSdkClient(this);
    }
  }
}
