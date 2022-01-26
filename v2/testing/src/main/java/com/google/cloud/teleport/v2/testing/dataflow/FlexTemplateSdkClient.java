/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.testing.dataflow;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.LaunchFlexTemplateParameter;
import com.google.api.services.dataflow.model.LaunchFlexTemplateRequest;
import com.google.api.services.dataflow.model.LaunchFlexTemplateResponse;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.common.base.Strings;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client for interacting with Dataflow Flex Templates using the Dataflow SDK. */
public final class FlexTemplateSdkClient implements FlexTemplateClient {
  private static final Logger LOG = LoggerFactory.getLogger(FlexTemplateSdkClient.class);

  private final Dataflow client;

  private FlexTemplateSdkClient(Builder builder) {
    this.client =
        new Dataflow(
            Utils.getDefaultTransport(),
            Utils.getDefaultJsonFactory(),
            new HttpCredentialsAdapter(builder.getCredentials()));
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

  @Override
  public JobInfo launchNewJob(String project, String region, LaunchOptions options)
      throws IOException {
    LOG.info("Getting ready to launch {} in {} under {}", options.jobName(), region, project);
    LOG.info("Using the spec at {}", options.specPath());
    LOG.info("Using parameters:\n{}", options.parameters());

    LaunchFlexTemplateParameter parameter =
        new LaunchFlexTemplateParameter()
            .setJobName(options.jobName())
            .setParameters(options.parameters())
            .setContainerSpecGcsPath(options.specPath());
    LaunchFlexTemplateRequest request =
        new LaunchFlexTemplateRequest().setLaunchParameter(parameter);
    LOG.info("Sending request:\n{}", request.toPrettyString());

    LaunchFlexTemplateResponse response =
        client.projects().locations().flexTemplates().launch(project, region, request).execute();
    LOG.info("Received response:\n{}", response.toPrettyString());

    Job job = response.getJob();
    // The initial response will not return the state, so need to explicitly get it
    JobState state = getJobStatus(project, region, job.getId());
    return JobInfo.builder().setJobId(job.getId()).setState(state).build();
  }

  @Override
  public JobState getJobStatus(String project, String region, String jobId) throws IOException {
    LOG.info("Getting the status of {} under {}", jobId, project);

    Job job = client.projects().locations().jobs().get(project, region, jobId).execute();
    LOG.info("Received job on get request for {}:\n{}", jobId, job.toPrettyString());
    return handleJobState(job);
  }

  @Override
  public void cancelJob(String project, String region, String jobId)
      throws IOException {
    LOG.info("Cancelling {} under {}", jobId, project);
    Job job = new Job().setRequestedState(JobState.CANCELLED.toString());
    LOG.info("Sending job to update {}:\n{}", jobId, job.toPrettyString());
    client.projects().locations().jobs().update(project, region, jobId, job).execute();
  }

  private static JobState handleJobState(Job job) {
    String currentState = job.getCurrentState();
    return Strings.isNullOrEmpty(currentState) ? JobState.UNKNOWN : JobState.parse(currentState);
  }

  /** Builder for {@link FlexTemplateSdkClient}. */
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
