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
package com.google.cloud.teleport.it.launcher;

import static com.google.cloud.teleport.it.logging.LogStrings.formatForLogging;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default class for implementation of {@link PipelineLauncher} interface. */
public class DefaultPipelineLauncher extends AbstractPipelineLauncher {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultPipelineLauncher.class);
  private static final Pattern JOB_ID_PATTERN = Pattern.compile("Submitted job: (\\S+)");

  private DefaultPipelineLauncher(DefaultPipelineLauncher.Builder builder) {
    super(
        new Dataflow(
            Utils.getDefaultTransport(),
            Utils.getDefaultJsonFactory(),
            builder.getCredentials() == null
                ? null
                : new HttpCredentialsAdapter(builder.getCredentials())));
  }

  public static DefaultPipelineLauncher.Builder builder() {
    return new DefaultPipelineLauncher.Builder();
  }

  @Override
  public LaunchInfo launch(String project, String region, LaunchConfig options) throws IOException {
    checkState(
        options.sdk() != null,
        "Cannot launch a dataflow job "
            + "without sdk specified. Please specify sdk and try again!");
    LOG.info("Getting ready to launch {} in {} under {}", options.jobName(), region, project);
    LOG.info("Using parameters:\n{}", formatForLogging(options.parameters()));
    // Create SDK specific command and execute to launch dataflow job
    List<String> cmd = new ArrayList<>();
    String jobId = null;
    switch (options.sdk()) {
      case JAVA:
        checkState(
            options.pipeline() != null,
            "Cannot launch a dataflow job "
                + "without pipeline specified. Please specify pipeline and try again!");
        cmd = extractOptions(project, region, options);
        DataflowPipelineJob job =
            (DataflowPipelineJob) options.pipeline().runWithAdditionalOptionArgs(cmd);
        jobId = job.getJobId();
        break;
      case PYTHON:
        checkState(
            options.executable() != null,
            "Cannot launch a dataflow job "
                + "without executable specified. Please specify executable and try again!");
        LOG.info("Using the executable at {}", options.executable());
        cmd.add("python3");
        cmd.add(options.executable());
        cmd.addAll(extractOptions(project, region, options));
        jobId = executeCommandAndParseResponse(cmd);
        break;
      case GO:
        checkState(
            options.executable() != null,
            "Cannot launch a dataflow job "
                + "without executable specified. Please specify executable and try again!");
        LOG.info("Using the executable at {}", options.executable());
        cmd.add("go");
        cmd.add("run");
        cmd.add(options.executable());
        cmd.addAll(extractOptions(project, region, options));
        jobId = executeCommandAndParseResponse(cmd);
        break;
      default:
        throw new RuntimeException(
            String.format(
                "Invalid sdk %s specified. " + "sdk can be one of java, python, or go.",
                options.sdk()));
    }
    // Wait until the job is active to get more information
    JobState state = waitUntilActive(project, region, jobId);
    Job job = getJob(project, region, jobId);
    return getJobInfo(options, state, job, options.getParameter("runner"));
  }

  private List<String> extractOptions(String project, String region, LaunchConfig options) {
    List<String> additionalOptions = new ArrayList<>();
    for (Map.Entry<String, String> parameter : options.parameters().entrySet()) {
      additionalOptions.add(String.format("--%s=%s", parameter.getKey(), parameter.getValue()));
    }
    additionalOptions.add(String.format("--project=%s", project));
    additionalOptions.add(String.format("--region=%s", region));
    return additionalOptions;
  }

  /** Executes the specified command and parses the response to get the Job ID. */
  private String executeCommandAndParseResponse(List<String> cmd) throws IOException {
    Process process = new ProcessBuilder().command(cmd).redirectErrorStream(true).start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    Matcher m = JOB_ID_PATTERN.matcher(output);
    if (!m.find()) {
      throw new RuntimeException(
          String.format(
              "Dataflow output in unexpected format. Failed to parse Dataflow Job ID. "
                  + "Result from process: %s",
              output));
    }
    String jobId = m.group(1);
    LOG.info("Submitted job: {}", jobId);
    return jobId;
  }

  /** Builder for {@link DefaultPipelineLauncher}. */
  public static final class Builder {
    private Credentials credentials;

    private Builder() {}

    public Credentials getCredentials() {
      return credentials;
    }

    public DefaultPipelineLauncher.Builder setCredentials(Credentials value) {
      credentials = value;
      return this;
    }

    public DefaultPipelineLauncher build() {
      return new DefaultPipelineLauncher(this);
    }
  }
}
