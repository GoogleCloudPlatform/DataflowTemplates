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
package com.google.cloud.teleport.dfmetrics.commands;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.cloud.teleport.dfmetrics.model.CommandLineArgs;
import com.google.cloud.teleport.dfmetrics.model.JobConfig;
import com.google.cloud.teleport.dfmetrics.model.JobInfo;
import com.google.cloud.teleport.dfmetrics.model.TemplateLauncherConfig;
import com.google.cloud.teleport.dfmetrics.output.IOutputStore;
import com.google.cloud.teleport.dfmetrics.pipelinemanager.DataflowJobManager;
import com.google.cloud.teleport.dfmetrics.pipelinemanager.JobState;
import com.google.cloud.teleport.dfmetrics.pipelinemanager.TemplateClient;
import com.google.cloud.teleport.dfmetrics.pipelinemanager.TemplateConfig;
import com.google.cloud.teleport.dfmetrics.utils.ConfigValidator;
import com.google.cloud.teleport.dfmetrics.utils.MetricsCollectorUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link TemplateLauncherCommand} launches the template, fetches Dataflow metrics from Cloud
 * Monitoring and writes to appropriate output store.
 */
public class TemplateLauncherCommand extends Command {

  private static final Logger LOG = LoggerFactory.getLogger(TemplateLauncherCommand.class);

  private final CommandLineArgs commandargs;
  private final Dataflow dataflowClient;

  private final IOutputStore outputStore;

  private JobConfig jobConfig;
  private TemplateClient templateClient;
  private TemplateConfig templateConfig;

  public TemplateLauncherCommand(
      CommandLineArgs args, Dataflow dataflowClient, IOutputStore outputStore) {
    this.commandargs = args;
    this.dataflowClient = dataflowClient;
    this.outputStore = outputStore;
  }

  /**
   * Loads & validates the supplied input configuration file and converts to jobconfig. Builds
   * appropriate template client based on the given template type (i.e classic or flex).
   */
  @Override
  public void initialize() {
    try {
      TemplateLauncherConfig templateLauncherConfig = loadConfig(commandargs.configFile);
      ConfigValidator.validateTemplateLauncherConfig(templateLauncherConfig);
      LOG.info("Creating Job Config from supplied config file:{}", commandargs.configFile);
      this.jobConfig = createJobConfig(templateLauncherConfig);
      LOG.info("Creating pipeline client");
      templateClient = TemplateClient.create(jobConfig.templateType(), this.dataflowClient);
      templateConfig = templateClient.buildTemplateConfig(jobConfig);
    } catch (IOException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /** Creates a JobInfo object from the provided parameters. */
  public JobInfo getLaunchedJobInfo(
      DataflowJobManager jobManager, JobConfig jobConfig, TemplateConfig templateConfig)
      throws IOException {

    LOG.info("Job started running..");
    JobInfo.Builder jobInfoBuilder = jobManager.getJobInfoBuilder(jobConfig);

    // Combine the pipeline options and environment options supplied
    Map<String, String> parameters = new HashMap<>(templateConfig.parameters());
    templateConfig.environment().forEach((key, val) -> parameters.put(key, val.toString()));

    jobInfoBuilder.setParameters(ImmutableMap.copyOf(parameters));
    return jobInfoBuilder.build();
  }

  /**
   * Launches the template, fetches the job metrics and stores the results in appropriate output
   * store.
   */
  @Override
  public void run() {
    LOG.info("Launching the dataflow job in project:{}", jobConfig.projectId());
    try {
      Job dataflowJob =
          templateClient.launch(jobConfig.projectId(), jobConfig.location(), templateConfig);
      LOG.info(
          "Launched dataflow job with jobid:{} in project:{}",
          dataflowJob.getId(),
          dataflowJob.getProjectId());

      DataflowJobManager jobManager =
          DataflowJobManager.builder(this.dataflowClient, dataflowJob)
              .setMaxTimeOut(Duration.ofMinutes(jobConfig.timeoutInMinutes()))
              .build();

      jobManager.printJobResponse();

      // Wait until the job is active to get more information
      Job job = jobManager.waitUntilActive();

      // Refresh the job manager with latest job view
      jobManager = jobManager.withJob(job);

      // Get the job info
      JobInfo jobInfo = getLaunchedJobInfo(jobManager, jobConfig, templateConfig);

      LOG.info("Waiting for the job to finish or timeout");
      DataflowJobManager.ExecutionStatus executionStatus =
          jobInfo.isBatchJob()
              ? jobManager.waitUntilDoneOrCancelAfterTimeOut()
              : jobManager.waitForDurationAndCancelJob();

      LOG.info("Completed waiting for the job");
      if (executionStatus == DataflowJobManager.ExecutionStatus.TIMEOUT) {
        ImmutableSet<JobState> terminalJobStates =
            ImmutableSet.<JobState>builder()
                .addAll(JobState.DONE_STATES)
                .addAll(JobState.FINISHING_STATES)
                .build();

        String expectedJobStates =
            terminalJobStates.stream().map(JobState::toString).collect(Collectors.joining(","));
        throw new RuntimeException(
            String.format("Job failed to reach one of the terminal states: %s", expectedJobStates));
      }

      LOG.info("Fetching job metrics..");
      Map<String, Double> metrics = jobManager.getMetrics();

      if (jobConfig.resourcePricing() != null) {
        LOG.info("Estimating job cost..");
        metrics.put("EstimatedJobCost", jobConfig.resourcePricing().estimateJobCost(metrics));
      }

      LOG.info("Storing the results in the output store..");
      this.outputStore.load(jobInfo, metrics);

      LOG.info("Completed all operations");

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Parses the supplied configuration file and creates job config. */
  @VisibleForTesting
  JobConfig createJobConfig(TemplateLauncherConfig templateLauncherConfig) throws IOException {
    String jobName =
        templateLauncherConfig.getJobName() != null
            ? templateLauncherConfig.getJobName()
            : MetricsCollectorUtils.sanitizeJobName(templateLauncherConfig.getJobPrefix());
    return JobConfig.builder()
        .setProjectId(templateLauncherConfig.getProject())
        .setLocation(templateLauncherConfig.getRegion())
        .setTemplateName(templateLauncherConfig.getTemplateName())
        .setTemplateVersion(templateLauncherConfig.getTemplateVersion())
        .setTemplateType(templateLauncherConfig.getTemplateType())
        .setTemplateSpec(templateLauncherConfig.getTemplateSpec())
        .setJobName(jobName)
        .setTimeoutInMinutes(templateLauncherConfig.getTimeoutInMinutes())
        .setPipelineOptions(templateLauncherConfig.getPipelineOptions())
        .setEnvironmentOptions(templateLauncherConfig.getEnvironmentOptions())
        .setResourcePricing(templateLauncherConfig.getResourcePricing())
        .build();
  }

  /** Loads the configuration from the supplied configuration file. */
  private TemplateLauncherConfig loadConfig(File filePath) throws IOException {
    FileReader reader = new FileReader(filePath);
    return new Gson().fromJson(reader, TemplateLauncherConfig.class);
  }
}
