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
import com.google.cloud.teleport.dfmetrics.model.CommandLineArgs;
import com.google.cloud.teleport.dfmetrics.model.JobConfig;
import com.google.cloud.teleport.dfmetrics.model.JobInfo;
import com.google.cloud.teleport.dfmetrics.model.MetricsFetcherConfig;
import com.google.cloud.teleport.dfmetrics.output.IOutputStore;
import com.google.cloud.teleport.dfmetrics.pipelinemanager.DataflowJobManager;
import com.google.cloud.teleport.dfmetrics.utils.ConfigValidator;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link MetricsFetcherCommand} fetches Dataflow metrics from Cloud Monitoring for a given job
 * and writes to appropriate output store.
 */
public class MetricsFetcherCommand extends Command {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsFetcherCommand.class);
  private JobConfig jobConfig;
  private final CommandLineArgs commandargs;
  private final Dataflow dataflowClient;
  private final IOutputStore outputStore;

  public MetricsFetcherCommand(
      CommandLineArgs args, Dataflow dataflowClient, IOutputStore outputStore) {
    this.commandargs = args;
    this.dataflowClient = dataflowClient;
    this.outputStore = outputStore;
  }

  /** Loads & validates the supplied input configuration file and converts to jobconfig. */
  @Override
  public void initialize() {
    try {
      MetricsFetcherConfig metricsFetcherConfig = loadConfig(commandargs.configFile);
      ConfigValidator.validateMetricsFetcherConfig(metricsFetcherConfig);
      LOG.info("Creating Job Config from supplied config file:{}", commandargs.configFile);
      this.jobConfig = createJobConfig(metricsFetcherConfig);
    } catch (IOException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /** Fetches the job metrics and stores the results in appropriate output store. */
  @Override
  public void run() {
    try {
      LOG.info("Creating Dataflow job manager..");
      DataflowJobManager jobManager =
          DataflowJobManager.builder(this.dataflowClient)
              .build()
              .withJob(jobConfig.projectId(), jobConfig.location(), jobConfig.jobId());

      LOG.info("Fetching job metrics..");
      Map<String, Double> metrics = jobManager.getMetrics();

      if (jobConfig.resourcePricing() != null) {
        LOG.info("Estimating job cost..");
        double estimatedJobCost = jobConfig.resourcePricing().estimateJobCost(metrics);
        metrics.put("EstimatedJobCost", estimatedJobCost);
      }

      JobInfo jobInfo = jobManager.getJobInfoBuilder(jobConfig).build();

      LOG.info("Storing the results in the output store..");
      this.outputStore.load(jobInfo, metrics);

      LOG.info("Completed all operations");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Creates Job Config from the metrics fetcher config. */
  @VisibleForTesting
  JobConfig createJobConfig(MetricsFetcherConfig metricsFetcherConfig) throws IOException {
    return JobConfig.builder()
        .setProjectId(metricsFetcherConfig.getProject())
        .setLocation(metricsFetcherConfig.getRegion())
        .setJobId(metricsFetcherConfig.getJobId())
        .setResourcePricing(metricsFetcherConfig.getResourcePricing())
        .build();
  }

  /** Loads the configuration from the supplied configuration file. */
  private MetricsFetcherConfig loadConfig(File filePath) throws IOException {
    FileReader reader = new FileReader(filePath);
    return new Gson().fromJson(reader, MetricsFetcherConfig.class);
  }
}
