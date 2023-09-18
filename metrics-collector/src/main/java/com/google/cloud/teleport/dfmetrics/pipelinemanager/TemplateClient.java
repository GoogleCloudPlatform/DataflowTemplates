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
package com.google.cloud.teleport.dfmetrics.pipelinemanager;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.cloud.teleport.dfmetrics.model.JobConfig;
import java.io.IOException;

/** Abstract class covering the common operations between Classic and Flex templates. */
public abstract class TemplateClient {

  public static TemplateClient create(String templateType, Dataflow dataflowClient) {
    // Flex Template
    if (templateType.equalsIgnoreCase("flex")) {
      return new FlexTemplateClient(dataflowClient);
    }

    // Classic Template
    return new ClassicTemplateClient(dataflowClient);
  }

  /** Builds Template config for launching the pipeline. */
  public TemplateConfig buildTemplateConfig(JobConfig jobConfig) {
    return TemplateConfig.builder(jobConfig.templateSpec())
        .setjobName(jobConfig.jobName())
        .setsdk(TemplateConfig.Sdk.JAVA)
        .setparameters(jobConfig.pipelineOptions())
        .setenvironment(jobConfig.environmentOptions())
        .build();
  }

  /**
   * Launches a new Dataflow job.
   *
   * @param project the project to run the job in
   * @param region the region to run the job in (e.g. us-east1)
   * @param templateConfig options for configuring the job
   * @return info about the request to launch a new job
   * @throws IOException if there is an issue sending the request
   */
  public abstract Job launch(String project, String region, TemplateConfig templateConfig)
      throws IOException;
}
