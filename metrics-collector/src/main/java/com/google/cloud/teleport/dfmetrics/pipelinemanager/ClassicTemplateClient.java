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
import com.google.api.services.dataflow.model.CreateJobFromTemplateRequest;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.RuntimeEnvironment;
import com.google.cloud.teleport.dfmetrics.utils.MetricsCollectorUtils;
import com.google.cloud.teleport.dfmetrics.utils.RetryUtil;
import dev.failsafe.Failsafe;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class {@link ClassicTemplateClient} for interacting with Dataflow classic templates using the
 * Dataflow SDK.
 */
public final class ClassicTemplateClient extends TemplateClient {

  private static final Logger LOG = LoggerFactory.getLogger(ClassicTemplateClient.class);

  private final Dataflow dataflowClient;

  public ClassicTemplateClient(Dataflow dataflowClient) {
    this.dataflowClient = dataflowClient;
  }

  @Override
  public Job launch(String project, String region, TemplateConfig templateConfig)
      throws IOException {
    LOG.info(
        "Launching {} using spec {} in region {} under {}",
        templateConfig.jobName(),
        templateConfig.specPath(),
        region,
        project);

    CreateJobFromTemplateRequest templateRequest =
        new CreateJobFromTemplateRequest()
            .setJobName(templateConfig.jobName())
            .setParameters(templateConfig.parameters())
            .setLocation(region)
            .setGcsPath(templateConfig.specPath());

    if (templateConfig.environment() != null) {
      templateRequest.setEnvironment(buildEnvironment(templateConfig));
    }

    return Failsafe.with(RetryUtil.clientRetryPolicy())
        .get(
            () ->
                dataflowClient
                    .projects()
                    .locations()
                    .templates()
                    .create(project, region, templateRequest)
                    .execute());
  }

  /** Build run time environment of classic template. */
  private RuntimeEnvironment buildEnvironment(TemplateConfig templateConfig) {
    RuntimeEnvironment environment = new RuntimeEnvironment();
    Map<String, Object> envConfig =
        MetricsCollectorUtils.castValuesToAppropriateTypes(templateConfig.environment());

    environment.putAll(envConfig);

    return environment;
  }
}
