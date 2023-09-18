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
import com.google.api.services.dataflow.model.FlexTemplateRuntimeEnvironment;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.LaunchFlexTemplateParameter;
import com.google.api.services.dataflow.model.LaunchFlexTemplateRequest;
import com.google.api.services.dataflow.model.LaunchFlexTemplateResponse;
import com.google.cloud.teleport.dfmetrics.utils.MetricsCollectorUtils;
import com.google.cloud.teleport.dfmetrics.utils.RetryUtil;
import dev.failsafe.Failsafe;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class {@link FlexTemplateClient} for interacting with Dataflow Flex templates using the Dataflow
 * SDK.
 */
public final class FlexTemplateClient extends TemplateClient {
  private static final Logger LOG = LoggerFactory.getLogger(FlexTemplateClient.class);

  private final Dataflow dataflowClient;

  public FlexTemplateClient(Dataflow dataflowClient) {
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

    LaunchFlexTemplateParameter parameter =
        new LaunchFlexTemplateParameter()
            .setJobName(templateConfig.jobName())
            .setParameters(templateConfig.parameters())
            .setContainerSpecGcsPath(templateConfig.specPath())
            .setEnvironment(buildEnvironment(templateConfig));

    LaunchFlexTemplateRequest request =
        new LaunchFlexTemplateRequest().setLaunchParameter(parameter);

    LaunchFlexTemplateResponse response =
        Failsafe.with(RetryUtil.clientRetryPolicy())
            .get(
                () ->
                    dataflowClient
                        .projects()
                        .locations()
                        .flexTemplates()
                        .launch(project, region, request)
                        .execute());

    return response.getJob();
  }

  private FlexTemplateRuntimeEnvironment buildEnvironment(TemplateConfig templateConfig) {
    FlexTemplateRuntimeEnvironment environment = new FlexTemplateRuntimeEnvironment();

    Map<String, Object> envConfig =
        MetricsCollectorUtils.castValuesToAppropriateTypes(templateConfig.environment());
    environment.putAll(envConfig);

    if (System.getProperty("launcherMachineType") != null) {
      environment.setLauncherMachineType(System.getProperty("launcherMachineType"));
    }

    return environment;
  }
}
