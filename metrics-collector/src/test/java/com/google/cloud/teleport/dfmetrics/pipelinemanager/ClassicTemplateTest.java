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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.CreateJobFromTemplateRequest;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.RuntimeEnvironment;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ClassicTemplateTest {
  @Test
  public void launch_Success() throws IOException {
    // Assign
    String projectId = "testProject";
    String region = "testRegion";

    Dataflow mockDataflowClient = mock(Dataflow.class);
    Dataflow.Projects mockProjects = mock(Dataflow.Projects.class);
    Dataflow.Projects.Locations mockLocations = mock(Dataflow.Projects.Locations.class);
    Dataflow.Projects.Locations.Templates mockTemplates =
        mock(Dataflow.Projects.Locations.Templates.class);
    Dataflow.Projects.Locations.Templates.Create mockCreate =
        mock(Dataflow.Projects.Locations.Templates.Create.class);

    ClassicTemplateClient classicTemplate = new ClassicTemplateClient(mockDataflowClient);
    Map<String, String> pipelineOptions = new HashMap<String, String>();
    Map<String, Object> environment = new HashMap<String, Object>();
    RuntimeEnvironment runtimeEnvironment = new RuntimeEnvironment();

    TemplateConfig templateConfig =
        TemplateConfig.builder("gs://path/to/template.spec")
            .setjobName("testJob")
            .setsdk(TemplateConfig.Sdk.JAVA)
            .setparameters(pipelineOptions)
            .setenvironment(environment)
            .build();

    CreateJobFromTemplateRequest templateRequest =
        new CreateJobFromTemplateRequest()
            .setJobName(templateConfig.jobName())
            .setParameters(templateConfig.parameters())
            .setLocation(region)
            .setGcsPath(templateConfig.specPath())
            .setEnvironment(runtimeEnvironment);

    Job expectedJob =
        new Job()
            .setName(templateConfig.jobName())
            .setId("dummyId")
            .setCurrentState("JOB_STATE_RUNNING");

    // Act
    when(mockDataflowClient.projects()).thenReturn(mockProjects);
    when(mockProjects.locations()).thenReturn(mockLocations);
    when(mockLocations.templates()).thenReturn(mockTemplates);
    when(mockTemplates.create(eq(projectId), eq(region), eq(templateRequest)))
        .thenReturn(mockCreate);
    when(mockCreate.execute()).thenReturn(expectedJob);

    Job actual = classicTemplate.launch(projectId, region, templateConfig);

    // Assert
    assertThat(actual, equalTo(expectedJob));
  }
}
