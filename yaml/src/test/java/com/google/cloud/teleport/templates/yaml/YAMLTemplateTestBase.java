/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.templates.yaml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.gcp.JDBCBaseIT;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public abstract class YAMLTemplateTestBase extends TemplateTestBase {

  protected String yamlPipeline;
  protected String yamlPipelineGcsPath;

  @Before
  @Override
  public void setUpBase() {
    try {
      setUp(
          new TemplateIntegrationTest() {

            @Override
            public Class<? extends Annotation> annotationType() {
              return null;
            }

            @Override
            public Class<?> value() {
              return YAMLTemplate.class;
            }

            @Override
            public String template() {
              return YAMLTemplate.class.getAnnotation(Template.class).name();
            }
          });

      JDBCBaseIT.uploadArtifacts(gcsClient);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    yamlPipeline = getClass().getSimpleName() + ".yaml";
    yamlPipelineGcsPath = "input/" + yamlPipeline;
  }

  public PipelineLauncher.LaunchInfo launchYamlTemplate(Map<String, String> parameters)
      throws IOException {
    gcsClient.createArtifact(yamlPipelineGcsPath, loadYamlPipelineFile());

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("yaml_pipeline_file", getGcsPath(yamlPipelineGcsPath))
            .addParameter("jinja_variables", new ObjectMapper().writeValueAsString(parameters));

    return launchTemplate(options);
  }

  private String loadYamlPipelineFile() throws IOException {
    return Files.readString(Paths.get(Resources.getResource(yamlPipeline).getPath()));
  }
}
