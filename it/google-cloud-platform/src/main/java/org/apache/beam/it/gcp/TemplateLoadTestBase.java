/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.gcp;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.dataflow.ClassicTemplateClient;
import org.apache.beam.it.gcp.dataflow.FlexTemplateClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class for Template Load Tests. */
public class TemplateLoadTestBase extends LoadTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TemplateLoadTestBase.class);

  public PipelineLauncher launcher() {
    // Return the appropriate dataflow template client for the template under test
    String flexContainerName = getTemplateAnnotation().flexContainerName();
    if (flexContainerName != null && !flexContainerName.isEmpty()) {
      return FlexTemplateClient.builder(CREDENTIALS).build();
    }
    return ClassicTemplateClient.builder(CREDENTIALS).build();
  }

  /**
   * Returns the template spec path to launch for the template under test (specified via {@link
   * TemplateLoadTest}).
   *
   * <p>If {@code -DspecPath} is provided, it is used as is (e.g. to run against a released
   * template). Otherwise, the template is built and staged from the checked out source code (i.e.
   * mainline), the same way integration tests do. Staging is cached per JVM, so this can be called
   * multiple times cheaply.
   */
  protected String getTemplateSpecPath() {
    String specPath = TestProperties.specPath();
    if (specPath != null && !specPath.isEmpty()) {
      LOG.info("A spec path was given, not staging template: {}", specPath);
      return specPath;
    }
    try {
      return TemplateTestBase.stageTemplate(getTemplateAnnotation(), "pom.xml", CREDENTIALS);
    } catch (ExecutionException e) {
      throw new RuntimeException("Error staging template for " + getClass().getSimpleName(), e);
    }
  }

  private Template getTemplateAnnotation() {
    TemplateLoadTest annotation = getClass().getAnnotation(TemplateLoadTest.class);
    if (annotation == null) {
      throw new RuntimeException(
          String.format(
              "%s did not specify which template is tested using @TemplateLoadTest.", getClass()));
    }
    Class<?> templateClass = annotation.value();
    Template[] templateAnnotations = templateClass.getAnnotationsByType(Template.class);
    if (templateAnnotations.length == 0) {
      throw new RuntimeException(
          String.format(
              "Template mentioned in @TemplateLoadTest for %s does not contain a @Template"
                  + " annotation.",
              getClass()));
    }
    if (templateAnnotations.length == 1 || annotation.template().isEmpty()) {
      return templateAnnotations[0];
    }
    for (Template template : templateAnnotations) {
      if (template.name().equals(annotation.template())) {
        return template;
      }
    }
    throw new RuntimeException(
        String.format(
            "template '%s' in @TemplateLoadTest for %s does not match any @Template annotation.",
            annotation.template(), getClass()));
  }

  protected LaunchConfig.Builder enableRunnerV2(LaunchConfig.Builder config) {
    return config.addEnvironment(
        "additionalExperiments", Collections.singletonList("use_runner_v2"));
  }

  protected LaunchConfig.Builder disableRunnerV2(LaunchConfig.Builder config) {
    return config.addEnvironment(
        "additionalExperiments", Collections.singletonList("disable_runner_v2"));
  }

  protected LaunchConfig.Builder enableStreamingEngine(LaunchConfig.Builder config) {
    return config.addEnvironment("enableStreamingEngine", true);
  }
}
