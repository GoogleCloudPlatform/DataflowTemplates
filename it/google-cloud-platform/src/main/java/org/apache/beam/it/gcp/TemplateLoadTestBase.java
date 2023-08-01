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
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.gcp.dataflow.ClassicTemplateClient;
import org.apache.beam.it.gcp.dataflow.FlexTemplateClient;

/** Base class for Template Load Tests. */
public class TemplateLoadTestBase extends LoadTestBase {

  PipelineLauncher launcher() {
    // If there is a TemplateLoadTest annotation, return appropriate dataflow template client
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
    } else if (templateAnnotations[0].flexContainerName() != null
        && !templateAnnotations[0].flexContainerName().isEmpty()) {
      return FlexTemplateClient.builder().setCredentials(CREDENTIALS).build();
    } else {
      return ClassicTemplateClient.builder().setCredentials(CREDENTIALS).build();
    }
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
