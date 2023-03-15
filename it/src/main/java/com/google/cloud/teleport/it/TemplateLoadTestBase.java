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
package com.google.cloud.teleport.it;

import com.google.cloud.teleport.it.dataflow.ClassicTemplateClient;
import com.google.cloud.teleport.it.dataflow.FlexTemplateClient;
import com.google.cloud.teleport.it.launcher.PipelineLauncher;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateLoadTest;

/** Base class for Templates Load Tests. */
public class TemplateLoadTestBase extends LoadTestBase {

  PipelineLauncher launcher() {
    // If there is a TemplateLoadTest annotation, return appropriate dataflow template client
    // Else, return null.
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
}
