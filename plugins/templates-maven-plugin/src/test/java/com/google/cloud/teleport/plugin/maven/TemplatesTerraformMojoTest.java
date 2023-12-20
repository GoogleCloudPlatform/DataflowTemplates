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
package com.google.cloud.teleport.plugin.maven;

import static com.google.common.base.Preconditions.checkArgument;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.plugin.model.TemplateDefinitions;
import java.io.File;
import java.nio.file.Paths;
import org.junit.Test;

/** Tests for {@link TemplatesTerraformMojo}. */
public class TemplatesTerraformMojoTest {

  @Test
  public void givenFlexTemplate_thenModulePathInSubdirectory() {
    TemplatesTerraformMojo mojo = new TemplatesTerraformMojo();
    mojo.targetDirectory = Paths.get("v2", SomeFlexTemplate.class.getSimpleName()).toFile();
    TemplateDefinitions definition = definitions(SomeFlexTemplate.class);
    assertThat(definition.isFlex(), equalTo(true));
    File modulePath = mojo.modulePath(definition);
    assertThat(
        modulePath,
        equalTo(
            Paths.get("v2/SomeFlexTemplate/terraform/dataflow_job.tf.json")
                .toAbsolutePath()
                .toFile()));
  }

  @Test
  public void givenClassicTemplate_thenModulePathInSubdirectory() {
    TemplatesTerraformMojo mojo = new TemplatesTerraformMojo();
    mojo.targetDirectory = Paths.get("v1").toFile();
    TemplateDefinitions definition = definitions(SomeClassicTemplate.class);
    assertThat(definition.isFlex(), equalTo(false));
    File modulePath = mojo.modulePath(definition);
    assertThat(
        modulePath,
        equalTo(
            Paths.get("v1/terraform/SomeClassicTemplate/dataflow_job.tf.json")
                .toAbsolutePath()
                .toFile()));
  }

  private static TemplateDefinitions definitions(Class<?> templateClass) {
    checkArgument(templateClass.isAnnotationPresent(Template.class));
    Template annotation = templateClass.getAnnotation(Template.class);
    return new TemplateDefinitions(templateClass, annotation);
  }

  @Template(
      name = "SomeFlexTemplate",
      displayName = "Some Flex Template",
      description = "some description",
      category = TemplateCategory.BATCH,
      flexContainerName = "some container")
  private static class SomeFlexTemplate {}

  @Template(
      name = "SomeClassicTemplate",
      displayName = "Some Classic Template",
      description = "some description",
      category = TemplateCategory.LEGACY)
  private static class SomeClassicTemplate {}
}
