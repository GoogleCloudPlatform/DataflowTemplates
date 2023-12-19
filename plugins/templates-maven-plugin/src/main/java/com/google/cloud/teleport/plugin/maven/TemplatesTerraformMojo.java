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

import com.google.cloud.teleport.plugin.TemplateDefinitionsParser;
import com.google.cloud.teleport.plugin.model.ImageSpec;
import com.google.cloud.teleport.plugin.model.TemplateDefinitions;
import com.google.cloud.teleport.plugin.terraform.TemplateTerraformGenerator;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URLClassLoader;
import java.util.List;
import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Goal which generates terraform modules from parsed template metadata. */
@Mojo(
    name = "terraform",
    defaultPhase = LifecyclePhase.PREPARE_PACKAGE,
    requiresDependencyResolution = ResolutionScope.COMPILE)
public class TemplatesTerraformMojo extends TemplatesBaseMojo {
  private static final Logger LOG = LoggerFactory.getLogger(TemplatesTerraformMojo.class);
  private static final String FILE_PATH = "terraform/main.tf.json";

  @Override
  public void execute() throws MojoExecutionException {

    try {
      URLClassLoader loader = buildClassloader();

      LOG.info("Generating Template Specs, saving at target: {}", targetDirectory);

      if (!targetDirectory.exists()) {
        boolean ignored = targetDirectory.mkdirs();
      }

      List<TemplateDefinitions> templateDefinitions =
          TemplateDefinitionsParser.scanDefinitions(loader);

      for (TemplateDefinitions definition : templateDefinitions) {

        LOG.info(
            "Generating terraform from template: "
                + definition.getTemplateAnnotation().name()
                + "...");

        ImageSpec imageSpec = definition.buildSpecModel(false);
        File module = new File(targetDirectory, FILE_PATH);
        boolean ignored = module.createNewFile();
        FileOutputStream output = new FileOutputStream(module);
        TemplateTerraformGenerator.terraform(imageSpec, output);
      }

    } catch (MalformedURLException e) {
      throw new MojoExecutionException("Dependency resolution failed", e);
    } catch (DependencyResolutionRequiredException e) {
      throw new MojoExecutionException("URL generation failed", e);
    } catch (FileNotFoundException e) {
      throw new MojoExecutionException("File not found", e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
