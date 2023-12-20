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

import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.teleport.plugin.TemplateDefinitionsParser;
import com.google.cloud.teleport.plugin.model.ImageSpec;
import com.google.cloud.teleport.plugin.model.TemplateDefinitions;
import com.google.cloud.teleport.plugin.terraform.TemplateTerraformGenerator;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

  private static final String TERRAFORM = "terraform";
  private static final String TF_JSON_FILE_NAME = "dataflow_job.tf.json";

  @Override
  public void execute() throws MojoExecutionException {

    try {
      URLClassLoader loader = buildClassloader();

      List<TemplateDefinitions> templateDefinitions =
          TemplateDefinitionsParser.scanDefinitions(loader);

      for (TemplateDefinitions definition : templateDefinitions) {

        LOG.info(
            "Generating terraform from template: "
                + definition.getTemplateAnnotation().name()
                + "...");

        File module = modulePath(definition);
        File moduleDirectory = module.getParentFile();
        checkState(moduleDirectory.isDirectory());
        Files.createDirectories(Path.of(moduleDirectory.toURI()));

        ImageSpec imageSpec = definition.buildSpecModel(false);
        boolean ignored = module.createNewFile();
        LOG.info("Creating terraform module in {}...", module);
        FileOutputStream output = new FileOutputStream(module);
        TemplateTerraformGenerator.terraform(imageSpec, output);
        LOG.info("Finished creating terraform module in {}", module);
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

  @VisibleForTesting
  File modulePath(TemplateDefinitions definition) {
    if (definition.isFlex()) {
      return Path.of(targetDirectory.toURI())
          .resolve(Paths.get(TERRAFORM, TF_JSON_FILE_NAME))
          .toFile();
    }

    // definition.isClassic()
    return Path.of(targetDirectory.toURI())
        .resolve(Paths.get(TERRAFORM, definition.getTemplateAnnotation().name(), TF_JSON_FILE_NAME))
        .toFile();
  }
}
