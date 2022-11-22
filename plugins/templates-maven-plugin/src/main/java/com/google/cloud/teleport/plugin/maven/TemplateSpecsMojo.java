/*
 * Copyright (C) 2022 Google LLC
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

import com.google.cloud.teleport.plugin.TemplateSpecsGenerator;
import java.net.MalformedURLException;
import java.net.URLClassLoader;
import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Goal which generates specs required by Templates. It reads all the annotations for the Template
 * files in the current context, parses and generates the metadata files in the target folders.
 */
@Mojo(
    name = "spec",
    defaultPhase = LifecyclePhase.PREPARE_PACKAGE,
    requiresDependencyResolution = ResolutionScope.COMPILE)
public class TemplateSpecsMojo extends TemplateBaseMojo {

  private static final Logger LOG = LoggerFactory.getLogger(TemplateSpecsMojo.class);

  public void execute() throws MojoExecutionException {

    try {
      URLClassLoader loader = buildClassloader();

      LOG.info("Generating Template Specs, saving at target: {}", targetDirectory);

      if (!targetDirectory.exists()) {
        targetDirectory.mkdirs();
      }

      TemplateSpecsGenerator generator = new TemplateSpecsGenerator();
      generator.generateSpecs(loader, targetDirectory);

    } catch (DependencyResolutionRequiredException e) {
      throw new MojoExecutionException("Dependency resolution failed", e);
    } catch (MalformedURLException e) {
      throw new MojoExecutionException("URL generation failed", e);
    }
  }
}
