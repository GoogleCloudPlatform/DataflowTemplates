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

import static com.google.cloud.teleport.metadata.util.MetadataUtils.bucketNameOnly;
import static com.google.cloud.teleport.plugin.DockerfileGenerator.BASE_CONTAINER_IMAGE;
import static com.google.cloud.teleport.plugin.DockerfileGenerator.BASE_PYTHON_CONTAINER_IMAGE;
import static com.google.cloud.teleport.plugin.DockerfileGenerator.JAVA_LAUNCHER_ENTRYPOINT;
import static com.google.cloud.teleport.plugin.DockerfileGenerator.PYTHON_LAUNCHER_ENTRYPOINT;
import static com.google.cloud.teleport.plugin.DockerfileGenerator.PYTHON_VERSION;

import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.cloud.teleport.plugin.TemplateDefinitionsParser;
import com.google.cloud.teleport.plugin.TemplateSpecsGenerator;
import com.google.cloud.teleport.plugin.model.ImageSpec;
import com.google.cloud.teleport.plugin.model.TemplateDefinitions;
import java.net.MalformedURLException;
import java.net.URLClassLoader;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.plugin.BuildPluginManager;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Goal which stages and releases a specific Template. */
@Mojo(
    name = "release",
    defaultPhase = LifecyclePhase.PACKAGE,
    requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class TemplatesReleaseMojo extends TemplatesBaseMojo {

  private static final Logger LOG = LoggerFactory.getLogger(TemplatesReleaseMojo.class);

  @Parameter(defaultValue = "${projectId}", readonly = true, required = true)
  protected String projectId;

  @Parameter(defaultValue = "${templateName}", readonly = true, required = false)
  protected String templateName;

  @Parameter(defaultValue = "${bucketName}", readonly = true, required = true)
  protected String bucketName;

  @Parameter(defaultValue = "${librariesBucketName}", readonly = true, required = false)
  protected String librariesBucketName;

  @Parameter(defaultValue = "${stagePrefix}", readonly = true, required = false)
  protected String stagePrefix;

  @Parameter(defaultValue = "${region}", readonly = true, required = false)
  protected String region;

  @Parameter(defaultValue = "${artifactRegion}", readonly = true, required = false)
  protected String artifactRegion;

  @Parameter(defaultValue = "${gcpTempLocation}", readonly = true, required = false)
  protected String gcpTempLocation;

  @Parameter(
      defaultValue = BASE_CONTAINER_IMAGE,
      property = "baseContainerImage",
      readonly = true,
      required = false)
  protected String baseContainerImage;

  @Parameter(
      defaultValue = BASE_PYTHON_CONTAINER_IMAGE,
      property = "basePythonContainerImage",
      readonly = true,
      required = false)
  protected String basePythonContainerImage;

  @Parameter(
      defaultValue = PYTHON_LAUNCHER_ENTRYPOINT,
      property = "pythonTemplateLauncherEntryPoint",
      readonly = true,
      required = false)
  protected String pythonTemplateLauncherEntryPoint;

  @Parameter(
      defaultValue = JAVA_LAUNCHER_ENTRYPOINT,
      property = "javaTemplateLauncherEntryPoint",
      readonly = true,
      required = false)
  protected String javaTemplateLauncherEntryPoint;

  @Parameter(
      defaultValue = PYTHON_VERSION,
      property = "pythonVersion",
      readonly = true,
      required = false)
  protected String pythonVersion;

  @Parameter(defaultValue = "${beamVersion}", readonly = true, required = false)
  protected String beamVersion;

  @Parameter(defaultValue = "${unifiedWorker}", readonly = true, required = false)
  protected boolean unifiedWorker;

  public void execute() throws MojoExecutionException {

    if (librariesBucketName == null || librariesBucketName.isEmpty()) {
      librariesBucketName = bucketName;
    }

    try {
      URLClassLoader loader = buildClassloader();
      TemplateSpecsGenerator generator = new TemplateSpecsGenerator();

      BuildPluginManager pluginManager =
          (BuildPluginManager) session.lookup("org.apache.maven.plugin.BuildPluginManager");

      LOG.info("Releasing Templates to bucket '{}'...", bucketNameOnly(bucketName));

      List<TemplateDefinitions> templateDefinitions =
          TemplateDefinitionsParser.scanDefinitions(loader);

      if (templateName != null && !templateName.isEmpty()) {
        templateDefinitions =
            templateDefinitions.stream()
                .filter(candidate -> candidate.getTemplateAnnotation().name().equals(templateName))
                .collect(Collectors.toList());
      }

      if (templateDefinitions.isEmpty()) {
        LOG.warn("Not found templates to release in this module.");
        return;
      }

      for (TemplateDefinitions definition : templateDefinitions) {

        ImageSpec imageSpec = definition.buildSpecModel(true);
        String currentTemplateName = imageSpec.getMetadata().getName();

        if (stagePrefix == null || stagePrefix.isEmpty()) {
          throw new IllegalArgumentException("Stage Prefix must be informed for releases");
        }

        LOG.info("Staging template {}...", currentTemplateName);

        String useRegion = StringUtils.isNotEmpty(region) ? region : "us-central1";

        // TODO: is there a better way to get the plugin on the _same project_?
        TemplatesStageMojo configuredMojo =
            new TemplatesStageMojo(
                project,
                session,
                outputDirectory,
                outputClassesDirectory,
                resourcesDirectory,
                targetDirectory,
                projectId,
                templateName,
                bucketName,
                librariesBucketName,
                stagePrefix,
                useRegion,
                artifactRegion,
                gcpTempLocation,
                baseContainerImage,
                basePythonContainerImage,
                pythonTemplateLauncherEntryPoint,
                javaTemplateLauncherEntryPoint,
                pythonVersion,
                beamVersion,
                unifiedWorker);

        String templatePath = configuredMojo.stageTemplate(definition, imageSpec, pluginManager);

        if (!definition.getTemplateAnnotation().stageImageOnly()) {
          LOG.info("Template staged: {}", templatePath);

          // Export the specs for collection
          generator.saveMetadata(definition, imageSpec.getMetadata(), targetDirectory);
          if (definition.isFlex()) {
            generator.saveImageSpec(definition, imageSpec, targetDirectory);
          }
        }
      }

    } catch (DependencyResolutionRequiredException e) {
      throw new MojoExecutionException("Dependency resolution failed", e);
    } catch (MalformedURLException e) {
      throw new MojoExecutionException("URL generation failed", e);
    } catch (InvalidArgumentException e) {
      throw new MojoExecutionException("Invalid run argument", e);
    } catch (Exception e) {
      throw new MojoExecutionException("Template run failed", e);
    }
  }
}
