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

import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.cloud.teleport.plugin.TemplateDefinitionsParser;
import com.google.cloud.teleport.plugin.model.ImageSpec;
import com.google.cloud.teleport.plugin.model.TemplateDefinitions;
import com.google.dataflow.v1beta3.FlexTemplatesServiceClient;
import com.google.dataflow.v1beta3.Job;
import com.google.dataflow.v1beta3.LaunchFlexTemplateParameter;
import com.google.dataflow.v1beta3.LaunchFlexTemplateRequest;
import com.google.dataflow.v1beta3.LaunchFlexTemplateResponse;
import com.google.dataflow.v1beta3.LaunchTemplateParameters;
import com.google.dataflow.v1beta3.LaunchTemplateRequest;
import com.google.dataflow.v1beta3.LaunchTemplateResponse;
import com.google.dataflow.v1beta3.TemplatesServiceClient;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URLClassLoader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringTokenizer;
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

/** Goal which stages and runs a specific Template. */
@Mojo(
    name = "run",
    defaultPhase = LifecyclePhase.PACKAGE,
    requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class TemplateRunMojo extends TemplateBaseMojo {

  private static final Logger LOG = LoggerFactory.getLogger(TemplateRunMojo.class);

  @Parameter(defaultValue = "${projectId}", readonly = true, required = true)
  protected String projectId;

  @Parameter(defaultValue = "${jobName}", readonly = true, required = false)
  protected String jobName;

  @Parameter(defaultValue = "${templateName}", readonly = true, required = false)
  protected String templateName;

  @Parameter(defaultValue = "${bucketName}", readonly = true, required = true)
  protected String bucketName;

  @Parameter(defaultValue = "${stagePrefix}", readonly = true, required = false)
  protected String stagePrefix;

  @Parameter(defaultValue = "${region}", readonly = true, required = false)
  protected String region;

  @Parameter(defaultValue = "${artifactRegion}", readonly = true, required = false)
  protected String artifactRegion;

  @Parameter(
      name = "baseContainerImage",
      defaultValue = "gcr.io/dataflow-templates-base/java11-template-launcher-base:latest",
      required = false)
  protected String baseContainerImage;

  @Parameter(defaultValue = "${parameters}", readonly = true, required = true)
  protected String parameters;

  public void execute() throws MojoExecutionException {

    try {
      URLClassLoader loader = buildClassloader();

      BuildPluginManager pluginManager =
          (BuildPluginManager) session.lookup("org.apache.maven.plugin.BuildPluginManager");

      LOG.info("Staging Templates...");

      List<TemplateDefinitions> templateDefinitions =
          TemplateDefinitionsParser.scanDefinitions(loader);

      Optional<TemplateDefinitions> definitionsOptional =
          templateDefinitions.stream()
              .filter(candidate -> candidate.getTemplateAnnotation().name().equals(templateName))
              .findFirst();

      if (definitionsOptional.isEmpty()) {
        LOG.warn("Not found template with the name " + templateName + " to run in this module.");
        return;
      }

      TemplateDefinitions definition = definitionsOptional.get();
      ImageSpec imageSpec = definition.buildSpecModel(false);
      String currentTemplateName = imageSpec.getMetadata().getName();

      if (stagePrefix == null || stagePrefix.isEmpty()) {
        stagePrefix = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date()) + "_RC01";
      }

      LOG.info("Staging template {}...", currentTemplateName);

      String useRegion = StringUtils.isNotEmpty(region) ? region : "us-central1";

      // TODO: is there a better way to get the plugin on the _same project_?
      TemplateStageMojo configuredMojo =
          new TemplateStageMojo(
              project,
              session,
              outputDirectory,
              outputClassesDirectory,
              resourcesDirectory,
              targetDirectory,
              projectId,
              templateName,
              bucketName,
              stagePrefix,
              region,
              artifactRegion,
              baseContainerImage);

      String useJobName =
          StringUtils.isNotEmpty(jobName)
              ? jobName
              : templateName.toLowerCase().replace('_', '-')
                  + "-"
                  + new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date());
      Job job;

      if (definition.isClassic()) {
        job =
            runClassicTemplate(
                pluginManager, definition, imageSpec, configuredMojo, useJobName, useRegion);
      } else {
        job =
            runFlexTemplate(
                pluginManager, definition, imageSpec, configuredMojo, useJobName, useRegion);
      }

      LOG.info(
          "Created template job with ID {}. Console:"
              + " https://console.cloud.google.com/dataflow/jobs/{}/{}?project={}",
          job.getId(),
          job.getLocation(),
          job.getId(),
          job.getProjectId());

      LOG.info(
          "To cancel the project, run: gcloud dataflow jobs cancel {} --project {} --region {}",
          job.getId(),
          job.getProjectId(),
          job.getLocation());

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

  private Job runClassicTemplate(
      BuildPluginManager pluginManager,
      TemplateDefinitions definition,
      ImageSpec imageSpec,
      TemplateStageMojo configuredMojo,
      String jobName,
      String useRegion)
      throws MojoExecutionException, IOException, InterruptedException {
    Job job;
    String templatePath = configuredMojo.stageClassicTemplate(definition, imageSpec, pluginManager);
    String stagingPath = "gs://" + bucketName + "/" + stagePrefix + "/staging/";

    String[] runCmd =
        new String[] {
          "gcloud",
          "dataflow",
          "jobs",
          "run",
          jobName,
          "--gcs-location",
          templatePath,
          "--project",
          projectId,
          "--region",
          useRegion,
          "--staging-location",
          stagingPath,
          "--parameters",
          parameters
        };

    LOG.info(
        "Template {} staged! Run Classic Template command: {}",
        templateName,
        String.join(" ", runCmd));

    try (TemplatesServiceClient client = TemplatesServiceClient.create()) {
      LaunchTemplateParameters launchParameters =
          LaunchTemplateParameters.newBuilder()
              .setJobName(jobName)
              .putAllParameters(parseParameters(parameters))
              .build();
      LaunchTemplateResponse launchTemplateResponse =
          client.launchTemplate(
              LaunchTemplateRequest.newBuilder()
                  .setLaunchParameters(launchParameters)
                  .setGcsPath(templatePath)
                  .setProjectId(projectId)
                  .setLocation(useRegion)
                  .build());

      job = launchTemplateResponse.getJob();
    }
    return job;
  }

  private Job runFlexTemplate(
      BuildPluginManager pluginManager,
      TemplateDefinitions definition,
      ImageSpec imageSpec,
      TemplateStageMojo configuredMojo,
      String jobName,
      String useRegion)
      throws MojoExecutionException, IOException, InterruptedException {
    Job job;
    String templatePath = configuredMojo.stageFlexTemplate(definition, imageSpec, pluginManager);

    String[] runCmd =
        new String[] {
          "gcloud",
          "dataflow",
          "flex-template",
          "run",
          jobName,
          "--template-file-gcs-location",
          templatePath,
          "--project",
          projectId,
          "--region",
          useRegion,
          "--parameters",
          parameters
        };

    LOG.info(
        "Template {} staged! Run Flex Template command: {}",
        templateName,
        String.join(" ", runCmd));

    try (FlexTemplatesServiceClient client = FlexTemplatesServiceClient.create()) {
      LaunchFlexTemplateParameter launchParameters =
          LaunchFlexTemplateParameter.newBuilder()
              .setJobName(jobName)
              .setContainerSpecGcsPath(templatePath)
              .putAllParameters(parseParameters(parameters))
              .build();
      LaunchFlexTemplateResponse launchFlexTemplateResponse =
          client.launchFlexTemplate(
              LaunchFlexTemplateRequest.newBuilder()
                  .setLaunchParameter(launchParameters)
                  .setProjectId(projectId)
                  .setLocation(useRegion)
                  .build());

      job = launchFlexTemplateResponse.getJob();
    }
    return job;
  }

  private Map<String, String> parseParameters(String parameters) {
    Map<String, String> parametersMap = new LinkedHashMap<>();

    StringTokenizer tokenizer = new StringTokenizer(parameters, ",");
    while (tokenizer.hasMoreElements()) {
      String[] token = tokenizer.nextToken().split("=");
      parametersMap.put(token[0], token[1]);
    }
    return parametersMap;
  }
}
