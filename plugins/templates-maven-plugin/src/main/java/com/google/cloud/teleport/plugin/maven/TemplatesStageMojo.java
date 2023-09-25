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
import static org.twdata.maven.mojoexecutor.MojoExecutor.configuration;
import static org.twdata.maven.mojoexecutor.MojoExecutor.element;
import static org.twdata.maven.mojoexecutor.MojoExecutor.executeMojo;
import static org.twdata.maven.mojoexecutor.MojoExecutor.executionEnvironment;
import static org.twdata.maven.mojoexecutor.MojoExecutor.goal;
import static org.twdata.maven.mojoexecutor.MojoExecutor.plugin;

import com.google.cloud.teleport.metadata.Template.TemplateType;
import com.google.cloud.teleport.plugin.PythonDockerfileGenerator;
import com.google.cloud.teleport.plugin.TemplateDefinitionsParser;
import com.google.cloud.teleport.plugin.TemplatePluginUtils;
import com.google.cloud.teleport.plugin.TemplateSpecsGenerator;
import com.google.cloud.teleport.plugin.model.ImageSpec;
import com.google.cloud.teleport.plugin.model.TemplateDefinitions;
import freemarker.template.TemplateException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.BuildPluginManager;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twdata.maven.mojoexecutor.MojoExecutor.Element;

/**
 * Goal which stages the Templates into Cloud Storage / Artifact Registry.
 *
 * <p>The process is different for Classic Templates and Flex Templates, please check {@link
 * #stageClassicTemplate(TemplateDefinitions, ImageSpec, BuildPluginManager)} and {@link
 * #stageFlexTemplate(TemplateDefinitions, ImageSpec, BuildPluginManager)}, respectively.
 */
@Mojo(
    name = "stage",
    defaultPhase = LifecyclePhase.PACKAGE,
    requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class TemplatesStageMojo extends TemplatesBaseMojo {

  private static final Logger LOG = LoggerFactory.getLogger(TemplatesStageMojo.class);

  @Parameter(defaultValue = "${projectId}", readonly = true, required = true)
  protected String projectId;

  @Parameter(defaultValue = "${templateName}", readonly = true, required = false)
  protected String templateName;

  @Parameter(defaultValue = "${bucketName}", readonly = true, required = true)
  protected String bucketName;

  @Parameter(defaultValue = "${librariesBucketName}", readonly = true, required = false)
  protected String librariesBucketName;

  @Parameter(defaultValue = "${stagePrefix}", readonly = true, required = true)
  protected String stagePrefix;

  @Parameter(defaultValue = "${region}", readonly = true, required = false)
  protected String region;

  @Parameter(defaultValue = "${artifactRegion}", readonly = true, required = false)
  protected String artifactRegion;

  @Parameter(defaultValue = "${artifactRegistry}", readonly = true, required = false)
  protected String artifactRegistry;

  @Parameter(defaultValue = "${gcpTempLocation}", readonly = true, required = false)
  protected String gcpTempLocation;

  @Parameter(
      name = "baseContainerImage",
      defaultValue =
          "gcr.io/dataflow-templates-base/java11-template-launcher-base-distroless:latest",
      required = false)
  protected String baseContainerImage;

  @Parameter(
      name = "basePythonContainerImage",
      defaultValue = "gcr.io/dataflow-templates-base/python311-template-launcher-base:latest",
      required = false)
  protected String basePythonContainerImage;

  public TemplatesStageMojo() {}

  public TemplatesStageMojo(
      MavenProject project,
      MavenSession session,
      File outputDirectory,
      File outputClassesDirectory,
      File resourcesDirectory,
      File targetDirectory,
      String projectId,
      String templateName,
      String bucketName,
      String librariesBucketName,
      String stagePrefix,
      String region,
      String artifactRegion,
      String gcpTempLocation,
      String baseContainerImage,
      String basePythonContainerImage) {
    this.project = project;
    this.session = session;
    this.outputDirectory = outputDirectory;
    this.outputClassesDirectory = outputClassesDirectory;
    this.resourcesDirectory = resourcesDirectory;
    this.targetDirectory = targetDirectory;
    this.projectId = projectId;
    this.templateName = templateName;
    this.bucketName = bucketName;
    this.librariesBucketName = librariesBucketName;
    this.stagePrefix = stagePrefix;
    this.region = region;
    this.artifactRegion = artifactRegion;
    this.gcpTempLocation = gcpTempLocation;
    this.baseContainerImage = baseContainerImage;
    this.basePythonContainerImage = basePythonContainerImage;
  }

  public void execute() throws MojoExecutionException {

    if (librariesBucketName == null || librariesBucketName.isEmpty()) {
      librariesBucketName = bucketName;
    }
    // Remove trailing slash if given
    if (stagePrefix.endsWith("/")) {
      stagePrefix = stagePrefix.substring(0, stagePrefix.length() - 1);
    }

    try {
      URLClassLoader loader = buildClassloader();

      BuildPluginManager pluginManager =
          (BuildPluginManager) session.lookup("org.apache.maven.plugin.BuildPluginManager");

      LOG.info("Staging Templates to bucket '{}'...", bucketNameOnly(bucketName));

      List<TemplateDefinitions> templateDefinitions =
          TemplateDefinitionsParser.scanDefinitions(loader);
      for (TemplateDefinitions definition : templateDefinitions) {

        ImageSpec imageSpec = definition.buildSpecModel(false);
        String currentTemplateName = definition.getTemplateAnnotation().name();
        String currentDisplayName = definition.getTemplateAnnotation().displayName();

        // Filter out the template if there was a specific one given
        if (templateName != null
            && !templateName.isEmpty()
            && !templateName.equals(currentTemplateName)
            && !templateName.equals(currentDisplayName)) {
          LOG.info("Skipping template {} ({})", currentTemplateName, currentDisplayName);
          continue;
        }

        LOG.info("Staging template {}...", currentTemplateName);

        if (definition.isFlex()) {
          stageFlexTemplate(definition, imageSpec, pluginManager);
        } else {
          stageClassicTemplate(definition, imageSpec, pluginManager);
        }
      }

    } catch (DependencyResolutionRequiredException e) {
      throw new MojoExecutionException("Dependency resolution failed", e);
    } catch (MalformedURLException e) {
      throw new MojoExecutionException("URL generation failed", e);
    } catch (Exception e) {
      throw new MojoExecutionException("Template staging failed", e);
    }
  }

  /**
   * Stages a template based on its specific type. See {@link
   * #stageClassicTemplate(TemplateDefinitions, ImageSpec, BuildPluginManager)} and {@link
   * #stageFlexTemplate(TemplateDefinitions, ImageSpec, BuildPluginManager)} for more details.
   */
  public String stageTemplate(
      TemplateDefinitions definition, ImageSpec imageSpec, BuildPluginManager pluginManager)
      throws MojoExecutionException, IOException, InterruptedException, TemplateException {
    if (definition.isClassic()) {
      return stageClassicTemplate(definition, imageSpec, pluginManager);
    } else {
      return stageFlexTemplate(definition, imageSpec, pluginManager);
    }
  }

  /**
   * Stage a classic template. The way that it works, in high level, is:
   *
   * <p>Step 1: Use the Specs generator to create the metadata files.
   *
   * <p>Step 2: Use the exec:java plugin to execute the Template class, and passing parameters such
   * that the DataflowRunner knows that it is a Template staging (--stagingLocation,
   * --templateLocation). The class itself is responsible for copying all the dependencies to Cloud
   * Storage and saving the Template file.
   *
   * <p>Step 3: Use `gcloud storage` to copy the metadata / parameters file into Cloud Storage as
   * well, using `{templatePath}_metadata`, so that parameters can be shown in the UI when using
   * this specific Template.
   */
  protected String stageClassicTemplate(
      TemplateDefinitions definition, ImageSpec imageSpec, BuildPluginManager pluginManager)
      throws MojoExecutionException, IOException, InterruptedException {
    File metadataFile =
        new TemplateSpecsGenerator()
            .saveMetadata(definition, imageSpec.getMetadata(), outputClassesDirectory);
    String currentTemplateName = definition.getTemplateAnnotation().name();

    String stagingPath =
        "gs://" + bucketNameOnly(librariesBucketName) + "/" + stagePrefix + "/staging/";
    String templatePath =
        "gs://" + bucketNameOnly(bucketName) + "/" + stagePrefix + "/" + currentTemplateName;
    String templateMetadataPath = templatePath + "_metadata";

    List<Element> arguments = new ArrayList<>();
    arguments.add(element("argument", "--runner=DataflowRunner"));
    arguments.add(element("argument", "--stagingLocation=" + stagingPath));
    arguments.add(element("argument", "--templateLocation=" + templatePath));
    arguments.add(element("argument", "--project=" + projectId));
    arguments.add(element("argument", "--region=" + region));

    if (gcpTempLocation != null) {
      String gcpTempLocationPath = "gs://" + bucketNameOnly(gcpTempLocation) + "/temp/";
      arguments.add(element("argument", "--gcpTempLocation=" + gcpTempLocationPath));
      arguments.add(element("argument", "--tempLocation=" + gcpTempLocationPath));
    }

    arguments.add(
        element(
            "argument",
            "--labels={\"goog-dataflow-provided-template-name\":\""
                + currentTemplateName.toLowerCase()
                + "\", \"goog-dataflow-provided-template-version\":\""
                + TemplateDefinitionsParser.parseVersion(stagePrefix)
                + "\", \"goog-dataflow-provided-template-type\":\"legacy\"}"));

    for (Map.Entry<String, String> runtimeParameter :
        imageSpec.getMetadata().getRuntimeParameters().entrySet()) {
      arguments.add(
          element(
              "argument", "--" + runtimeParameter.getKey() + "=" + runtimeParameter.getValue()));
    }

    LOG.info(
        "Running "
            + imageSpec.getMetadata().getMainClass()
            + " with parameters: "
            + arguments.stream()
                .map(element -> element.toDom().getValue())
                .collect(Collectors.toList()));

    executeMojo(
        plugin("org.codehaus.mojo", "exec-maven-plugin"),
        goal("java"),
        configuration(
            // Base image to use
            element("mainClass", imageSpec.getMetadata().getMainClass()),
            element("cleanupDaemonThreads", "false"),
            element("arguments", arguments.toArray(new Element[0]))),
        executionEnvironment(project, session, pluginManager));

    String[] copyCmd =
        new String[] {
          "gcloud",
          "storage",
          "cp",
          metadataFile.getAbsolutePath(),
          templateMetadataPath,
          "--project",
          projectId
        };
    LOG.info("Running: {}", String.join(" ", copyCmd));

    Process process = Runtime.getRuntime().exec(copyCmd);

    if (process.waitFor() != 0) {
      throw new RuntimeException(
          "Error copying template using 'gcloud storage'. Please make sure it is up to date. "
              + new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8)
              + "\n"
              + new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8));
    }

    LOG.info("Classic Template was staged! {}", templatePath);
    return templatePath;
  }

  /**
   * Stage a Flex template. The way that it works, in high level, is:
   *
   * <p>Step 1: Use the Specs generator to create the metadata and command spec files.
   *
   * <p>Step 2: Use JIB to create a Docker image containing the Template code, and upload to
   * Artifact Registry / GCR.
   *
   * <p>Step 3: Use `gcloud dataflow build` to create the Template based on the image created by
   * JIB, and uploads the metadata file to the proper directory.
   */
  protected String stageFlexTemplate(
      TemplateDefinitions definition, ImageSpec imageSpec, BuildPluginManager pluginManager)
      throws MojoExecutionException, IOException, InterruptedException, TemplateException {

    String currentTemplateName = definition.getTemplateAnnotation().name();
    TemplateSpecsGenerator generator = new TemplateSpecsGenerator();

    String prefix = "";
    if (artifactRegion != null && !artifactRegion.isEmpty()) {
      prefix = artifactRegion + ".";
    }

    String containerName = definition.getTemplateAnnotation().flexContainerName();

    // GCR paths can not contain ":", if the project id has it, it should be converted to "/".
    String projectIdUrl = projectId.replace(':', '/');
    String imagePath =
        Optional.ofNullable(artifactRegistry)
            .map(
                value ->
                    value
                        + "/"
                        + projectIdUrl
                        + "/"
                        + stagePrefix.toLowerCase()
                        + "/"
                        + containerName)
            .orElse(
                prefix
                    + "gcr.io/"
                    + projectIdUrl
                    + "/"
                    + stagePrefix.toLowerCase()
                    + "/"
                    + containerName);
    LOG.info("Stage image to GCR: {}", imagePath);

    File metadataFile =
        generator.saveMetadata(definition, imageSpec.getMetadata(), outputClassesDirectory);
    File commandSpecFile = generator.saveCommandSpec(definition, outputClassesDirectory);

    String appRoot = "/template/" + containerName;
    String commandSpec = appRoot + "/resources/" + commandSpecFile.getName();

    String templatePath =
        "gs://" + bucketNameOnly(bucketName) + "/" + stagePrefix + "/flex/" + currentTemplateName;

    if (definition.getTemplateAnnotation().type() == TemplateType.JAVA) {
      stageFlexJavaTemplate(
          definition,
          pluginManager,
          currentTemplateName,
          imagePath,
          metadataFile,
          appRoot,
          commandSpec,
          templatePath);
    } else if (definition.getTemplateAnnotation().type() == TemplateType.PYTHON) {
      stageFlexPythonTemplate(
          definition, currentTemplateName, imagePath, metadataFile, containerName, templatePath);
    } else {
      throw new IllegalArgumentException(
          "Type not known: " + definition.getTemplateAnnotation().type());
    }

    LOG.info("Flex Template was staged! {}", templatePath);
    return templatePath;
  }

  private void stageFlexJavaTemplate(
      TemplateDefinitions definition,
      BuildPluginManager pluginManager,
      String currentTemplateName,
      String imagePath,
      File metadataFile,
      String appRoot,
      String commandSpec,
      String templatePath)
      throws MojoExecutionException, IOException, InterruptedException {
    executeMojo(
        plugin("com.google.cloud.tools", "jib-maven-plugin"),
        goal("build"),
        configuration(
            // Base image to use
            element("from", element("image", baseContainerImage)),
            // Target image to stage
            element("to", element("image", imagePath)),
            element(
                "container",
                element("appRoot", appRoot),
                // Keep the original entrypoint
                element("entrypoint", "INHERIT"),
                // Point to the command spec
                element("environment", element("DATAFLOW_JAVA_COMMAND_SPEC", commandSpec)))),
        executionEnvironment(project, session, pluginManager));

    String[] flexTemplateBuildCmd =
        new String[] {
          "gcloud",
          "dataflow",
          "flex-template",
          "build",
          templatePath,
          "--image",
          imagePath,
          "--project",
          projectId,
          "--sdk-language",
          definition.getTemplateAnnotation().type().name(),
          "--metadata-file",
          outputClassesDirectory.getAbsolutePath() + "/" + metadataFile.getName(),
          "--additional-user-labels",
          "goog-dataflow-provided-template-name="
              + currentTemplateName.toLowerCase()
              + ",goog-dataflow-provided-template-version="
              + TemplateDefinitionsParser.parseVersion(stagePrefix)
              + ",goog-dataflow-provided-template-type=flex"
        };
    LOG.info("Running: {}", String.join(" ", flexTemplateBuildCmd));

    Process process = Runtime.getRuntime().exec(flexTemplateBuildCmd);

    if (process.waitFor() != 0) {
      throw new RuntimeException(
          "Error building template using gcloud. Please make sure it is up to date. "
              + new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8)
              + "\n"
              + new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  private void stageFlexPythonTemplate(
      TemplateDefinitions definition,
      String currentTemplateName,
      String imagePath,
      File metadataFile,
      String containerName,
      String templatePath)
      throws IOException, InterruptedException, TemplateException {

    String dockerfilePath = outputClassesDirectory.getPath() + "/" + containerName + "/Dockerfile";
    File dockerfile = new File(dockerfilePath);
    if (!dockerfile.exists()) {
      PythonDockerfileGenerator.generateDockerfile(
          basePythonContainerImage, containerName, outputClassesDirectory);
    }
    stageUsingDockerfile(imagePath, containerName);

    String[] flexTemplateBuildCmd =
        new String[] {
          "gcloud",
          "dataflow",
          "flex-template",
          "build",
          templatePath,
          "--image",
          imagePath,
          "--project",
          projectId,
          "--sdk-language",
          definition.getTemplateAnnotation().type().name(),
          "--metadata-file",
          outputClassesDirectory.getAbsolutePath() + "/" + metadataFile.getName(),
          "--additional-user-labels",
          "goog-dataflow-provided-template-name="
              + currentTemplateName.toLowerCase()
              + ",goog-dataflow-provided-template-version="
              + TemplateDefinitionsParser.parseVersion(stagePrefix)
              + ",goog-dataflow-provided-template-type=flex"
        };
    LOG.info("Running: {}", String.join(" ", flexTemplateBuildCmd));

    Process process = Runtime.getRuntime().exec(flexTemplateBuildCmd);

    if (process.waitFor() != 0) {
      throw new RuntimeException(
          "Error building template using gcloud. Please make sure it is up to date. "
              + new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8)
              + "\n"
              + new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  private void stageUsingDockerfile(String imagePath, String containerName)
      throws IOException, InterruptedException {
    File directory = new File(outputClassesDirectory.getAbsolutePath() + "/" + containerName);

    File cloudbuildFile = File.createTempFile("cloudbuild", ".yaml");
    try (FileWriter writer = new FileWriter(cloudbuildFile)) {
      String cacheFolder = imagePath.substring(0, imagePath.lastIndexOf('/')) + "/cache";
      writer.write(
          "steps:\n"
              + "- name: gcr.io/kaniko-project/executor\n"
              + "  args:\n"
              + "  - --destination="
              + imagePath
              + "\n"
              + "  - --cache=true\n"
              + "  - --cache-ttl=6h\n"
              + "  - --compressed-caching=false\n"
              + "  - --cache-copy-layers=true\n"
              + "  - --cache-repo="
              + cacheFolder);
    }

    Process stageProcess =
        runCommand(
            new String[] {
              "gcloud",
              "builds",
              "submit",
              "--config",
              cloudbuildFile.getAbsolutePath(),
              "--machine-type",
              "e2-highcpu-8",
              "--disk-size",
              "200",
              "--project",
              projectId
            },
            directory);
    if (stageProcess.waitFor() != 0) {
      throw new RuntimeException(
          "Error building container image using gcloud. Check logs for details.");
    }
  }

  private static Process runCommand(String[] gcloudBuildsCmd, File directory) throws IOException {
    LOG.info("Running: {}", String.join(" ", gcloudBuildsCmd));

    Process process = Runtime.getRuntime().exec(gcloudBuildsCmd, null, directory);
    TemplatePluginUtils.redirectLinesLog(process.getInputStream(), LOG);
    TemplatePluginUtils.redirectLinesLog(process.getErrorStream(), LOG);
    return process;
  }
}
