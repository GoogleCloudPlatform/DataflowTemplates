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
import static org.twdata.maven.mojoexecutor.MojoExecutor.attribute;
import static org.twdata.maven.mojoexecutor.MojoExecutor.configuration;
import static org.twdata.maven.mojoexecutor.MojoExecutor.dependency;
import static org.twdata.maven.mojoexecutor.MojoExecutor.element;
import static org.twdata.maven.mojoexecutor.MojoExecutor.executeMojo;
import static org.twdata.maven.mojoexecutor.MojoExecutor.executionEnvironment;
import static org.twdata.maven.mojoexecutor.MojoExecutor.goal;
import static org.twdata.maven.mojoexecutor.MojoExecutor.plugin;

import com.google.cloud.teleport.metadata.Template.TemplateType;
import com.google.cloud.teleport.plugin.DockerfileGenerator;
import com.google.cloud.teleport.plugin.TemplateDefinitionsParser;
import com.google.cloud.teleport.plugin.TemplatePluginUtils;
import com.google.cloud.teleport.plugin.TemplateSpecsGenerator;
import com.google.cloud.teleport.plugin.model.ImageSpec;
import com.google.cloud.teleport.plugin.model.TemplateDefinitions;
import com.google.common.base.Strings;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import freemarker.template.TemplateException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.model.Plugin;
import org.apache.maven.plugin.BuildPluginManager;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.checkerframework.checker.nullness.qual.Nullable;
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

  /**
   * Artifact registry.
   *
   * <p>If not set, images will be built to [artifactRegion.]gcr.io/[projectId].
   *
   * <p>If set to "xxx.gcr.io", image will be built to xxx.gcr.io/[projectId].
   *
   * <p>Otherwise, image will be built to artifactRegion.
   */
  @Parameter(defaultValue = "${artifactRegistry}", readonly = true, required = false)
  protected String artifactRegistry;

  /**
   * Staging artifact registry.
   *
   * <p>If set, images will first build inside stagingArtifactRegistry before promote to final
   * destination. Only effective when generateSBOM.
   */
  @Parameter(defaultValue = "${stagingArtifactRegistry}", readonly = true, required = false)
  protected String stagingArtifactRegistry;

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

  @Parameter(defaultValue = "${saSecretName}", readonly = true, required = false)
  protected String saSecretName;

  @Parameter(defaultValue = "${airlockPythonRepo}", readonly = true, required = false)
  protected String airlockPythonRepo;

  @Parameter(defaultValue = "${airlockJavaRepo}", readonly = true, required = false)
  protected String airlockJavaRepo;

  @Parameter(defaultValue = "false", property = "generateSBOM", readonly = true, required = false)
  protected boolean generateSBOM;

  private boolean internalMaven;

  private String mavenRepo;

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
      String basePythonContainerImage,
      String pythonTemplateLauncherEntryPoint,
      String javaTemplateLauncherEntryPoint,
      String pythonVersion,
      String beamVersion,
      String artifactRegistry,
      String stagingArtifactRegistry,
      boolean unifiedWorker,
      boolean generateSBOM) {
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
    this.pythonTemplateLauncherEntryPoint = pythonTemplateLauncherEntryPoint;
    this.javaTemplateLauncherEntryPoint = javaTemplateLauncherEntryPoint;
    this.pythonVersion = pythonVersion;
    this.beamVersion = beamVersion;
    this.artifactRegistry = artifactRegistry;
    this.stagingArtifactRegistry = stagingArtifactRegistry;
    this.unifiedWorker = unifiedWorker;
    this.internalMaven = false;
    this.generateSBOM = generateSBOM;
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
          TemplateDefinitionsParser.scanDefinitions(loader, outputDirectory);
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
        stageTemplate(definition, imageSpec, pluginManager);
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
    if (imageSpec.getMetadata().isStreaming()) {
      // Default to THROUGHPUT_BASED autoscaling for streaming classic templates
      arguments.add(element("argument", "--autoscalingAlgorithm=THROUGHPUT_BASED"));
      arguments.add(element("argument", "--maxNumWorkers=5"));
    }

    if (gcpTempLocation != null) {
      String gcpTempLocationPath = "gs://" + bucketNameOnly(gcpTempLocation) + "/temp/";
      arguments.add(element("argument", "--gcpTempLocation=" + gcpTempLocationPath));
      arguments.add(element("argument", "--tempLocation=" + gcpTempLocationPath));
    }

    if (unifiedWorker) {
      arguments.add(element("argument", "--experiments=use_runner_v2"));
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

    gcsCopy(metadataFile.getAbsolutePath(), templateMetadataPath);

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

    // These are set by the .mvn/settings.xml file. This tells the plugin to use Airlock repos
    // for building artifacts in Dockerfile-based images (XLANG, PYTHON, YAML). Airlock deps are
    // only available when running PRs on DataflowTemplates GitHub repo and when releasing
    // internally, so avoid specifying these 3 parameters when building custom templates externally.
    if (!Strings.isNullOrEmpty(saSecretName)
        && !Strings.isNullOrEmpty(airlockPythonRepo)
        && !Strings.isNullOrEmpty(airlockJavaRepo)) {
      internalMaven = true;
    }

    String maybeMavenRepo = project.getProperties().getProperty("beam-maven-repo");
    if (!Strings.isNullOrEmpty(maybeMavenRepo)) {
      maybeMavenRepo = maybeMavenRepo.replaceAll("/$", "");
    }
    this.mavenRepo = maybeMavenRepo;

    // Override some image spec attributes available only during staging/release:
    String version = TemplateDefinitionsParser.parseVersion(stagePrefix);
    String containerName = definition.getTemplateAnnotation().flexContainerName();
    boolean stageImageOnly = definition.getTemplateAnnotation().stageImageOnly();
    imageSpec.setAdditionalUserLabel("goog-dataflow-provided-template-version", version);
    imageSpec.setImage(
        generateFlexTemplateImagePath(
            containerName,
            projectId,
            artifactRegion,
            artifactRegistry,
            stagePrefix,
            stageImageOnly));

    if (beamVersion == null || beamVersion.isEmpty()) {
      beamVersion = project.getProperties().getProperty("beam-python.version");
    }

    String currentTemplateName = definition.getTemplateAnnotation().name();
    TemplateSpecsGenerator generator = new TemplateSpecsGenerator();

    boolean stageImageBeforePromote =
        generateSBOM && !Strings.isNullOrEmpty(stagingArtifactRegistry);
    String imagePath =
        stageImageBeforePromote
            ? generateFlexTemplateImagePath(
                containerName,
                projectId,
                null,
                stagingArtifactRegistry,
                stagePrefix,
                stageImageOnly)
            : imageSpec.getImage();
    String buildProjectId =
        stageImageBeforePromote
            ? new PromoteHelper.ArtifactRegImageSpec(imagePath).project
            : projectId;
    LOG.info("Stage image to GCR: {}", imagePath);

    String metadataFile = "";
    if (!stageImageOnly) {
      metadataFile =
          generator
              .saveMetadata(definition, imageSpec.getMetadata(), outputClassesDirectory)
              .getName();
    }

    File xlangOutputDir;
    File commandSpecFile;
    if (definition.getTemplateAnnotation().type() == TemplateType.XLANG) {
      xlangOutputDir =
          new File(outputClassesDirectory.getPath() + "/" + containerName + "/resources");
      commandSpecFile = generator.saveCommandSpec(definition, xlangOutputDir);
    } else {
      commandSpecFile = generator.saveCommandSpec(definition, outputClassesDirectory);
    }
    String appRoot = "/template/" + containerName;
    String commandSpec = appRoot + "/resources/" + commandSpecFile.getName();

    String templatePath =
        "gs://" + bucketNameOnly(bucketName) + "/" + stagePrefix + "/flex/" + currentTemplateName;
    File imageSpecFile = null;

    if (definition.getTemplateAnnotation().type() == TemplateType.JAVA
        || definition.getTemplateAnnotation().type() == TemplateType.XLANG) {
      stageFlexJavaTemplate(
          definition,
          pluginManager,
          currentTemplateName,
          buildProjectId,
          imagePath,
          metadataFile,
          appRoot,
          commandSpec,
          commandSpecFile.getName(),
          templatePath);

      // stageFlexJavaTemplate calls `gcloud dataflow flex-template build` command, which takes
      // metadataFile as input and generates its own imageSpecFile at templatePath location, but it
      // doesn't use metadataFile as-is and only picks a few attributes from it.
      // Below, we are going to override this file with the one generated by the plugin, to avoid
      // having a dependency on gcloud CLI. Otherwise every time a new attribute is added to the
      // metadata we'll have to update gcloud CLI logic accordingly.
      // TODO: Check if the same should be applied to Python templates:
      if (!stageImageOnly) {
        imageSpecFile = generator.saveImageSpec(definition, imageSpec, outputClassesDirectory);
        LOG.info(
            "Overriding Flex template spec file generated by gcloud command at [{}] with local file"
                + " [{}]",
            templatePath,
            imageSpecFile.getName());
      }
    } else if (definition.getTemplateAnnotation().type() == TemplateType.PYTHON) {
      stageFlexPythonTemplate(
          definition,
          currentTemplateName,
          buildProjectId,
          imagePath,
          metadataFile,
          containerName,
          templatePath);
    } else if (definition.getTemplateAnnotation().type() == TemplateType.YAML) {
      stageFlexYamlTemplate(
          definition,
          currentTemplateName,
          buildProjectId,
          imagePath,
          metadataFile,
          containerName,
          templatePath);
    } else {
      throw new IllegalArgumentException(
          "Type not known: " + definition.getTemplateAnnotation().type());
    }

    if (generateSBOM) {
      // generate SBOM
      File buildDir = new File(outputClassesDirectory.getAbsolutePath());
      performVulnerabilityScanAndGenerateUserSBOM(imagePath, buildProjectId, buildDir);
      GenerateSBOMRunnable runnable = new GenerateSBOMRunnable(imagePath);
      Failsafe.with(GenerateSBOMRunnable.sbomRetryPolicy()).run(runnable);
      String digest = runnable.getDigest();

      if (stageImageBeforePromote) {
        // promote image
        PromoteHelper promoteHelper = new PromoteHelper(imagePath, imageSpec.getImage(), digest);
        promoteHelper.promote();

        if (!stageImageOnly) {
          // overwrite image spec file
          if (imageSpecFile == null) {
            File folder = new File(outputClassesDirectory.getAbsolutePath() + containerName);
            if (!folder.exists()) {
              folder.mkdir();
            }
            imageSpecFile = new File(folder, currentTemplateName + "-spec-generated-metadata.json");
            gcsCopy(templatePath, imageSpecFile.getAbsolutePath());
          }

          String content =
              new String(Files.readAllBytes(imageSpecFile.toPath()), StandardCharsets.UTF_8);
          String replaced = content.replace(imagePath, imageSpec.getImage());
          // verify we have replaced the image path. Note: the file content may already have the
          // final target image path if it was overwritten before (see "Overriding Flex template
          // spec file ...") above
          if (replaced.equals(content) && !content.contains(imageSpec.getImage())) {
            throw new RuntimeException(
                String.format(
                    "Unable overwrite %s to %s. Content: %s",
                    imagePath, imageSpec.getImage(), content.substring(0, 1000)));
          }
          Files.writeString(imageSpecFile.toPath(), replaced);
        }
      }
    }

    if (imageSpecFile != null) {
      gcsCopy(imageSpecFile.getAbsolutePath(), templatePath);
    }

    LOG.info("Flex Template was staged! {}", stageImageOnly ? imageSpec.getImage() : templatePath);
    return templatePath;
  }

  private void stageFlexJavaTemplate(
      TemplateDefinitions definition,
      BuildPluginManager pluginManager,
      String currentTemplateName,
      String buildProjectId,
      String imagePath,
      String metadataFile,
      String appRoot,
      String commandSpec,
      String commandSpecFileName,
      String templatePath)
      throws MojoExecutionException, IOException, InterruptedException, TemplateException {
    String containerName = definition.getTemplateAnnotation().flexContainerName();
    String tarFileName =
        String.format("%s/%s/%s.tar", outputDirectory.getPath(), containerName, containerName);
    Plugin plugin =
        plugin(
            "com.google.cloud.tools",
            "jib-maven-plugin",
            null,
            List.of(
                dependency("com.google.cloud.tools", "jib-layer-filter-extension-maven", "0.3.0")));
    List<Element> elements = new ArrayList<>();

    // Base image to use
    elements.add(element("from", element("image", baseContainerImage)));

    // Target image to stage
    elements.add(element("to", element("image", imagePath)));
    elements.add(
        element(
            "container",
            element("appRoot", appRoot),
            // Keep the original entrypoint
            element("entrypoint", "INHERIT"),
            // Point to the command spec
            element("environment", element("DATAFLOW_JAVA_COMMAND_SPEC", commandSpec))));
    elements.add(element("outputPaths", element("tar", tarFileName)));

    // Only use shaded JAR and exclude libraries if shade was not disabled
    if (System.getProperty("skipShade") == null
        || System.getProperty("skipShade").equalsIgnoreCase("false")) {
      elements.add(
          element(
              "extraDirectories",
              element(
                  "paths",
                  element(
                      "path",
                      element("from", targetDirectory + "/classes"),
                      element("includes", commandSpecFileName),
                      element("into", "/template/" + containerName + "/resources")))));

      elements.add(element("containerizingMode", "packaged"));
      elements.add(
          element(
              "pluginExtensions",
              element(
                  "pluginExtension",
                  element(
                      "implementation",
                      "com.google.cloud.tools.jib.maven.extension.layerfilter.JibLayerFilterExtension"),
                  element(
                      "configuration",
                      attribute(
                          "implementation",
                          "com.google.cloud.tools.jib.maven.extension.layerfilter.Configuration"),
                      element(
                          "filters",
                          element("filter", element("glob", "**/libs/*.jar")),
                          element(
                              "filter",
                              element("glob", "**/libs/conscrypt-openjdk-uber-*.jar"),
                              element("toLayer", "conscrypt")))))));
    }
    // X-lang templates need to have a custom image which builds both python and java.
    String[] flexTemplateBuildCmd;
    if (definition.getTemplateAnnotation().type() == TemplateType.XLANG) {
      String dockerfileContainer = outputClassesDirectory.getPath() + "/" + containerName;
      String dockerfilePath = dockerfileContainer + "/Dockerfile";
      File dockerfile = new File(dockerfilePath);
      if (!dockerfile.exists()) {
        List<String> filesToCopy = List.of(definition.getTemplateAnnotation().filesToCopy());
        if (filesToCopy.isEmpty()) {
          filesToCopy =
              List.of(
                  String.format("%s-generated-metadata.json", containerName), "requirements.txt");
        }
        List<String> entryPoint = List.of(definition.getTemplateAnnotation().entryPoint());
        if (entryPoint.isEmpty()) {
          entryPoint = List.of(javaTemplateLauncherEntryPoint);
        }
        String xlangCommandSpec =
            "/template/" + containerName + "/resources/" + commandSpecFileName;

        // Copy in requirements.txt if present
        File sourceRequirements = new File(outputClassesDirectory.getPath() + "/requirements.txt");
        File destRequirements = new File(dockerfileContainer + "/requirements.txt");
        if (sourceRequirements.exists()) {
          Files.copy(
              sourceRequirements.toPath(),
              destRequirements.toPath(),
              StandardCopyOption.REPLACE_EXISTING);
        }

        // Generate Dockerfile
        LOG.info("Generating dockerfile " + dockerfilePath);
        Set<String> directoriesToCopy = Set.of(containerName);
        DockerfileGenerator.Builder dockerfileBuilder =
            DockerfileGenerator.builder(
                    definition.getTemplateAnnotation().type(),
                    beamVersion,
                    containerName,
                    outputClassesDirectory)
                .setBasePythonContainerImage(basePythonContainerImage)
                .setBaseJavaContainerImage(baseContainerImage)
                .setPythonVersion(pythonVersion)
                .setEntryPoint(entryPoint)
                .setCommandSpec(xlangCommandSpec)
                .setFilesToCopy(filesToCopy)
                .setDirectoriesToCopy(directoriesToCopy);

        // Set Airlock parameters
        if (internalMaven) {
          dockerfileBuilder
              .setServiceAccountSecretName(saSecretName)
              .setAirlockPythonRepo(airlockPythonRepo);
        }
        if (!Strings.isNullOrEmpty(mavenRepo)) {
          dockerfileBuilder.setMavenRepo(mavenRepo);
        }

        dockerfileBuilder.build().generate();
      }

      // Copy java classes and libs to build directory
      copyJavaArtifacts(containerName, targetDirectory, project.getArtifact().getFile());

      LOG.info("Staging XLANG image using Dockerfile");
      stageXlangUsingDockerfile(imagePath, containerName, buildProjectId);
    } else {
      // Jib's LayerFilter extension is not thread-safe, do only one at a time
      synchronized (TemplatesStageMojo.class) {
        executeMojo(
            plugin,
            goal(generateSBOM ? "buildTar" : "build"),
            configuration(elements.toArray(new Element[elements.size()])),
            executionEnvironment(project, session, pluginManager));
      }

      if (generateSBOM) {
        // Send image tar to Cloud Build for vulnerability scanning before pushing
        LOG.info("Using Cloud Build to push image {}", imagePath);
        stageFlexTemplateUsingCloudBuild(new File(tarFileName), imagePath, buildProjectId);
      }
    }

    // Skip GCS spec file creation
    if (definition.getTemplateAnnotation().stageImageOnly()) {
      return;
    }

    flexTemplateBuildCmd =
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
          "JAVA",
          "--metadata-file",
          outputClassesDirectory.getAbsolutePath() + "/" + metadataFile,
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

  private void stageFlexYamlTemplate(
      TemplateDefinitions definition,
      String currentTemplateName,
      String buildProjectId,
      String imagePath,
      String metadataFile,
      String containerName,
      String templatePath)
      throws IOException, InterruptedException, TemplateException {

    // extract image properties for Dockerfile
    String dockerfilePath = outputClassesDirectory.getPath() + "/" + containerName + "/Dockerfile";
    File dockerfile = new File(dockerfilePath);
    if (!dockerfile.exists()) {
      List<String> filesToCopy = List.of(definition.getTemplateAnnotation().filesToCopy());
      if (filesToCopy.isEmpty()) {
        filesToCopy = List.of("main.py", "requirements.txt");
      }
      List<String> entryPoint = List.of(definition.getTemplateAnnotation().entryPoint());
      if (entryPoint.isEmpty()) {
        entryPoint = List.of(pythonTemplateLauncherEntryPoint);
      }

      // Generate Dockerfile
      LOG.info("Generating dockerfile " + dockerfilePath);
      DockerfileGenerator.Builder dockerfileBuilder =
          DockerfileGenerator.builder(
                  definition.getTemplateAnnotation().type(),
                  beamVersion,
                  containerName,
                  outputClassesDirectory)
              .setBasePythonContainerImage(basePythonContainerImage)
              .setBaseJavaContainerImage(baseContainerImage)
              .setPythonVersion(pythonVersion)
              .setEntryPoint(entryPoint)
              .setFilesToCopy(filesToCopy);

      // Set Airlock parameters
      if (internalMaven) {
        dockerfileBuilder
            .setServiceAccountSecretName(saSecretName)
            .setAirlockPythonRepo(airlockPythonRepo)
            .setAirlockJavaRepo(airlockJavaRepo);
      }
      if (!Strings.isNullOrEmpty(mavenRepo)) {
        dockerfileBuilder.setMavenRepo(mavenRepo);
      }
      dockerfileBuilder.build().generate();
    }

    LOG.info("Staging YAML image using Dockerfile");
    stageYamlUsingDockerfile(buildProjectId, imagePath, containerName);

    // Skip GCS spec file creation
    if (definition.getTemplateAnnotation().stageImageOnly()) {
      return;
    }

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
          buildProjectId,
          "--sdk-language",
          "PYTHON",
          "--metadata-file",
          outputClassesDirectory.getAbsolutePath() + "/" + metadataFile,
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
      String buildProjectId,
      String imagePath,
      String metadataFile,
      String containerName,
      String templatePath)
      throws IOException, InterruptedException, TemplateException {
    String dockerfileContainer = outputClassesDirectory.getPath() + "/" + containerName;
    String dockerfilePath = dockerfileContainer + "/Dockerfile";
    File dockerfile = new File(dockerfilePath);
    if (!dockerfile.exists()) {
      List<String> filesToCopy = List.of(definition.getTemplateAnnotation().filesToCopy());
      if (filesToCopy.isEmpty()) {
        filesToCopy = List.of("main.py", "requirements.txt");
      }
      List<String> entryPoint = List.of(definition.getTemplateAnnotation().entryPoint());
      if (entryPoint.isEmpty()) {
        entryPoint = List.of(pythonTemplateLauncherEntryPoint);
      }

      // Copy in requirements.txt if present
      File sourceRequirements = new File(outputClassesDirectory.getPath() + "/requirements.txt");
      File destRequirements = new File(dockerfileContainer + "/requirements.txt");
      if (sourceRequirements.exists()) {
        Files.copy(
            sourceRequirements.toPath(),
            destRequirements.toPath(),
            StandardCopyOption.REPLACE_EXISTING);
      }

      // Generate Dockerfile
      LOG.info("Generating dockerfile " + dockerfilePath);
      DockerfileGenerator.Builder dockerfileBuilder =
          DockerfileGenerator.builder(
                  definition.getTemplateAnnotation().type(),
                  beamVersion,
                  containerName,
                  targetDirectory)
              .setBasePythonContainerImage(basePythonContainerImage)
              .setFilesToCopy(filesToCopy)
              .setEntryPoint(entryPoint);

      // Set Airlock parameters
      if (internalMaven) {
        dockerfileBuilder
            .setServiceAccountSecretName(saSecretName)
            .setAirlockPythonRepo(airlockPythonRepo);
      }
      if (!Strings.isNullOrEmpty(mavenRepo)) {
        dockerfileBuilder.setMavenRepo(mavenRepo);
      }
      dockerfileBuilder.build().generate();
    }

    LOG.info("Staging PYTHON image using Dockerfile");
    stagePythonUsingDockerfile(buildProjectId, imagePath, containerName);

    // Skip GCS spec file creation
    if (definition.getTemplateAnnotation().stageImageOnly()) {
      return;
    }

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
          buildProjectId,
          "--sdk-language",
          definition.getTemplateAnnotation().type().name(),
          "--metadata-file",
          outputClassesDirectory.getAbsolutePath() + "/" + metadataFile,
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

  private void stageYamlUsingDockerfile(
      String buildProjectId, String imagePath, String yamlTemplateName)
      throws IOException, InterruptedException {
    File directory = new File(outputClassesDirectory.getAbsolutePath() + "/" + yamlTemplateName);

    File cloudbuildFile = File.createTempFile("cloudbuild", ".yaml");
    try (FileWriter writer = new FileWriter(cloudbuildFile)) {
      String cacheFolder = imagePath.substring(0, imagePath.lastIndexOf('/')) + "/cache";
      String tarPath = "/workspace/" + yamlTemplateName + ".tar\n";
      writer.write(
          "steps:\n"
              + "- name: gcr.io/kaniko-project/executor\n"
              + "  args:\n"
              + "  - --destination="
              + imagePath
              + "\n"
              + "  - --dockerfile=Dockerfile\n"
              + "  - --cache=true\n"
              + "  - --cache-ttl=6h\n"
              + "  - --compressed-caching=false\n"
              + "  - --cache-copy-layers=true\n"
              + "  - --cache-repo="
              + cacheFolder
              + (generateSBOM
                  ? "\n"
                      + "  - --no-push\n"
                      + "  - --tar-path="
                      + tarPath
                      + "\n"
                      + "- name: 'gcr.io/cloud-builders/docker'\n"
                      + "  args:\n"
                      + "  - load\n"
                      + "  - --input="
                      + tarPath
                      + "\n"
                      + "images: ['"
                      + imagePath
                      + "']\n"
                      + "options:\n"
                      + "  logging: CLOUD_LOGGING_ONLY\n"
                      + "  requestedVerifyOption: VERIFIED"
                  : "\noptions:\n" + "  logging: CLOUD_LOGGING_ONLY\n"));
    }

    LOG.info("Submitting Cloud Build job with config: " + cloudbuildFile.getAbsolutePath());
    StringBuilder cloudBuildLogs = new StringBuilder();
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
              buildProjectId
            },
            directory,
            cloudBuildLogs);

    int retval = stageProcess.waitFor();
    if (retval != 0) {
      throw new RuntimeException(
          String.format(
              "Error building yaml image using gcloud. Code %d. Check logs for details.\n%s",
              retval, cloudBuildLogs));
    }
  }

  private void stagePythonUsingDockerfile(
      String buildProjectId, String imagePath, String containerName)
      throws IOException, InterruptedException {
    File directory = new File(outputClassesDirectory.getAbsolutePath() + "/" + containerName);

    File cloudbuildFile = File.createTempFile("cloudbuild", ".yaml");
    try (FileWriter writer = new FileWriter(cloudbuildFile)) {
      String cacheFolder = imagePath.substring(0, imagePath.lastIndexOf('/')) + "/cache";
      String tarPath = "/workspace/" + containerName + ".tar\n";
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
              + cacheFolder
              + (generateSBOM
                  ? "\n"
                      + "  - --no-push\n"
                      + "  - --tar-path="
                      + tarPath
                      + "\n"
                      + "- name: 'gcr.io/cloud-builders/docker'\n"
                      + "  args:\n"
                      + "  - load\n"
                      + "  - --input="
                      + tarPath
                      + "\n"
                      + "images: ['"
                      + imagePath
                      + "']\n"
                      + "options:\n"
                      + "  logging: CLOUD_LOGGING_ONLY\n"
                      + "  requestedVerifyOption: VERIFIED"
                  : "\noptions:\n" + "  logging: CLOUD_LOGGING_ONLY\n"));
    }

    LOG.info("Submitting Cloud Build job with config: " + cloudbuildFile.getAbsolutePath());
    StringBuilder cloudBuildLogs = new StringBuilder();
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
              buildProjectId
            },
            directory,
            cloudBuildLogs);

    int retval = stageProcess.waitFor();
    if (retval != 0) {
      throw new RuntimeException(
          String.format(
              "Error building Python image using gcloud. Code %d. Check logs for details. %s",
              retval, cloudBuildLogs));
    }
  }

  static String generateFlexTemplateImagePath(
      String containerName,
      String projectId,
      String artifactRegion,
      String artifactRegistry,
      String stagePrefix,
      boolean skipStagingPart) {
    String prefix = Strings.isNullOrEmpty(artifactRegion) ? "" : artifactRegion + ".";
    // GCR paths can not contain ":", if the project id has it, it should be converted to "/".
    String projectIdUrl = Strings.isNullOrEmpty(projectId) ? "" : projectId.replace(':', '/');
    String stagingPart = skipStagingPart ? "" : stagePrefix.toLowerCase() + "/";
    return Optional.ofNullable(artifactRegistry)
        .map(
            value ->
                value.endsWith("gcr.io")
                    ? value + "/" + projectIdUrl + "/" + stagingPart + containerName
                    : value + "/" + stagingPart + containerName)
        .orElse(prefix + "gcr.io/" + projectIdUrl + "/" + stagingPart + containerName);
  }

  private void gcsCopy(String fromPath, String toPath) throws InterruptedException, IOException {
    String[] copyCmd =
        new String[] {"gcloud", "storage", "cp", fromPath, toPath, "--project", projectId};
    LOG.info("Running: {}", String.join(" ", copyCmd));

    Process process = Runtime.getRuntime().exec(copyCmd);

    if (process.waitFor() != 0) {
      throw new RuntimeException(
          "Error copying using 'gcloud storage'. Please make sure it is up to date. "
              + new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8)
              + "\n"
              + new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  private void stageFlexTemplateUsingCloudBuild(
      File tarFile, String imagePath, String buildProjectId)
      throws IOException, InterruptedException {
    File directory = tarFile.getParentFile();

    File cloudbuildFile = File.createTempFile(directory + "/cloudbuild", ".yaml");
    try (FileWriter writer = new FileWriter(cloudbuildFile)) {
      writer.write(
          "steps:\n"
              + "- name: 'gcr.io/cloud-builders/docker'\n"
              + "  args:\n"
              + "  - load\n"
              + "  - --input="
              + tarFile.getName()
              + "\n"
              + "images: ['"
              + imagePath
              + "']\n"
              + "options:\n"
              + "  logging: CLOUD_LOGGING_ONLY\n"
              + "  requestedVerifyOption: VERIFIED");
    }

    LOG.info("Submitting Cloud Build job with config: " + cloudbuildFile.getAbsolutePath());
    StringBuilder cloudBuildLogs = new StringBuilder();
    Process stageProcess =
        runCommand(
            new String[] {
              "gcloud",
              "builds",
              "submit",
              "--config",
              cloudbuildFile.getAbsolutePath(),
              "--project",
              buildProjectId
            },
            directory,
            cloudBuildLogs);

    int retval = stageProcess.waitFor();
    if (retval != 0) {
      throw new RuntimeException(
          String.format(
              "Possible error building Flex image using gcloud. Code %d. Check logs for details. %s",
              retval, cloudBuildLogs));
    }
  }

  private void stageXlangUsingDockerfile(
      String imagePath, String containerName, String buildProjectId)
      throws IOException, InterruptedException {
    String dockerfile = containerName + "/Dockerfile";
    File directory = new File(outputClassesDirectory.getAbsolutePath());

    File cloudbuildFile = File.createTempFile("cloudbuild", ".yaml");
    String tarPath = "/workspace/" + containerName + ".tar\n";
    try (FileWriter writer = new FileWriter(cloudbuildFile)) {
      String cacheFolder = imagePath.substring(0, imagePath.lastIndexOf('/')) + "/cache";
      writer.write(
          "steps:\n"
              + "- name: gcr.io/kaniko-project/executor\n"
              + "  args:\n"
              + "  - --destination="
              + imagePath
              + "\n"
              + "  - --dockerfile="
              + dockerfile
              + "\n"
              + "  - --cache=true\n"
              + "  - --cache-ttl=6h\n"
              + "  - --compressed-caching=false\n"
              + "  - --cache-copy-layers=true\n"
              + "  - --cache-repo="
              + cacheFolder
              + (generateSBOM
                  ? "\n"
                      + "  - --no-push\n"
                      + "  - --tar-path="
                      + tarPath
                      + "\n"
                      + "- name: 'gcr.io/cloud-builders/docker'\n"
                      + "  args:\n"
                      + "  - load\n"
                      + "  - --input="
                      + tarPath
                      + "\n"
                      + "images: ['"
                      + imagePath
                      + "']\n"
                      + "options:\n"
                      + "  logging: CLOUD_LOGGING_ONLY\n"
                      + "  requestedVerifyOption: VERIFIED"
                  : "\noptions:\n" + "  logging: CLOUD_LOGGING_ONLY\n"));
    }

    LOG.info("Submitting Cloud Build job with config: " + cloudbuildFile.getAbsolutePath());
    StringBuilder cloudBuildLogs = new StringBuilder();
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
              buildProjectId
            },
            directory,
            cloudBuildLogs);

    int retval = stageProcess.waitFor();
    if (retval != 0) {
      throw new RuntimeException(
          String.format(
              "Possible error building Xlang image using gcloud. Code %d. Check logs for details. %s",
              retval, cloudBuildLogs));
    }
  }

  private static void copyJavaArtifacts(
      String containerName, File targetDirectory, File artifactFile) throws IOException {

    String classesDirectory = targetDirectory.getPath() + "/classes";
    try {
      Files.createDirectories(Path.of(classesDirectory + "/" + containerName + "/classpath"));
      Files.createDirectories(Path.of(classesDirectory + "/" + containerName + "/libs"));

      String artifactPath = artifactFile.getPath();
      String targetArtifactPath =
          artifactPath.substring(artifactPath.lastIndexOf("/"), artifactPath.length());

      Files.copy(
          Path.of(targetDirectory.getPath() + targetArtifactPath),
          Path.of(classesDirectory + "/" + containerName + "/classpath" + targetArtifactPath));
      String sourceLibsDirectory = targetDirectory.getPath() + "/extra_libs";
      String destLibsDirectory = classesDirectory + "/" + containerName + "/libs/";
      Files.walk(Paths.get(sourceLibsDirectory))
          .forEach(
              source -> {
                LOG.warn("current source: " + source.toString());
                LOG.warn("current source libs directory: " + sourceLibsDirectory);
                Path dest =
                    Paths.get(
                        destLibsDirectory,
                        source.toString().substring(sourceLibsDirectory.length()));
                try {
                  Files.copy(source, dest);
                } catch (IOException e) {
                  LOG.warn("Unable to copy contents of " + sourceLibsDirectory);
                }
              });
    } catch (Exception e) {
      LOG.warn("unable to copy jar files");
      throw e;
    }
  }

  private static void performVulnerabilityScanAndGenerateUserSBOM(
      String imagePath, String buildProjectId, File buildDir)
      throws IOException, InterruptedException {
    LOG.info("Generating user SBOM and Performing security scan for {}...", imagePath);

    File cloudbuildFile = File.createTempFile("cloudbuild", ".yaml");
    try (FileWriter writer = new FileWriter(cloudbuildFile)) {
      writer.write(
          "steps:\n"
              + "- name: 'gcr.io/cloud-builders/docker:24.0.9'\n"
              + "  entrypoint: bash\n"
              + "  args:\n"
              + "  - -c\n"
              + "  - |-\n"
              + "    mkdir -p ~/.docker/cli-plugins\n"
              + "    curl -sSfL https://raw.githubusercontent.com/docker/sbom-cli-plugin/main/install.sh | sh -s --\n"
              + "    docker sbom "
              + imagePath
              + " --format=spdx-json --output=/workspace/user-sbom.json\n"
              + "- name: 'gcr.io/google.com/cloudsdktool/cloud-sdk'\n"
              + "  entrypoint: gcloud\n"
              + "  args:\n"
              + "  - artifacts\n"
              + "  - sbom\n"
              + "  - load\n"
              + "  - --source=/workspace/user-sbom.json\n"
              + "  - --uri="
              + imagePath
              + "\n"
              + "- name: 'us-docker.pkg.dev/scaevola-builder-integration/release/scanvola/scanvola'\n"
              + "  args:\n"
              + "  - --image="
              + imagePath
              + ":latest");
    }

    LOG.info("Submitting Cloud Build job with config: " + cloudbuildFile.getAbsolutePath());
    StringBuilder cloudBuildLogs = new StringBuilder();
    Process stageProcess =
        runCommand(
            new String[] {
              "gcloud",
              "builds",
              "submit",
              "--config",
              cloudbuildFile.getAbsolutePath(),
              "--project",
              buildProjectId
            },
            buildDir,
            cloudBuildLogs);

    // Ideally this should raise an exception, but this returns NZE even for
    // successful runs.
    if (stageProcess.waitFor() != 0) {
      LOG.warn(
          "Scanning container nonzero return code. This does not necessarily mean an error. "
              + "Check logs for details. "
              + cloudBuildLogs);
    }
  }

  /** A runnable used for generating system SBOM, fetching image digest for retrivial. */
  private static class GenerateSBOMRunnable implements dev.failsafe.function.CheckedRunnable {
    private static final Pattern IMAGE_WITH_SBOM_DIGEST =
        Pattern.compile("@(?<DIGEST>sha256:[0-9a-f]{64})");
    private String digest;
    private final String imagePath;

    public String getDigest() {
      return digest;
    }

    public GenerateSBOMRunnable(String imagePath) {
      this.imagePath = imagePath;
    }

    @Override
    public void run() throws Throwable {
      LOG.info("Generating system SBOM for {}...", imagePath);
      String output;
      try {
        output =
            runCommandCapturesOutput(
                new String[] {
                  "gcloud", "artifacts", "sbom", "export", "--uri", imagePath + ":latest"
                },
                null);
      } catch (Exception e) {
        throw new RuntimeException("Error generating SBOM.", e);
      }
      Matcher matcher = IMAGE_WITH_SBOM_DIGEST.matcher(output);
      if (!matcher.find()) {
        throw new RuntimeException(
            String.format("Cannot obtain image digest from response: %s", output));
      }
      digest = matcher.group("DIGEST");
    }

    private static <T> RetryPolicy<T> sbomRetryPolicy() {
      return RetryPolicy.<T>builder()
          .handleIf(
              throwable ->
                  throwable.getMessage() != null
                      && throwable.getMessage().contains("Error generating SBOM."))
          .withBackoff(Duration.ofSeconds(10), Duration.ofSeconds(60))
          .withMaxRetries(5)
          .build();
    }
  }

  /** Run a command in a subprocess, returns its stdout or stderr, whichever is non-empty. */
  @VisibleForTesting
  static String runCommandCapturesOutput(String[] gcloudBuildsCmd, @Nullable File directory)
      throws IOException, InterruptedException {
    // Do not output whole command to avoid print token
    LOG.info("Running: {}", gcloudBuildsCmd[0]);
    Process process = Runtime.getRuntime().exec(gcloudBuildsCmd, null, directory);
    int retCode = process.waitFor();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    String outputErr = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
    if (retCode != 0) {
      throw new RuntimeException(
          "Error invoking command. Code: " + retCode + "." + output + outputErr);
    }
    LOG.debug(output + outputErr);
    return Strings.isNullOrEmpty(output) ? outputErr : output;
  }

  static Process runCommand(String[] gcloudBuildsCmd, File directory, StringBuilder cloudBuildLogs)
      throws IOException {
    LOG.info("Running: {}", String.join(" ", gcloudBuildsCmd));

    Process process = Runtime.getRuntime().exec(gcloudBuildsCmd, null, directory);
    TemplatePluginUtils.redirectLinesLog(process.getInputStream(), LOG, cloudBuildLogs);
    TemplatePluginUtils.redirectLinesLog(process.getErrorStream(), LOG, cloudBuildLogs);
    return process;
  }
}
