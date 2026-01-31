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

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.Template.TemplateType;
import com.google.cloud.teleport.plugin.DockerfileGenerator;
import com.google.cloud.teleport.plugin.TemplateDefinitionsParser;
import com.google.cloud.teleport.plugin.TemplatePluginUtils;
import com.google.cloud.teleport.plugin.TemplateSpecsGenerator;
import com.google.cloud.teleport.plugin.model.ImageSpec;
import com.google.cloud.teleport.plugin.model.ImageSpecMetadata;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.ImmutablePair;
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

  @Parameter(defaultValue = "${flexContainerName}", readonly = true, required = false)
  protected String flexContainerName;

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
  // used to track if same images are scanned
  private static final Set<ImmutablePair<String, TemplateType>> SCANNED_TYPES = new HashSet<>();

  private String mavenRepo;

  private ContainerStageTracker containerStageTracker;

  public TemplatesStageMojo() {
    this.containerStageTracker = new ContainerStageTracker();
  }

  public TemplatesStageMojo(
      MavenProject project,
      MavenSession session,
      File outputDirectory,
      File outputClassesDirectory,
      File resourcesDirectory,
      File targetDirectory,
      String projectId,
      String templateName,
      String flexContainerName,
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
    this.flexContainerName = flexContainerName;
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
    this.containerStageTracker = new ContainerStageTracker();
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
      stageCommandSpecs(templateDefinitions);
      for (TemplateDefinitions definition : templateDefinitions) {

        ImageSpec imageSpec = definition.buildSpecModel(false);
        String currentTemplateName = definition.getTemplateAnnotation().name();
        String currentDisplayName = definition.getTemplateAnnotation().displayName();

        // Filter out the template if there was a specific one given
        if (!Strings.isNullOrEmpty(templateName)) {
          if (!templateName.equals(currentTemplateName)
              && !templateName.equals(currentDisplayName)) {
            LOG.info("Skipping template {} ({})", currentTemplateName, currentDisplayName);
            continue;
          }
        }
        if (!Strings.isNullOrEmpty(flexContainerName)) {
          if (!flexContainerName.equals(definition.getTemplateAnnotation().flexContainerName())) {
            LOG.info("Skipping template {} ({})", currentTemplateName, currentDisplayName);
            continue;
          }
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
   * Save command specs for templates. This is needed before staging any Java/XLang flex templates
   * as they share same image.
   */
  public void stageCommandSpecs(List<TemplateDefinitions> allDefinitions) {
    TemplateSpecsGenerator generator = new TemplateSpecsGenerator();
    for (TemplateDefinitions definition : allDefinitions) {
      if (!definition.isFlex()) {
        continue;
      }
      File xlangOutputDir;
      File commandSpecFile;
      Template annotation = definition.getTemplateAnnotation();
      String containerName = annotation.flexContainerName();
      if (definition.getTemplateAnnotation().type() == TemplateType.JAVA) {
        commandSpecFile = generator.saveCommandSpec(definition, outputClassesDirectory);
      } else if (definition.getTemplateAnnotation().type() == TemplateType.XLANG) {
        xlangOutputDir =
            new File(outputClassesDirectory.getPath() + "/" + containerName + "/resources");
        commandSpecFile = generator.saveCommandSpec(definition, xlangOutputDir);
      } else {
        continue;
      }
      containerStageTracker.addContainer(containerName, annotation.name(), commandSpecFile);
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
    String targetImagePath =
        generateFlexTemplateImagePath(containerName, projectId, artifactRegion, artifactRegistry);
    imageSpec.setImage(targetImagePath + ":" + stagePrefix);

    if (beamVersion == null || beamVersion.isEmpty()) {
      beamVersion = project.getProperties().getProperty("beam-python.version");
    }

    String currentTemplateName = definition.getTemplateAnnotation().name();
    TemplateSpecsGenerator generator = new TemplateSpecsGenerator();

    boolean stageImageBeforePromote =
        generateSBOM && !Strings.isNullOrEmpty(stagingArtifactRegistry);
    String imagePath =
        stageImageBeforePromote
            ? generateFlexTemplateImagePath(containerName, projectId, null, stagingArtifactRegistry)
            : targetImagePath;
    String imagePathTag = imagePath + ":" + stagePrefix;
    String buildProjectId =
        stageImageBeforePromote
            ? new PromoteHelper.ArtifactRegImageSpec(imagePath).project
            : projectId;
    LOG.info("Stage image to GCR: {}", imagePathTag);

    String metadataFile = "";
    if (!stageImageOnly) {
      metadataFile =
          generator
              .saveMetadata(definition, imageSpec.getMetadata(), outputClassesDirectory)
              .getName();
    }
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
          imagePathTag,
          metadataFile,
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
          imagePathTag,
          metadataFile,
          containerName,
          templatePath);
    } else if (definition.getTemplateAnnotation().type() == TemplateType.YAML) {

      stageFlexYamlTemplate(
          definition,
          currentTemplateName,
          buildProjectId,
          imagePathTag,
          metadataFile,
          containerName,
          templatePath);
    } else {
      throw new IllegalArgumentException(
          "Type not known: " + definition.getTemplateAnnotation().type());
    }

    if (generateSBOM) {
      if (!containerStageTracker.isStaged(containerName, currentTemplateName)) {
        // generate SBOM
        File buildDir = new File(outputClassesDirectory.getAbsolutePath());
        performVulnerabilityScanAndGenerateUserSBOM(
            imagePathTag, buildProjectId, buildDir, definition.getTemplateAnnotation().type());
        GenerateSBOMRunnable runnable = new GenerateSBOMRunnable(imagePathTag);
        Failsafe.with(GenerateSBOMRunnable.sbomRetryPolicy()).run(runnable);
        String digest = runnable.getDigest();

        if (stageImageBeforePromote) {
          // resolve tag to apply
          ImageSpecMetadata metadata = imageSpec.getMetadata();
          String trackTag = "public-image-latest";
          String dateSuffix =
              LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"));
          String deprecatedTag = "update-available-" + dateSuffix;
          if (metadata.isHidden()) {
            trackTag = "no-new-use-public-image-latest";
          } else if (metadata.getName().contains("[Deprecated]")) {
            trackTag = "deprecated-public-image-latest";
          }
          // promote image
          PromoteHelper promoteHelper =
              new PromoteHelper(
                  imagePath, targetImagePath, stagePrefix, trackTag, deprecatedTag, digest);
          promoteHelper.promote();
        }
      }

      if (stageImageBeforePromote) {
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
          String replaced = content.replace(imagePathTag, imageSpec.getImage());
          // verify we have replaced the image path. Note: the file content may already have the
          // final target image path if it was overwritten before (see "Overriding Flex template
          // spec file ...") above
          if (replaced.equals(content) && !content.contains(imageSpec.getImage())) {
            throw new RuntimeException(
                String.format(
                    "Unable overwrite %s to %s. Content: %s",
                    imagePathTag, imageSpec.getImage(), content.substring(0, 1000)));
          }
          Files.writeString(imageSpecFile.toPath(), replaced);
        }
      }
    }

    if (imageSpecFile != null) {
      gcsCopy(imageSpecFile.getAbsolutePath(), templatePath);
    }

    containerStageTracker.setStaged(containerName);

    LOG.info("Flex Template was staged! {}", stageImageOnly ? imageSpec.getImage() : templatePath);
    return templatePath;
  }

  /**
   * Prepares the necessary files for building a YAML-based Flex Template.
   *
   * <p>This method checks the {@link TemplateDefinitions} for a {@code yamlTemplateFile}
   * annotation. If the annotation is present and specifies a file, this method copies that file
   * from {@code src/main/yaml} to the build output directory, renaming it to {@code template.yaml}.
   * This {@code template.yaml} is then expected to be packaged into the template's Docker
   * container.
   *
   * @param definition The template definition containing metadata and annotations.
   * @throws MojoExecutionException if the specified YAML template file does not exist.
   * @throws IOException if an I/O error occurs while copying the file.
   */
  @VisibleForTesting
  void prepareYamlTemplateFiles(TemplateDefinitions definition)
      throws MojoExecutionException, IOException {
    LOG.info("Preparing YAML template.");
    String yamlTemplateFile = definition.getTemplateAnnotation().yamlTemplateFile();
    // If a YAML template file is provided in the annotation, it will be copied to
    // the build directory as "template.yaml" and packaged into the container.
    if (yamlTemplateFile != null && !yamlTemplateFile.isEmpty()) {
      Path source =
          Paths.get(
              project.getBasedir().getAbsolutePath(), "src", "main", "yaml", yamlTemplateFile);

      if (!source.toFile().exists()) {
        throw new MojoExecutionException("YAML template file not found: " + source);
      }
      Path destination = Paths.get(outputClassesDirectory.getAbsolutePath(), "template.yaml");

      LOG.info("Copying " + source + " to " + destination);
      Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
    } else {
      LOG.info("No YAML template file provided for copying to `template.yaml`");
    }
  }

  private void stageFlexJavaTemplate(
      TemplateDefinitions definition,
      BuildPluginManager pluginManager,
      String currentTemplateName,
      String buildProjectId,
      String imagePathTag,
      String metadataFile,
      String templatePath)
      throws MojoExecutionException, IOException, InterruptedException, TemplateException {
    String containerName = definition.getTemplateAnnotation().flexContainerName();
    // check if the image of this template has been staged
    if (containerStageTracker.isStaged(containerName, currentTemplateName)) {
      return;
    }
    String tarFileName =
        String.format("%s/%s/%s.tar", outputDirectory.getPath(), containerName, containerName);
    Plugin plugin =
        plugin(
            "com.google.cloud.tools",
            "jib-maven-plugin",
            null,
            List.of(
                dependency("com.google.cloud.tools", "jib-layer-filter-extension-maven", "0.3.0")));

    // X-lang templates need to have a custom image which builds both python and java.
    String[] flexTemplateBuildCmd;
    if (definition.getTemplateAnnotation().type() == TemplateType.XLANG) {
      String dockerfileContainer = outputClassesDirectory.getPath() + "/" + containerName;
      String dockerfilePath = dockerfileContainer + "/Dockerfile";
      File dockerfile = new File(dockerfilePath);
      if (!dockerfile.exists()) {
        List<String> filesToCopy = List.of(definition.getTemplateAnnotation().filesToCopy());
        if (filesToCopy.isEmpty()) {
          filesToCopy = List.of("requirements.txt");
        } else {
          // Currently not used.
          LOG.warn(
              "filesToCopy is overridden in Template {}. Make sure its flexContainerName is set unique.",
              currentTemplateName);
        }
        List<String> entryPoint = List.of(definition.getTemplateAnnotation().entryPoint());
        if (entryPoint.isEmpty() || (entryPoint.size() == 1 && entryPoint.get(0).isEmpty())) {
          entryPoint = List.of(javaTemplateLauncherEntryPoint);
        } else {
          // entryPoint is used by YAML templates. XLANG Templates always use Java entrypoint
          throw new IllegalArgumentException("Cannot override entrypoint for XLANG template.");
        }
        String xlangCommandSpec = containerStageTracker.getCommandSpecEnv(containerName);

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
      stageXlangUsingDockerfile(imagePathTag, containerName, buildProjectId);
    } else {
      List<Element> elements = new ArrayList<>();

      // Base image to use
      elements.add(element("from", element("image", baseContainerImage)));

      // Target image to stage
      elements.add(element("to", element("image", imagePathTag)));
      elements.add(
          element(
              "container",
              element("appRoot", containerStageTracker.getAppRoot(containerName)),
              // Keep the original entrypoint
              element("entrypoint", "INHERIT"),
              // Point to the command spec
              element(
                  "environment",
                  element(
                      "DATAFLOW_JAVA_COMMAND_SPEC",
                      containerStageTracker.getCommandSpecEnv(containerName)))));
      elements.add(element("outputPaths", element("tar", tarFileName)));

      // Only use shaded JAR and exclude libraries if shade was not disabled
      if (System.getProperty("skipShade") == null
          || System.getProperty("skipShade").equalsIgnoreCase("false")) {
        List<Element> paths = new ArrayList<>();
        for (File commandSpecFile : containerStageTracker.getCommandSpecFile(containerName)) {
          paths.add(
              element(
                  "path",
                  element("from", targetDirectory + "/classes"),
                  element("includes", commandSpecFile.getName()),
                  element("into", "/template/" + containerName + "/resources")));
        }

        elements.add(element("extraDirectories", element("paths", paths.toArray(new Element[0]))));

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
        LOG.info("Using Cloud Build to push image {}", imagePathTag);
        stageFlexTemplateUsingCloudBuild(new File(tarFileName), imagePathTag, buildProjectId);
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
          imagePathTag,
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
      String imagePathTag,
      String metadataFile,
      String containerName,
      String templatePath)
      throws IOException, InterruptedException, TemplateException, MojoExecutionException {

    try {
      prepareYamlTemplateFiles(definition);
      prepareYamlDockerfile(definition, containerName);
    } catch (IOException | InterruptedException | TemplateException e) {
      throw new MojoExecutionException("Error preparing YAML Dockerfile", e);
    }

    LOG.info("Staging YAML image using Dockerfile");
    stageYamlUsingDockerfile(buildProjectId, imagePathTag, containerName);

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
          imagePathTag,
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

  /**
   * Prepares the Dockerfile for a YAML-based Flex Template.
   *
   * <p>This method generates a Dockerfile if one does not already exist in the build output
   * directory. The Dockerfile is configured based on properties from the {@link
   * TemplateDefinitions}, including which files to copy into the container, the container
   * entrypoint, and base images.
   *
   * @param definition The template definition containing metadata and annotations.
   * @param containerName The name of the container, used for creating the Dockerfile path.
   * @throws IOException if an I/O error occurs during file generation.
   * @throws InterruptedException if a thread is interrupted.
   * @throws TemplateException if there is an error processing the template for the Dockerfile.
   */
  @VisibleForTesting
  void prepareYamlDockerfile(TemplateDefinitions definition, String containerName)
      throws IOException, InterruptedException, TemplateException {

    // extract image properties for Dockerfile
    String dockerfilePath = outputClassesDirectory.getPath() + "/" + containerName + "/Dockerfile";
    File dockerfile = new File(dockerfilePath);
    if (!dockerfile.exists()) {
      // Obtain file names to copy to docker container
      List<String> filesToCopy =
          new ArrayList<>(List.of(definition.getTemplateAnnotation().filesToCopy()));
      if (filesToCopy.isEmpty()) {
        filesToCopy.addAll(List.of("main.py", "requirements.txt"));
      }

      String yamlTemplateFile = definition.getTemplateAnnotation().yamlTemplateFile();
      if (yamlTemplateFile != null && !yamlTemplateFile.isEmpty()) {
        filesToCopy.add("template.yaml");
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
              .setEntryPoint(entryPoint);
      // Copy required files to docker
      if (!filesToCopy.isEmpty()) {
        dockerfileBuilder.setFilesToCopy(filesToCopy);
      }

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
      String buildProjectId, String imagePathTag, String yamlTemplateName)
      throws IOException, InterruptedException {

    String submoduleName = project.getBasedir().toPath().getFileName().toString();
    File directory;
    String dockerfile;
    if (submoduleName.equals("python")) {
      // Should be for the yaml-template in the python submodule
      directory = new File(outputClassesDirectory.getAbsolutePath() + "/" + yamlTemplateName);
      dockerfile = "Dockerfile";
    } else {
      // For any yaml template in the yaml submodule
      directory = new File(outputClassesDirectory.getAbsolutePath());
      dockerfile = yamlTemplateName + "/Dockerfile";
    }

    File cloudbuildFile = File.createTempFile("cloudbuild", ".yaml");
    try (FileWriter writer = new FileWriter(cloudbuildFile)) {
      String tarPath = "/workspace/" + yamlTemplateName + ".tar\n";
      writer.write(
          "steps:\n"
              + "- name: gcr.io/kaniko-project/executor\n"
              + "  args:\n"
              + "  - --destination="
              + imagePathTag
              + "\n"
              + "  - --dockerfile="
              + dockerfile
              + "\n"
              + "  - --cache=true\n"
              + "  - --cache-ttl=6h\n"
              + "  - --compressed-caching=false\n"
              + "  - --cache-copy-layers=true\n"
              + "  - --cache-repo="
              + getCacheFolder(imagePathTag)
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
                      + imagePathTag
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
    // Ideally this should raise an exception, but this sometimes return NZE even for successful
    // runs.
    if (retval != 0) {
      validateImageExists(imagePathTag, buildProjectId);
    }
  }

  private void stagePythonUsingDockerfile(
      String buildProjectId, String imagePathTag, String containerName)
      throws IOException, InterruptedException {
    File directory = new File(outputClassesDirectory.getAbsolutePath() + "/" + containerName);

    File cloudbuildFile = File.createTempFile("cloudbuild", ".yaml");
    try (FileWriter writer = new FileWriter(cloudbuildFile)) {
      String tarPath = "/workspace/" + containerName + ".tar\n";
      writer.write(
          "steps:\n"
              + "- name: gcr.io/kaniko-project/executor\n"
              + "  args:\n"
              + "  - --destination="
              + imagePathTag
              + "\n"
              + "  - --cache=true\n"
              + "  - --cache-ttl=6h\n"
              + "  - --compressed-caching=false\n"
              + "  - --cache-copy-layers=true\n"
              + "  - --cache-repo="
              + getCacheFolder(imagePathTag)
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
                      + imagePathTag
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
    // Ideally this should raise an exception, but this sometimes return NZE even for successful
    // runs.
    if (retval != 0) {
      validateImageExists(imagePathTag, buildProjectId);
    }
  }

  /** generate image path (not including tag). */
  static String generateFlexTemplateImagePath(
      String containerName, String projectId, String artifactRegion, String artifactRegistry) {
    String prefix = Strings.isNullOrEmpty(artifactRegion) ? "" : artifactRegion + ".";
    // GCR paths can not contain ":", if the project id has it, it should be converted to "/".
    String projectIdUrl = Strings.isNullOrEmpty(projectId) ? "" : projectId.replace(':', '/');
    return Optional.ofNullable(artifactRegistry)
        .map(
            value ->
                value.endsWith("gcr.io") && !value.contains("pkg.dev")
                    ? value + "/" + projectIdUrl + "/" + containerName
                    : value + "/" + containerName)
        .orElse(prefix + "gcr.io/" + projectIdUrl + "/" + containerName);
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
      File tarFile, String imagePathTag, String buildProjectId)
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
              + imagePathTag
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
    // Ideally this should raise an exception, but this sometimes return NZE even for successful
    // runs.
    if (retval != 0) {
      validateImageExists(imagePathTag, buildProjectId);
    }
  }

  private void stageXlangUsingDockerfile(
      String imagePathTag, String containerName, String buildProjectId)
      throws IOException, InterruptedException {
    String dockerfile = containerName + "/Dockerfile";
    File directory = new File(outputClassesDirectory.getAbsolutePath());

    File cloudbuildFile = File.createTempFile("cloudbuild", ".yaml");
    String tarPath = "/workspace/" + containerName + ".tar\n";
    try (FileWriter writer = new FileWriter(cloudbuildFile)) {
      writer.write(
          "steps:\n"
              + "- name: gcr.io/kaniko-project/executor\n"
              + "  args:\n"
              + "  - --destination="
              + imagePathTag
              + "\n"
              + "  - --dockerfile="
              + dockerfile
              + "\n"
              + "  - --cache=true\n"
              + "  - --cache-ttl=6h\n"
              + "  - --compressed-caching=false\n"
              + "  - --cache-copy-layers=true\n"
              + "  - --cache-repo="
              + getCacheFolder(imagePathTag)
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
                      + imagePathTag
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
    // Ideally this should raise an exception, but this sometimes return NZE even for successful
    // runs.
    if (retval != 0) {
      validateImageExists(imagePathTag, buildProjectId);
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
          Path.of(classesDirectory + "/" + containerName + "/classpath" + targetArtifactPath),
          StandardCopyOption.REPLACE_EXISTING);
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

  private void performVulnerabilityScanAndGenerateUserSBOM(
      String imagePathTag, String buildProjectId, File buildDir, TemplateType imageType)
      throws IOException, InterruptedException {
    LOG.info("Generating user SBOM and Performing security scan for {}...", imagePathTag);

    // Continuous scanning is expensive. Images are built on identical dependencies and only differ
    // by entry point. We only need to check once.
    ImmutablePair<String, TemplateType> uniqueImage =
        ImmutablePair.of(buildDir.getPath(), imageType);
    String maybeScan = "";
    if (!SCANNED_TYPES.contains(uniqueImage)) {
      maybeScan =
          "- name: 'us-docker.pkg.dev/scaevola-builder-integration/release/scanvola/scanvola'\n"
              + "  args:\n"
              + "  - --image="
              + imagePathTag
              + "\n";
      SCANNED_TYPES.add(uniqueImage);
    }

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
              + imagePathTag
              + " --format=spdx-json --output=/workspace/user-sbom.json\n"
              + "- name: 'gcr.io/google.com/cloudsdktool/cloud-sdk'\n"
              + "  entrypoint: gcloud\n"
              + "  args:\n"
              + "  - artifacts\n"
              + "  - sbom\n"
              + "  - load\n"
              + "  - --source=/workspace/user-sbom.json\n"
              + "  - --uri="
              + imagePathTag
              + "\n"
              + maybeScan
              + "options:\n"
              + "  logging: CLOUD_LOGGING_ONLY\n");
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

    int retval = stageProcess.waitFor();
    // Ideally this should raise an exception, but this sometimes return NZE even for successful
    // runs.
    if (retval != 0) {
      validateImageExists(imagePathTag, buildProjectId);
    }
  }

  private static void validateImageExists(String imagePathTag, String buildProjectId)
      throws IOException, InterruptedException {
    LOG.info("Validating that image {} was created...", imagePathTag);

    // Tries to describe the image. If it fails, it means the image does not exist.
    Process validationProcess =
        runCommand(
            new String[] {
              "gcloud",
              "artifacts",
              "docker",
              "images",
              "describe",
              imagePathTag,
              "--project",
              buildProjectId
            },
            null,
            new StringBuilder());

    if (validationProcess.waitFor() != 0) {
      throw new RuntimeException(
          "Image "
              + imagePathTag
              + " was not created properly. Check the build logs for more details.");
    }

    LOG.info("Image {} validated successfully.", imagePathTag);
  }

  /** Tracking staged containers shared by templates. */
  private static class ContainerStageTracker {
    private final Map<String, ContainerCommandSpecs> containers;

    ContainerStageTracker() {
      this.containers = new HashMap<>();
    }

    void addContainer(String containerName, String templateName, File commandSpecFile) {
      if (!containers.containsKey(containerName)) {
        containers.put(containerName, new ContainerCommandSpecs());
      }
      containers.get(containerName).commandSpecFiles.put(templateName, commandSpecFile);
    }

    Collection<File> getCommandSpecFile(String containerName) {
      return containers.get(containerName).commandSpecFiles.values();
    }

    void setStaged(String containerName) {
      if (containers.containsKey(containerName)) {
        containers.get(containerName).staged = true;
      }
    }

    boolean isStaged(String containerName, String templateName) {
      if (!containers.containsKey(containerName)) {
        // not managed by ContainerStageTracker
        return false;
      }
      if (!containers.get(containerName).commandSpecFiles.containsKey(templateName)) {
        throw new IllegalStateException(
            String.format(
                "Template %s's command spec not included in %s", templateName, containerName));
      }
      return containers.get(containerName).staged;
    }

    String getAppRoot(String containerName) {
      return "/template/" + containerName;
    }

    String getCommandSpecEnv(String containerName) {
      return getAppRoot(containerName)
          + "/resources/{SPEC_FILE_TEMPLATE_NAME}-generated-command-spec.json";
    }

    private static class ContainerCommandSpecs {
      Map<String, File> commandSpecFiles;
      boolean staged;

      ContainerCommandSpecs() {
        commandSpecFiles = new HashMap<>();
        staged = false;
      }
    }
  }

  /** A runnable used for generating system SBOM, fetching image digest for retrivial. */
  private static class GenerateSBOMRunnable implements dev.failsafe.function.CheckedRunnable {
    private static final Pattern IMAGE_WITH_SBOM_DIGEST =
        Pattern.compile("@(?<DIGEST>sha256:[0-9a-f]{64})");
    private String digest;
    private final String imagePathTag;

    public String getDigest() {
      return digest;
    }

    public GenerateSBOMRunnable(String imagePathTag) {
      this.imagePathTag = imagePathTag;
    }

    @Override
    public void run() throws Throwable {
      LOG.info("Generating system SBOM for {}...", imagePathTag);
      String output;
      try {
        output =
            runCommandCapturesOutput(
                new String[] {"gcloud", "artifacts", "sbom", "export", "--uri", imagePathTag},
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

  private static String getCacheFolder(String imagePathTag) {
    LocalDate today = LocalDate.now();
    // cache dir moves weekly
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("YYYY-ww").withLocale(Locale.ROOT);
    String yearWeek = today.format(formatter);
    return imagePathTag.substring(0, imagePathTag.lastIndexOf('/')) + "/cache/" + yearWeek;
  }
}
