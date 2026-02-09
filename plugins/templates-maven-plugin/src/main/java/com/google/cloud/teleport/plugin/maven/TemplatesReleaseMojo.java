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
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.teleport.plugin.TemplateDefinitionsParser;
import com.google.cloud.teleport.plugin.TemplateSpecsGenerator;
import com.google.cloud.teleport.plugin.model.ImageSpec;
import com.google.cloud.teleport.plugin.model.TemplateDefinitions;
import com.google.gson.Gson;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

  private record Blueprint(String name, String path) {}
  ;

  private static final Gson GSON = new Gson();

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

  @Parameter(defaultValue = "${stagePrefix}", readonly = true, required = false)
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

  @Parameter(defaultValue = "true", property = "generateSBOM", readonly = true, required = false)
  protected boolean generateSBOM;

  @Parameter(
      defaultValue = "true",
      property = "publishYamlBlueprints",
      readonly = true,
      required = false)
  protected boolean publishYamlBlueprints;

  @Parameter(
      defaultValue = "src/main/yaml",
      property = "yamlBlueprintsPath",
      readonly = true,
      required = false)
  protected String yamlBlueprintsPath;

  @Parameter(
      defaultValue = "yaml-blueprints",
      property = "yamlBlueprintsGCSPath",
      readonly = true,
      required = false)
  protected String yamlBlueprintsGCSPath;

  @Parameter(
      defaultValue = "yaml-manifest.json",
      property = "yamlManifestName",
      readonly = true,
      required = false)
  protected String yamlManifestName;

  @Parameter(
      defaultValue = "yaml/src/main/python/options",
      property = "yamlOptionsPath",
      readonly = true,
      required = false)
  protected String yamlOptionsPath;

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
          TemplateDefinitionsParser.scanDefinitions(loader, outputDirectory);

      if (templateName != null && !templateName.isEmpty()) {
        templateDefinitions =
            templateDefinitions.stream()
                .filter(candidate -> candidate.getTemplateAnnotation().name().equals(templateName))
                .collect(Collectors.toList());
      }

      if ((!templateDefinitions.isEmpty() || publishYamlBlueprints)
          && (stagePrefix == null || stagePrefix.isEmpty())) {
        throw new IllegalArgumentException(
            "Stage Prefix must be informed for releases, when releasing templates or yaml blueprints.");
      }

      String useRegion = StringUtils.isNotEmpty(region) ? region : "us-central1";
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
              flexContainerName,
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
              artifactRegistry,
              stagingArtifactRegistry,
              unifiedWorker,
              generateSBOM);
      configuredMojo.stageCommandSpecs(templateDefinitions);

      for (TemplateDefinitions definition : templateDefinitions) {

        ImageSpec imageSpec = definition.buildSpecModel(true);
        String currentTemplateName = imageSpec.getMetadata().getName();

        LOG.info("Staging template {}...", currentTemplateName);

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

      if (publishYamlBlueprints) {
        LOG.info(
            "Trying to upload Job Builder blueprints to bucket '{}'...",
            bucketNameOnly(bucketName));
        Path yamlPath = Paths.get(project.getBasedir().getAbsolutePath(), yamlBlueprintsPath);
        Path yamlOptionsPath =
            Paths.get(project.getBasedir().getAbsolutePath(), this.yamlOptionsPath);

        if ((!Files.exists(yamlPath) || !Files.isDirectory(yamlPath))
            && (!Files.exists(yamlOptionsPath) || !Files.isDirectory(yamlOptionsPath))) {
          LOG.warn(
              "YAML blueprints and options directory not found, skipping upload for paths: {} and {}",
              yamlPath,
              yamlOptionsPath);
        } else {

          try (Storage storage = StorageOptions.getDefaultInstance().getService()) {
            List<Blueprint> blueprints = new ArrayList<>();
            List<Blueprint> options = new ArrayList<>();

            // Upload the main Yaml blueprints
            uploadYamlFiles(storage, yamlPath, "", blueprints);

            // Upload the jinja parameter option files
            uploadYamlFiles(storage, yamlOptionsPath, "options", options);

            // Build the manifest file
            String manifestObjectName =
                String.join("/", stagePrefix, yamlBlueprintsGCSPath, yamlManifestName);
            BlobId manifestBlobId = BlobId.of(bucketNameOnly(bucketName), manifestObjectName);
            BlobInfo manifestBlobInfo = BlobInfo.newBuilder(manifestBlobId).build();

            // Upload the manifest file with retries
            Map<String, List<Blueprint>> manifestMap = new HashMap<>();
            manifestMap.put("blueprints", blueprints);
            manifestMap.put("options", options);
            byte[] manifestBytes = GSON.toJson(manifestMap).getBytes(StandardCharsets.UTF_8);
            Failsafe.with(gcsRetryPolicy())
                .run(
                    () ->
                        storage.create(manifestBlobInfo, new ByteArrayInputStream(manifestBytes)));
            LOG.info(
                "Uploaded {} to gs://{}/{}",
                yamlManifestName,
                bucketNameOnly(bucketName),
                manifestObjectName);
          } catch (Exception e) {
            throw new MojoExecutionException("Error uploading YAML blueprints", e);
          }
        }
      } else {
        LOG.warn("YAML blueprints not published in this module.");
      }

    } catch (DependencyResolutionRequiredException e) {
      throw new MojoExecutionException("Dependency resolution failed", e);
    } catch (MalformedURLException e) {
      throw new MojoExecutionException("URL generation failed", e);
    } catch (InvalidArgumentException e) {
      throw new MojoExecutionException("Invalid run argument", e);
    } catch (MojoExecutionException e) {
      throw e;
    } catch (Exception e) {
      throw new MojoExecutionException("Template run failed", e);
    }
  }

  private void uploadYamlFiles(
      Storage storage, Path directory, String subFolder, List<Blueprint> blueprints)
      throws IOException {
    if (Files.exists(directory) && Files.isDirectory(directory)) {
      try (Stream<Path> paths = Files.list(directory)) {
        paths
            .filter(
                path ->
                    Files.isRegularFile(path) && path.getFileName().toString().endsWith(".yaml"))
            .forEach(
                path -> {
                  String fileName = path.getFileName().toString();
                  String objectName =
                      subFolder.isEmpty()
                          ? String.join("/", stagePrefix, yamlBlueprintsGCSPath, fileName)
                          : String.join(
                              "/", stagePrefix, yamlBlueprintsGCSPath, subFolder, fileName);
                  uploadToGcs(storage, path, objectName);
                  blueprints.add(new Blueprint(fileName, objectName));
                });
      }
    }
  }

  private void uploadToGcs(Storage storage, Path path, String objectName) {
    BlobId blobId = BlobId.of(bucketNameOnly(bucketName), objectName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

    // Upload every blueprint with retries
    Failsafe.with(gcsRetryPolicy())
        .run(
            () -> {
              try (InputStream inputStream = Files.newInputStream(path)) {
                storage.create(blobInfo, inputStream);
              }
            });

    LOG.info(
        "Uploaded file {} to gs://{}/{}",
        path.getFileName().toString(),
        bucketNameOnly(bucketName),
        objectName);
  }

  private static <T> RetryPolicy<T> gcsRetryPolicy() {
    return RetryPolicy.<T>builder()
        .handle(IOException.class)
        .withBackoff(Duration.ofSeconds(2), Duration.ofSeconds(30))
        .withMaxRetries(3)
        .build();
  }
}
