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

import static com.google.cloud.teleport.metadata.util.MetadataUtils.bucketNameOnly;

import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.plugin.model.ImageSpecMetadata;
import com.google.cloud.teleport.plugin.model.json.TemplatesCategoryJsonModel;
import com.google.cloud.teleport.plugin.model.json.TemplatesCategoryJsonModel.TemplatesCategoryJson;
import com.google.cloud.teleport.plugin.model.json.TemplatesCategoryJsonModel.TemplatesCategoryJsonTemplate;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Goal which runs after release targets, to create final artifacts such as categories.json. */
@Mojo(
    name = "release-finish",
    defaultPhase = LifecyclePhase.VERIFY,
    requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class TemplatesReleaseFinishMojo extends TemplatesBaseMojo {

  private static final Logger LOG = LoggerFactory.getLogger(TemplatesReleaseFinishMojo.class);

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
      name = "baseContainerImage",
      defaultValue =
          "gcr.io/dataflow-templates-base/java11-template-launcher-base-distroless:latest",
      required = false)
  protected String baseContainerImage;

  public void execute() {

    if (librariesBucketName == null || librariesBucketName.isEmpty()) {
      librariesBucketName = bucketName;
    }

    File projectFolder = project.getParentFile();
    if (projectFolder == null) {
      projectFolder = new File(System.getProperty("user.dir"));
      LOG.warn("Project folder is null. Using current directory instead.");
    }
    while (projectFolder.getParentFile() != null
        && new File(projectFolder.getParentFile(), "pom.xml").exists()) {
      projectFolder = projectFolder.getParentFile();
    }

    LOG.info("Wrapping up release. Folder to look: {}", projectFolder);

    final PathMatcher pathMatcher =
        FileSystems.getDefault().getPathMatcher("glob:**/target/*-generated-metadata.json");
    final PathMatcher specPathMatcher =
        FileSystems.getDefault().getPathMatcher("glob:**/target/*-spec-generated-metadata.json");
    final Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();

    // Using LinkedHashMap to preserve ordering
    Map<String, TemplatesCategoryJson> categories = new LinkedHashMap<>();
    for (TemplateCategory category : TemplateCategory.ORDERED) {
      categories.put(
          category.getName(),
          new TemplatesCategoryJson(category.getName(), category.getDisplayName()));
    }

    try {
      Set<String> templateNames = new HashSet<>();

      Files.walkFileTree(
          projectFolder.toPath(),
          new SimpleFileVisitor<>() {

            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs)
                throws FileNotFoundException {
              // Match metadata but not spec metadata, as the latter is only for Flex
              if (!pathMatcher.matches(path) || specPathMatcher.matches(path)) {
                return FileVisitResult.CONTINUE;
              }

              ImageSpecMetadata imageSpec =
                  gson.fromJson(new FileReader(path.toFile()), ImageSpecMetadata.class);

              // If the template is hidden, or same template is scanned twice in the classpath
              // (which happens for submodules), just log and ignore.
              if (imageSpec.isHidden() || !templateNames.add(imageSpec.getInternalName())) {
                LOG.info(
                    "Skipping template {} from spec file: {}", imageSpec.getInternalName(), path);
                return FileVisitResult.CONTINUE;
              }

              LOG.info(
                  "Processing metadata Spec File: {}, template: {}", path, imageSpec.getName());

              TemplatesCategoryJsonTemplate template = new TemplatesCategoryJsonTemplate();
              template.setName(imageSpec.getInternalName());
              template.setDisplayName(imageSpec.getName());
              template.setTemplateType(imageSpec.isFlexTemplate() ? "FLEX" : "LEGACY");

              TemplatesCategoryJson categoryJson =
                  categories.get(imageSpec.getCategory().getName());
              if (categoryJson == null) {
                throw new IllegalArgumentException(
                    "Category " + imageSpec.getCategory().getName() + " is invalid.");
              }
              categoryJson.getTemplates().add(template);

              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
              return FileVisitResult.CONTINUE;
            }
          });

      synchronized (TemplatesReleaseFinishMojo.class) {
        final TemplatesCategoryJsonModel categoryJsonModel = new TemplatesCategoryJsonModel();
        categoryJsonModel.setCategories(categories.values());

        // Sort all templates by name
        for (TemplatesCategoryJson categoryJson : categories.values()) {
          categoryJson
              .getTemplates()
              .sort(Comparator.comparing(TemplatesCategoryJsonTemplate::getDisplayName));
        }

        File categoriesFile = new File(targetDirectory, "categories.json");
        FileWriter out = new FileWriter(categoriesFile);
        out.write(gson.toJson(categoryJsonModel));
        out.close();

        String categoriesPath =
            "gs://"
                + bucketNameOnly(bucketName)
                + "/"
                + stagePrefix
                + "/ui_metadata/categories.json";

        String[] copyCmd =
            new String[] {
              "gcloud",
              "storage",
              "cp",
              categoriesFile.getAbsolutePath(),
              categoriesPath,
              "--project",
              projectId,
            };
        LOG.info("Running: {}", String.join(" ", copyCmd));

        Process process = Runtime.getRuntime().exec(copyCmd);

        if (process.waitFor() != 0) {
          throw new RuntimeException(
              "Error copying metadata using 'gcloud storage'. Please make sure it is up to date. "
                  + new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8)
                  + "\n"
                  + new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8));
        }

        // Write the index with all templates
        System.out.println("---- List of Templates for README.md ----");

        for (TemplatesCategoryJson categoryJson : categories.values()) {
          System.out.println("- " + categoryJson.getCategory().getDisplayName());
          for (TemplatesCategoryJsonTemplate template : categoryJson.getTemplates()) {
            System.out.println(
                "  - ["
                    + template.getDisplayName()
                    + "](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20"
                    + template.getName()
                    + "&type=code)");
          }
        }
      }

    } catch (IOException e) {
      throw new RuntimeException("Error looking for metadata files", e);
    } catch (InterruptedException e) {
      throw new RuntimeException("Error creating categories file", e);
    }
  }
}
