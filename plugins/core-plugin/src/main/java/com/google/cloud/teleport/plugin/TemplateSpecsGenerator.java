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
package com.google.cloud.teleport.plugin;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.Template.TemplateType;
import com.google.cloud.teleport.plugin.docs.TemplateDocsGenerator;
import com.google.cloud.teleport.plugin.model.ImageSpec;
import com.google.cloud.teleport.plugin.model.ImageSpecMetadata;
import com.google.cloud.teleport.plugin.model.TemplateDefinitions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;

/** Utility class that generates the specs in a specific/given target path. */
public class TemplateSpecsGenerator {

  private static final Logger LOG = Logger.getLogger(TemplateSpecsGenerator.class.getName());

  /** Gson helper to pretty-print metadata/specs. */
  private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

  /**
   * Scan the classloader for all Template classes, and then builds spec + saves the metadata for
   * every Template.
   */
  public void generateSpecs(ClassLoader classLoader, File baseDirectory, File targetDirectory) {

    List<TemplateDefinitions> templateDefinitions =
        TemplateDefinitionsParser.scanDefinitions(classLoader);
    for (TemplateDefinitions definition : templateDefinitions) {
      LOG.info("Generating template " + definition.getTemplateAnnotation().name() + "...");

      ImageSpec imageSpec = definition.buildSpecModel(false);
      saveMetadata(definition, imageSpec.getMetadata(), targetDirectory);

      if (definition.isFlex()) {
        saveImageSpec(definition, imageSpec, targetDirectory);
      }

      saveDocs(imageSpec, baseDirectory);
    }
  }

  /** Saves the image spec object to a JSON file. */
  public File saveImageSpec(
      TemplateDefinitions definition, ImageSpec imageSpec, File targetDirectory) {

    Template templateAnnotation = definition.getTemplateAnnotation();
    String templateDash = getTemplateNameDash(templateAnnotation.name());

    if (!targetDirectory.exists()) {
      targetDirectory.mkdirs();
    }

    File file =
        new File(targetDirectory, templateDash.toLowerCase() + "-spec-generated-metadata.json");
    LOG.info("Saving image spec " + file.getAbsolutePath());

    // The serialized image spec should match com.google.api.services.dataflow.model.ContainerSpec
    // model, ImageSpec contains some extra fields, so we'll pick up only the expected ones.

    ImageSpec is = new ImageSpec();
    is.setImage(imageSpec.getImage());
    is.setSdkInfo(imageSpec.getSdkInfo());
    is.setDefaultEnvironment(imageSpec.getDefaultEnvironment());

    ImageSpecMetadata m = new ImageSpecMetadata();
    m.setName(imageSpec.getMetadata().getName());
    m.setDescription(imageSpec.getMetadata().getDescription());
    m.setParameters(imageSpec.getMetadata().getParameters());
    m.setStreaming(imageSpec.getMetadata().isStreaming());
    m.setSupportsAtLeastOnce(imageSpec.getMetadata().isSupportsAtLeastOnce());
    m.setSupportsExactlyOnce(imageSpec.getMetadata().isSupportsExactlyOnce());
    m.setDefaultStreamingMode(imageSpec.getMetadata().getDefaultStreamingMode());
    is.setMetadata(m);

    try (FileWriter writer = new FileWriter(file)) {
      writer.write(gson.toJson(is));
    } catch (IOException e) {
      throw new RuntimeException("Error writing image spec", e);
    }

    return file;
  }

  /** Save the metadata file (definition of parameters for a Template) to a JSON file. */
  public File saveMetadata(
      TemplateDefinitions definition, ImageSpecMetadata imageSpecMetadata, File targetDirectory) {

    Template templateAnnotation = definition.getTemplateAnnotation();
    String templateDash = getTemplateNameDash(templateAnnotation.name());

    if (!targetDirectory.exists()) {
      targetDirectory.mkdirs();
    }

    String imageName = templateDash.toLowerCase();
    if (StringUtils.isNotEmpty(templateAnnotation.flexContainerName())) {
      imageName = templateAnnotation.flexContainerName();
    }

    File file = new File(targetDirectory, imageName + "-generated-metadata.json");
    LOG.info("Saving image spec metadata " + file.getAbsolutePath());

    try (FileWriter writer = new FileWriter(file)) {
      writer.write(gson.toJson(imageSpecMetadata));
    } catch (IOException e) {
      throw new RuntimeException("Error writing image spec metadata", e);
    }

    return file;
  }

  /** Save the Flex Template command spec to a given file. */
  public File saveCommandSpec(TemplateDefinitions definition, File targetDirectory) {

    Template templateAnnotation = definition.getTemplateAnnotation();
    String templateDash = getTemplateNameDash(templateAnnotation.name());

    if (!targetDirectory.exists()) {
      targetDirectory.mkdirs();
    }

    File file =
        new File(targetDirectory, templateDash.toLowerCase() + "-generated-command-spec.json");
    LOG.info("Saving command spec " + file.getAbsolutePath());

    try (FileWriter writer = new FileWriter(file)) {

      String containerName = templateAnnotation.flexContainerName();

      if (definition.getTemplateAnnotation().type() == TemplateType.JAVA
          || definition.getTemplateAnnotation().type() == TemplateType.XLANG) {
        writer.write(
            "{\n"
                + "  \"mainClass\": \""
                + definition.getTemplateClass().getName()
                + "\",\n"
                + "  \"classPath\": \"/template/"
                + containerName
                + "/libs/conscrypt-openjdk-uber.jar:/template/"
                + containerName
                + "/libs/*:/template/"
                + containerName
                + "/classes:/template/"
                + containerName
                + "/classpath/*:/template/"
                + containerName
                + "/resources\",\n"
                + "  \"defaultParameterValues\": {\n"
                + "    \"labels\": \"{\\\"goog-dataflow-provided-template-type\\\":\\\"flex\\\","
                + " \\\"goog-dataflow-provided-template-name\\\":\\\""
                + containerName.toLowerCase()
                + "\\\"}\"\n"
                + "  }\n"
                + "}\n");
      } else {
        writer.write(
            "{\n"
                + "  \"pyFile\": \"/template/"
                + containerName
                + "/main.py\",\n"
                + "  \"defaultParameterValues\": {\n"
                + "    \"labels\": \"{\\\"goog-dataflow-provided-template-type\\\":\\\"flex\\\","
                + " \\\"goog-dataflow-provided-template-name\\\":\\\""
                + containerName.toLowerCase()
                + "\\\"}\"\n"
                + "  }\n"
                + "}\n");
      }
    } catch (IOException e) {
      throw new RuntimeException("Error writing command spec", e);
    }

    return file;
  }

  private void saveDocs(ImageSpec imageSpec, File targetDirectory) {

    // Find the project root folder
    File projectRoot = targetDirectory;
    while (projectRoot.getParentFile() != null
        && new File(projectRoot.getParentFile(), "pom.xml").exists()) {
      projectRoot = projectRoot.getParentFile();
    }

    // Construct the source file path, to be used in the Open in Cloud Shell button
    File javaFile =
        new File(
            targetDirectory,
            "src/main/java/" + imageSpec.getMetadata().getMainClass().replace('.', '/') + ".java");
    String prefixReplace = projectRoot.getAbsolutePath();
    if (!prefixReplace.endsWith("/")) {
      prefixReplace += "/";
    }

    imageSpec
        .getMetadata()
        .setSourceFilePath(javaFile.getAbsolutePath().replace(prefixReplace, ""));

    generateReadmeMarkdown(imageSpec, targetDirectory);

    File siteFolder = new File(new File(projectRoot, "target"), "site");
    siteFolder.mkdirs();
    generateSiteMarkdown(imageSpec, siteFolder);
  }

  private static File generateReadmeMarkdown(ImageSpec imageSpec, File targetDirectory) {
    File file =
        new File(targetDirectory, "README_" + imageSpec.getMetadata().getInternalName() + ".md");
    LOG.info("Creating docs: " + file.getAbsolutePath());

    try {
      String markdown = TemplateDocsGenerator.readmeMarkdown(imageSpec);
      try (FileWriter out = new FileWriter(file)) {
        out.write(markdown);
      }
    } catch (Exception e) {
      LOG.warning("Error generating markdown docs");
    }
    return file;
  }

  private static File generateSiteMarkdown(ImageSpec imageSpec, File targetDirectory) {
    String siteFileName;
    String docLink = imageSpec.getMetadata().getDocumentationLink();
    if (StringUtils.isNotEmpty(docLink) && docLink.contains("cloud.google.com")) {
      String[] documentationParts = docLink.split("/");
      siteFileName = documentationParts[documentationParts.length - 1];
    } else {
      siteFileName = getTemplateNameDash(imageSpec.getMetadata().getInternalName());
    }
    File file = new File(targetDirectory, siteFileName + ".md");
    LOG.info("Creating site: " + file.getAbsolutePath());

    try {
      String markdown = TemplateDocsGenerator.siteMarkdown(imageSpec);
      try (FileWriter out = new FileWriter(file)) {
        out.write(markdown);
      }
    } catch (Exception e) {
      LOG.warning("Error generating site markdown");
    }
    return file;
  }

  public static String getTemplateNameDash(String templateName) {
    return templateName.replace(' ', '-').replace('_', '-').toLowerCase();
  }
}
