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
        saveDocs(definition, imageSpec, baseDirectory);
      }
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

    try (FileWriter writer = new FileWriter(file)) {
      writer.write(gson.toJson(imageSpec));
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
      writer.write(
          "{\n"
              + "  \"mainClass\": \""
              + definition.getTemplateClass().getName()
              + "\",\n"
              + "  \"classPath\": \"/template/"
              + templateAnnotation.flexContainerName()
              + "/*:/template/"
              + templateAnnotation.flexContainerName()
              + "/libs/conscrypt-openjdk-uber-*.jar:/template/"
              + templateAnnotation.flexContainerName()
              + "/libs/*:/template/"
              + templateAnnotation.flexContainerName()
              + "/classes:/template/"
              + templateAnnotation.flexContainerName()
              + "/resources\",\n"
              + "  \"defaultParameterValues\": {\n"
              + "    \"labels\": \"{\\\"goog-dataflow-provided-template-type\\\":\\\"flex\\\","
              + " \\\"goog-dataflow-provided-template-name\\\":\\\""
              + templateAnnotation.flexContainerName().toLowerCase()
              + "\\\"}\"\n"
              + "  }\n"
              + "}\n");
    } catch (IOException e) {
      throw new RuntimeException("Error writing command spec", e);
    }

    return file;
  }

  private File saveDocs(TemplateDefinitions definition, ImageSpec imageSpec, File targetDirectory) {

    Template templateAnnotation = definition.getTemplateAnnotation();

    File file = new File(targetDirectory, "README_" + templateAnnotation.name() + ".md");
    LOG.info("Saving docs " + file.getAbsolutePath());

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

  public String getTemplateNameDash(String templateName) {
    return templateName.replace(' ', '-').replace('_', '-').toLowerCase();
  }
}
