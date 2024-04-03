/*
 * Copyright (C) 2024 Google LLC
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

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class YamlDockerfileGenerator {

  private static final Logger LOG = Logger.getLogger(YamlDockerfileGenerator.class.getName());

  private YamlDockerfileGenerator() {}

  public static void generateDockerfile(
      String basePythonContainerImage, String yamlTemplateName, File targetDirectory)
      throws IOException, TemplateException {

    Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_32);
    freemarkerConfig.setDefaultEncoding("UTF-8");
    freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    freemarkerConfig.setLogTemplateExceptions(true);
    freemarkerConfig.setClassForTemplateLoading(PythonDockerfileGenerator.class, "/");

    Map<String, Object> parameters = new HashMap<>();
    parameters.put("baseContainerImage", basePythonContainerImage);
    parameters.put("yamlTemplateName", yamlTemplateName + ".yaml");

    Template template = freemarkerConfig.getTemplate("Dockerfile-template-yaml");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStreamWriter writer = new OutputStreamWriter(baos);

    try {
      template.process(parameters, writer);
      writer.flush();

      Files.createDirectories(Path.of(targetDirectory.getPath() + "/" + yamlTemplateName));
      Files.write(
          Path.of(targetDirectory.getPath() + "/" + yamlTemplateName + "/Dockerfile"),
          baos.toString(StandardCharsets.UTF_8).getBytes());
    } catch (Exception e) {
      LOG.warning("Unable to generate Dockerfile for " + yamlTemplateName);
      throw e;
    }
  }
}
