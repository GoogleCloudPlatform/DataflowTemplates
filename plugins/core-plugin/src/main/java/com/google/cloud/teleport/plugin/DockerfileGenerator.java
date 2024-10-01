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

import com.google.cloud.teleport.metadata.Template.TemplateType;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Utility class that generates a simple Dockerfile for Dockerfile-based templates. This currently
 * includes Python, Yaml and Xlang templates
 */
public class DockerfileGenerator {

  public static final String BASE_CONTAINER_IMAGE =
      "gcr.io/dataflow-templates-base/java11-template-launcher-base-distroless:latest";
  public static final String BASE_PYTHON_CONTAINER_IMAGE =
      "gcr.io/dataflow-templates-base/python311-template-launcher-base:latest";
  public static final String PYTHON_LAUNCHER_ENTRYPOINT =
      "/opt/google/dataflow/python_template_launcher";
  public static final String JAVA_LAUNCHER_ENTRYPOINT =
      "/opt/google/dataflow/java_template_launcher";

  // Keep pythonVersion below in sync with version in image
  public static final String PYTHON_VERSION = "3.11";

  private static final Logger LOG = Logger.getLogger(DockerfileGenerator.class.getName());

  protected Map<String, Object> parameters;
  protected String containerName;
  protected File targetDirectory;

  protected final TemplateType templateType;

  protected DockerfileGenerator(Builder builder) {
    this.parameters = builder.parameters;
    this.containerName = builder.containerName;
    this.targetDirectory = builder.targetDirectory;
    this.templateType = builder.templateType;
  }

  public static Builder builder(
      TemplateType templateType, String beamVersion, String containerName, File targetDirectory) {
    return new DockerfileGenerator.Builder(
        templateType, beamVersion, containerName, targetDirectory);
  }

  public void generate() throws IOException, TemplateException {
    Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_32);
    freemarkerConfig.setDefaultEncoding("UTF-8");
    freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    freemarkerConfig.setLogTemplateExceptions(true);
    freemarkerConfig.setClassForTemplateLoading(this.getClass(), "/");

    String dockerfileTemplateName;
    switch (templateType) {
      case XLANG:
        dockerfileTemplateName = "Dockerfile-template-xlang";
        break;
      case PYTHON:
        dockerfileTemplateName = "Dockerfile-template-python";
        break;
      case YAML:
        dockerfileTemplateName = "Dockerfile-template-yaml";
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("type %s is not supported.", templateType));
    }
    Template template = freemarkerConfig.getTemplate(dockerfileTemplateName);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStreamWriter writer = new OutputStreamWriter(baos);

    try {
      template.process(parameters, writer);
      writer.flush();

      Files.createDirectories(Path.of(targetDirectory.getPath() + "/" + containerName));
      Files.write(
          Path.of(targetDirectory.getPath() + "/" + containerName + "/Dockerfile"),
          baos.toString(StandardCharsets.UTF_8).getBytes());
    } catch (Exception e) {
      LOG.warning("Unable to generate Dockerfile for " + containerName);
      throw e;
    }
  }

  public static class Builder {

    private final TemplateType templateType;
    private final String containerName;
    private final File targetDirectory;
    private final Map<String, Object> parameters;

    public Builder(
        TemplateType templateType, String beamVersion, String containerName, File targetDirectory) {
      this.templateType = templateType;
      this.containerName = containerName;
      this.targetDirectory = targetDirectory;

      this.parameters = new HashMap<>();
      this.parameters.put("beamVersion", beamVersion);

      this.parameters.put("basePythonContainerImage", BASE_PYTHON_CONTAINER_IMAGE);
      this.parameters.put("baseJavaContainerImage", BASE_CONTAINER_IMAGE);
      this.parameters.put("pythonVersion", PYTHON_VERSION);
      this.parameters.put(
          "entryPoint",
          "[\""
              + (templateType.equals(TemplateType.XLANG)
                  ? JAVA_LAUNCHER_ENTRYPOINT
                  : PYTHON_LAUNCHER_ENTRYPOINT)
              + "\"]");

      this.parameters.put("filesToCopy", "");
      this.parameters.put("directoriesToCopy", "");
      this.parameters.put("commandSpec", "");
    }

    public Builder setBasePythonContainerImage(String basePythonContainerImage) {
      this.parameters.put("basePythonContainerImage", basePythonContainerImage);
      return this;
    }

    public Builder setBaseJavaContainerImage(String baseJavaContainerImage) {
      this.parameters.put("baseJavaContainerImage", baseJavaContainerImage);
      return this;
    }

    public Builder setPythonVersion(String pythonVersion) {
      this.parameters.put("pythonVersion", pythonVersion);
      return this;
    }

    public Builder setEntryPoint(String entryPoint) {
      return setEntryPoint(new String[] {entryPoint});
    }

    public Builder setEntryPoint(String[] entryPoint) {
      List<String> entries = new ArrayList<>();
      for (String cmd : entryPoint) {
        entries.add(String.format("\"%s\"", cmd));
      }
      this.parameters.put("entryPoint", "[" + String.join(", ", entries) + "]");
      return this;
    }

    public Builder setFilesToCopy(Map<String, Set<String>> filesToCopy) {
      StringBuilder files = new StringBuilder();
      for (String file : filesToCopy.keySet()) {
        files.append(String.format("COPY %s", file));
        if (!filesToCopy.get(file).isEmpty()) {
          files.append(String.format(" %s", String.join(" ", filesToCopy.get(file))));
        }
        files.append(" /$WORKDIR/\n");
      }
      this.parameters.put("filesToCopy", files);
      return this;
    }

    public Builder setDirectoriesToCopy(Set<String> directoriesToCopy) {
      StringBuilder directories = new StringBuilder();
      for (String directory : directoriesToCopy) {
        directory = directory.replaceAll("/*$", "");
        directories.append(String.format("COPY %s/ /$WORKDIR/%s/\n", directory, directory));
      }
      this.parameters.put("directoriesToCopy", directories);
      return this;
    }

    public Builder setCommandSpec(String commandSpec) {
      this.parameters.put("commandSpec", commandSpec);
      return this;
    }

    public Builder addParameter(String parameter, Object value) {
      this.parameters.put(parameter, value);
      return this;
    }

    public DockerfileGenerator build() {
      return new DockerfileGenerator(this);
    }
  }
}
