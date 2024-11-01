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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
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
  // Keep in sync with python version used in
  // https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/python/generate_dependencies.sh
  public static final String BASE_PYTHON_CONTAINER_IMAGE =
      "gcr.io/dataflow-templates-base/python311-template-launcher-base:latest";
  public static final String PYTHON_LAUNCHER_ENTRYPOINT =
      "/opt/google/dataflow/python_template_launcher";
  public static final String JAVA_LAUNCHER_ENTRYPOINT =
      "/opt/google/dataflow/java_template_launcher";

  // Keep pythonVersion below in sync with version in base image
  public static final String PYTHON_VERSION = "3.11";
  public static final String DEFAULT_WORKING_DIRECTORY = "/template";

  private static final String SA_SECRET_NAME_KEY = "saSecretName";
  private static final String AIRLOCK_PYTHON_REPO_KEY = "airlockPythonRepo";

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

  /**
   * Create a {@link Builder} instance for {@link DockerfileGenerator}.
   *
   * @param templateType A {@link TemplateType}.
   * @param beamVersion The Beam version to use in the Dockerfile.
   * @param containerName The container name for the Template.
   * @param targetDirectory The directory to write the Dockerfile to.
   * @return the {@link Builder} instance.
   */
  public static Builder builder(
      TemplateType templateType, String beamVersion, String containerName, File targetDirectory) {
    return new DockerfileGenerator.Builder(
        templateType, beamVersion, containerName, targetDirectory);
  }

  /**
   * Generate the Dockerfile.
   *
   * @throws IOException if there is an error writing the Dockerfile to the file system.
   * @throws TemplateException if there is an error constructing the final Dockerfile from the
   *     Dockerfile template.
   */
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

    configureAirlock(parameters);

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

  private static void configureAirlock(Map<String, Object> parameters) {
    String baseConfig = "";
    if (parameters.containsKey(SA_SECRET_NAME_KEY)
        && parameters.containsKey(AIRLOCK_PYTHON_REPO_KEY)) {
      baseConfig =
          "RUN mkdir -p $HOME/.pip\n"
              + "RUN mkdir -p $HOME/.config/pip"
              + "\n"
              + "RUN printf \""
              + "[distutils]\\n"
              + "index-servers =\\n"
              + "    AIRLOCK_PYTHON_REPO_KEY"
              + "\\n\\n"
              + "[AIRLOCK_PYTHON_REPO_KEY]\\n"
              + "repository: https://us-python.pkg.dev/artifact-foundry-prod/AIRLOCK_PYTHON_REPO_KEY/\\n"
              + "username: _json_key_base64\\n"
              + "password: $(gcloud secrets versions access latest --secret=SA_SECRET_NAME_KEY | base64 -w 0)\""
              + " > $HOME/.pip/.pypirc"
              + "\n"
              + "RUN printf \""
              + "[global]\\n"
              + "index-url = "
              + "https://_json_key_base64:"
              + "$(gcloud secrets versions access latest --secret=SA_SECRET_NAME_KEY | base64 -w 0)"
              + "@us-python.pkg.dev/artifact-foundry-prod/AIRLOCK_PYTHON_REPO_KEY/simple/\""
              + " > $HOME/.config/pip/pip.conf";
      baseConfig =
          baseConfig
              .replaceAll(
                  "AIRLOCK_PYTHON_REPO_KEY", parameters.get(AIRLOCK_PYTHON_REPO_KEY).toString())
              .replaceAll("SA_SECRET_NAME_KEY", parameters.get(SA_SECRET_NAME_KEY).toString());
    }
    parameters.put("airlockConfig", baseConfig);
  }

  /** Builder for {@link DockerfileGenerator}. */
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
      this.parameters.put("workingDirectory", DEFAULT_WORKING_DIRECTORY);
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

    /**
     * Sets the Base python image to use for the Docker image. This image is used as the python
     * builder for installing packages. To set the Base java image used as the base for the output
     * image, see {@link Builder#setBaseJavaContainerImage(String)}.
     *
     * <p>Default is {@value DockerfileGenerator#BASE_PYTHON_CONTAINER_IMAGE}
     *
     * @param basePythonContainerImage Base python image to use.
     * @return this {@link Builder}.
     * @throws IllegalArgumentException if basePythonContainerImage is null or empty.
     */
    public Builder setBasePythonContainerImage(String basePythonContainerImage) {
      return addStringParameter("basePythonContainerImage", basePythonContainerImage);
    }

    /**
     * Sets the Base java image to use for the Docker image. This image is used as the base image
     * for the output image. To set the Base python builder image used for installing packages, see
     * {@link Builder#setBasePythonContainerImage(String)}.
     *
     * <p>Default is {@value DockerfileGenerator#BASE_CONTAINER_IMAGE}
     *
     * @param baseJavaContainerImage Base java image to use.
     * @return this {@link Builder}.
     * @throws IllegalArgumentException if baseJavaContainerImage is null or empty.
     */
    public Builder setBaseJavaContainerImage(String baseJavaContainerImage) {
      return addStringParameter("baseJavaContainerImage", baseJavaContainerImage);
    }

    /**
     * Set the python version to use. This should correspond with the python version installed in
     * the base python image set by {@link Builder#setBasePythonContainerImage(String)}.
     *
     * <p>Default is version {@value DockerfileGenerator#PYTHON_VERSION}
     *
     * @param pythonVersion Python version to use.
     * @return this {@link Builder}.
     * @throws IllegalArgumentException if pythonVersion is null or empty.
     */
    public Builder setPythonVersion(String pythonVersion) {
      return addStringParameter("pythonVersion", pythonVersion);
    }

    /**
     * Set the Entrypoint override for the image. For example, an entryPoint of {"some_command",
     * "some_arg"} would format as:
     *
     * <p>ENTRYPOINT ["some_command", "some_arg"]
     *
     * <p>Defaults:
     *
     * <ul>
     *   <li>PYTHON/YAML: {@value DockerfileGenerator#PYTHON_LAUNCHER_ENTRYPOINT}
     *   <li>XLANG: {@value DockerfileGenerator#JAVA_LAUNCHER_ENTRYPOINT}
     * </ul>
     *
     * @param entryPoint entryPoint to use as override for Dockerfile.
     * @return this {@link Builder}.
     * @throws IllegalArgumentException if entryPoint is null or empty.
     */
    public Builder setEntryPoint(List<String> entryPoint) {
      Preconditions.checkNotNull(entryPoint);
      Preconditions.checkArgument(!entryPoint.isEmpty());

      List<String> entries = new ArrayList<>();
      for (String cmd : entryPoint) {
        entries.add(String.format("\"%s\"", cmd));
      }

      return addStringParameter("entryPoint", "[" + String.join(", ", entries) + "]");
    }

    /**
     * Sets the list of files to copy into the image's working directory (Default to {@value
     * DockerfileGenerator#DEFAULT_WORKING_DIRECTORY}).
     *
     * <p>To change the working directory, see {@link Builder#setWorkingDirectory(String)}.
     *
     * <p><b>Note</b>: Make sure when using wildcards, to specify an existing file first.
     *
     * <p>For example, if copying in a file, main.py, and an optional requirements.txt, pass the
     * list {@code {"main.py", "requirements.txt"} }.
     *
     * @param filesToCopy list of files to copy into image.
     * @return this {@link Builder}.
     * @throws IllegalArgumentException if filesToCopy is null or empty.
     */
    public Builder setFilesToCopy(List<String> filesToCopy) {
      Preconditions.checkNotNull(filesToCopy);
      Preconditions.checkArgument(!filesToCopy.isEmpty());

      StringBuilder files = new StringBuilder("COPY");
      for (String file : filesToCopy) {
        files.append(String.format(" %s", file));
      }
      files.append(" $WORKDIR/\n");

      return addStringParameter("filesToCopy", files.toString());
    }

    /**
     * Sets the list of directories to copy into the image's working directory (Default to {@value
     * DockerfileGenerator#DEFAULT_WORKING_DIRECTORY}).
     *
     * <p>To change the working directory, see {@link Builder#setWorkingDirectory(String)}.
     *
     * @param directoriesToCopy list of directories to copy into image.
     * @return this {@link Builder}.
     * @throws IllegalArgumentException if directoriesToCopy is null or empty.
     */
    public Builder setDirectoriesToCopy(Set<String> directoriesToCopy) {
      Preconditions.checkNotNull(directoriesToCopy);
      Preconditions.checkArgument(!directoriesToCopy.isEmpty());

      StringBuilder directories = new StringBuilder();
      for (String directory : directoriesToCopy) {
        directory = directory.replaceAll("/*$", "");
        directories.append(String.format("COPY %s/ $WORKDIR/%s/\n", directory, directory));
      }

      return addStringParameter("directoriesToCopy", directories.toString());
    }

    /**
     * For XLANG templates, set the {@code DATAFLOW_JAVA_COMMAND_SPEC} env variable to the command
     * spec location on the image.
     *
     * @param commandSpec location of command spec file on image.
     * @return this {@link Builder}.
     * @throws IllegalArgumentException if commandSpec is null or empty.
     */
    public Builder setCommandSpec(String commandSpec) {
      return addStringParameter("commandSpec", commandSpec);
    }

    /**
     * Sets the working directory for the image.
     *
     * <p>Defaults to {@value DockerfileGenerator#DEFAULT_WORKING_DIRECTORY}.
     *
     * @param workingDirectory path to use for working directory.
     * @return this {@link Builder}.
     * @throws IllegalArgumentException if workingDirectory is null or empty.
     */
    public Builder setWorkingDirectory(String workingDirectory) {
      return addStringParameter("workingDirectory", workingDirectory);
    }

    /**
     * Set a custom String parameter in the Dockerfile template.
     *
     * @param parameter name of parameter to set.
     * @param value to use for the parameter.
     * @return this {@link Builder}.
     * @throws IllegalArgumentException if the parameter name or value are null or empty.
     */
    public Builder addStringParameter(String parameter, String value) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(value));

      return addParameter(parameter, value);
    }

    public Builder setAirlockPythonRepo(String airlockPythonRepo) {
      this.parameters.put(AIRLOCK_PYTHON_REPO_KEY, airlockPythonRepo);
      return this;
    }

    public Builder setAirlockJavaRepo(String airlockJavaRepo) {
      this.parameters.put("airlockJavaRepo", airlockJavaRepo);
      return this;
    }

    public Builder setServiceAccountSecretName(String secretName) {
      this.parameters.put(SA_SECRET_NAME_KEY, secretName);
      return this;
    }

    /**
     * Set a custom parameter in the Dockerfile template.
     *
     * @param parameter name of parameter to set.
     * @param value to use for the parameter.
     * @return this {@link Builder}.
     * @throws IllegalArgumentException if the parameter name is null or empty, or if the value is
     *     null.
     */
    public Builder addParameter(String parameter, Object value) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(parameter));
      Preconditions.checkNotNull(value);

      this.parameters.put(parameter, value);
      return this;
    }

    public DockerfileGenerator build() {
      return new DockerfileGenerator(this);
    }
  }
}
