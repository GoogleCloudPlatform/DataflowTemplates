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
package com.google.cloud.teleport.plugin;

import static com.google.cloud.teleport.plugin.DockerfileGenerator.BASE_CONTAINER_IMAGE;
import static com.google.cloud.teleport.plugin.DockerfileGenerator.BASE_PYTHON_CONTAINER_IMAGE;
import static com.google.cloud.teleport.plugin.DockerfileGenerator.JAVA_LAUNCHER_ENTRYPOINT;
import static com.google.cloud.teleport.plugin.DockerfileGenerator.PYTHON_LAUNCHER_ENTRYPOINT;
import static com.google.cloud.teleport.plugin.DockerfileGenerator.PYTHON_VERSION;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.metadata.Template;
import com.google.common.io.Files;
import freemarker.template.TemplateException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for class {@link DockerfileGenerator}. */
@RunWith(JUnit4.class)
public class DockerfileGeneratorTest {

  private static final String containerName = "word-count";
  private final File outputFolder = Files.createTempDir().getAbsoluteFile();

  public DockerfileGenerator.Builder createDockerfileGeneratorBuilder(
      Template.TemplateType templateType, File outputFolder) {
    return DockerfileGenerator.builder(templateType, "beam_version", containerName, outputFolder);
  }

  @Test
  public void testGeneratePythonDockerfileDefaults() throws IOException, TemplateException {
    new File(outputFolder.getAbsolutePath() + "/" + containerName).mkdirs();
    createDockerfileGeneratorBuilder(Template.TemplateType.PYTHON, outputFolder).build().generate();
    File outputFile =
        new File(outputFolder.getAbsolutePath() + "/" + containerName + "/Dockerfile");

    assertTrue(outputFile.exists());
    String fileContents = Files.asCharSource(outputFile, StandardCharsets.UTF_8).read();
    assertThat(fileContents).contains("FROM " + BASE_PYTHON_CONTAINER_IMAGE);
    assertThat(fileContents)
        .contains("RUN pip install -U -r --require-hashes $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE");
    assertThat(fileContents)
        .contains(String.format("ENTRYPOINT [\"%s\"]", PYTHON_LAUNCHER_ENTRYPOINT));
  }

  @Test
  public void testGeneratePythonDockerfile() throws IOException, TemplateException {
    new File(outputFolder.getAbsolutePath() + "/" + containerName).mkdirs();
    createDockerfileGeneratorBuilder(Template.TemplateType.PYTHON, outputFolder)
        .setBasePythonContainerImage("a python container image")
        .setBaseJavaContainerImage("a java container image")
        .setFilesToCopy(List.of("main.py", "requirements.txt"))
        .setEntryPoint(List.of("python/entry/point"))
        .build()
        .generate();
    File outputFile =
        new File(outputFolder.getAbsolutePath() + "/" + containerName + "/Dockerfile");

    assertTrue(outputFile.exists());
    String fileContents = Files.asCharSource(outputFile, StandardCharsets.UTF_8).read();
    assertThat(fileContents).contains("FROM a python container image");
    assertThat(fileContents)
        .contains("RUN pip install -U -r --require-hashes $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE");
    assertThat(fileContents).contains("COPY main.py requirements.txt $WORKDIR/");
    assertThat(fileContents).contains("ENTRYPOINT [\"python/entry/point\"]");
  }

  @Test
  public void testGenerateXLangDockerfileDefaults() throws IOException, TemplateException {
    new File(outputFolder.getAbsolutePath() + "/containerName").mkdirs();
    new File(outputFolder.getAbsolutePath() + "/extra_libs/example").mkdirs();
    File artifactPath = new File(outputFolder.getAbsolutePath() + "/artifactPath");
    artifactPath.mkdirs();

    createDockerfileGeneratorBuilder(
            Template.TemplateType.XLANG, new File(outputFolder.getPath() + "/classes"))
        .build()
        .generate();
    File outputFile =
        new File(outputFolder.getAbsolutePath() + "/classes/" + containerName + "/Dockerfile");

    assertTrue(outputFile.exists());
    String fileContents = Files.asCharSource(outputFile, StandardCharsets.UTF_8).read();
    assertThat(fileContents).contains("FROM " + BASE_CONTAINER_IMAGE);
    assertThat(fileContents).contains("FROM " + BASE_PYTHON_CONTAINER_IMAGE);
    assertThat(fileContents)
        .contains("pip install --no-cache-dir --require-hashes -U -r $REQUIREMENTS_FILE");
    assertThat(fileContents).contains("PY_VERSION=" + PYTHON_VERSION);
    assertThat(fileContents).contains("ENV DATAFLOW_JAVA_COMMAND_SPEC=");
    assertThat(fileContents)
        .contains(String.format("ENTRYPOINT [\"%s\"]", JAVA_LAUNCHER_ENTRYPOINT));
  }

  @Test
  public void testGenerateXLangDockerfile() throws IOException, TemplateException {
    new File(outputFolder.getAbsolutePath() + "/" + containerName).mkdirs();
    new File(outputFolder.getAbsolutePath() + "/extra_libs/example").mkdirs();
    File artifactPath = new File(outputFolder.getAbsolutePath() + "/artifactPath");
    artifactPath.mkdirs();

    List<String> filesToCopy = List.of("container-generated-metadata.json", "requirements.txt");
    Set<String> directoriesToCopy = Set.of(containerName, "otherDirectory");
    createDockerfileGeneratorBuilder(
            Template.TemplateType.XLANG, new File(outputFolder.getPath() + "/classes"))
        .setBasePythonContainerImage("a python container image")
        .setBaseJavaContainerImage("a java container image")
        .setPythonVersion("py_version")
        .setEntryPoint(List.of("java/entry/point"))
        .setCommandSpec("command_spec")
        .setFilesToCopy(filesToCopy)
        .setDirectoriesToCopy(directoriesToCopy)
        .build()
        .generate();
    File outputFile =
        new File(outputFolder.getAbsolutePath() + "/classes/" + containerName + "/Dockerfile");

    assertTrue(outputFile.exists());
    String fileContents = Files.asCharSource(outputFile, StandardCharsets.UTF_8).read();
    assertThat(fileContents).contains("FROM a python container image");
    assertThat(fileContents).contains("FROM a java container image");
    assertThat(fileContents)
        .contains("COPY container-generated-metadata.json requirements.txt $WORKDIR/");
    assertThat(fileContents)
        .contains("COPY " + containerName + "/ $WORKDIR/" + containerName + "/");
    assertThat(fileContents).contains("COPY otherDirectory/ $WORKDIR/otherDirectory/");
    assertThat(fileContents).contains("=py_version");
    assertThat(fileContents).contains("ENTRYPOINT [\"java/entry/point\"]");
    assertThat(fileContents).contains("ENV DATAFLOW_JAVA_COMMAND_SPEC=command_spec");
  }

  @Test
  public void testGenerateYamlDockerfileDefaults() throws IOException, TemplateException {
    new File(outputFolder.getAbsolutePath() + "/" + containerName).mkdirs();
    createDockerfileGeneratorBuilder(Template.TemplateType.YAML, outputFolder).build().generate();
    File outputFile =
        new File(outputFolder.getAbsolutePath() + "/" + containerName + "/Dockerfile");

    assertTrue(outputFile.exists());
    String fileContents = Files.asCharSource(outputFile, StandardCharsets.UTF_8).read();
    assertThat(fileContents).contains("FROM " + BASE_CONTAINER_IMAGE);
    assertThat(fileContents).contains("FROM " + BASE_PYTHON_CONTAINER_IMAGE);
    assertThat(fileContents).contains("PY_VERSION=" + PYTHON_VERSION);
    assertThat(fileContents)
        .contains(String.format("ENTRYPOINT [\"%s\"]", PYTHON_LAUNCHER_ENTRYPOINT));
  }

  @Test
  public void testGenerateYamlDockerfile() throws IOException, TemplateException {
    new File(outputFolder.getAbsolutePath() + "/" + containerName).mkdirs();
    createDockerfileGeneratorBuilder(Template.TemplateType.YAML, outputFolder)
        .setBasePythonContainerImage("a python container image")
        .setBaseJavaContainerImage("a java container image")
        .setPythonVersion("py_version")
        .setEntryPoint(List.of("python/entry/point"))
        .build()
        .generate();
    File outputFile =
        new File(outputFolder.getAbsolutePath() + "/" + containerName + "/Dockerfile");

    assertTrue(outputFile.exists());
    String fileContents = Files.asCharSource(outputFile, StandardCharsets.UTF_8).read();
    assertThat(fileContents).contains("FROM a python container image");
    assertThat(fileContents).contains("FROM a java container image");
    assertThat(fileContents).contains("=py_version");
    assertThat(fileContents).contains("ENTRYPOINT [\"python/entry/point\"]");
  }

  @Test
  public void testGenerateYamlDockerfileWithOtherFiles() throws IOException, TemplateException {
    new File(outputFolder.getAbsolutePath() + "/" + containerName).mkdirs();
    createDockerfileGeneratorBuilder(Template.TemplateType.YAML, outputFolder)
        .setBasePythonContainerImage("a python container image")
        .setBaseJavaContainerImage("a java container image")
        .setPythonVersion("py_version")
        .setEntryPoint(List.of("python/entry/point"))
        .setFilesToCopy(List.of("other_file"))
        .build()
        .generate();
    File outputFile =
        new File(outputFolder.getAbsolutePath() + "/" + containerName + "/Dockerfile");

    assertTrue(outputFile.exists());
    String fileContents = Files.asCharSource(outputFile, StandardCharsets.UTF_8).read();
    assertThat(fileContents).contains("FROM a python container image");
    assertThat(fileContents).contains("FROM a java container image");
    assertThat(fileContents).contains("=py_version");
    assertThat(fileContents).contains("COPY other_file $WORKDIR/");
    assertThat(fileContents).contains("ENTRYPOINT [\"python/entry/point\"]");
  }

  @Test
  public void testGenerateYamlDockerfileWithInternalMaven() throws IOException, TemplateException {
    new File(outputFolder.getAbsolutePath() + "/" + containerName).mkdirs();
    createDockerfileGeneratorBuilder(Template.TemplateType.YAML, outputFolder)
        .setBasePythonContainerImage("a python container image")
        .setBaseJavaContainerImage("a java container image")
        .setPythonVersion("py_version")
        .setServiceAccountSecretName("someSecret")
        .setAirlockPythonRepo("airlockPythonRepo")
        .build()
        .generate();
    File outputFile =
        new File(outputFolder.getAbsolutePath() + "/" + containerName + "/Dockerfile");

    assertTrue(outputFile.exists());
    String fileContents = Files.asCharSource(outputFile, StandardCharsets.UTF_8).read();
    assertThat(fileContents).contains("FROM a python container image");
    assertThat(fileContents).contains("FROM a java container image");
    assertThat(fileContents).contains("=py_version");
    assertThat(fileContents).contains("gcloud secrets versions access latest --secret=someSecret");
    assertThat(fileContents)
        .contains("https://us-python.pkg.dev/artifact-foundry-prod/airlockPythonRepo");
  }

  @Test
  public void testGenerateDockerfileAddParameterWithNullOrEmptyParameterName() {
    DockerfileGenerator.Builder dockerfileBuilder =
        createDockerfileGeneratorBuilder(Template.TemplateType.XLANG, outputFolder);
    assertThrows(
        IllegalArgumentException.class, () -> dockerfileBuilder.addParameter(null, "some_value"));
    assertThrows(
        IllegalArgumentException.class, () -> dockerfileBuilder.addParameter("", "some_value"));
  }

  @Test
  public void testGenerateDockerfileAddParameterWithNullValue() {
    DockerfileGenerator.Builder dockerfileBuilder =
        createDockerfileGeneratorBuilder(Template.TemplateType.XLANG, outputFolder);
    assertThrows(
        NullPointerException.class, () -> dockerfileBuilder.addParameter("some_parameter", null));
  }

  @Test
  public void testGenerateDockerfileAddStringParameterWithEmptyValue() {
    DockerfileGenerator.Builder dockerfileBuilder =
        createDockerfileGeneratorBuilder(Template.TemplateType.XLANG, outputFolder);
    assertThrows(
        IllegalArgumentException.class,
        () -> dockerfileBuilder.addStringParameter("some_parameter", ""));
  }

  @Test
  public void testGenerateDockerfileSetPredefinedParametersWithNullOrEmptyValues() {
    DockerfileGenerator.Builder dockerfileBuilder =
        createDockerfileGeneratorBuilder(Template.TemplateType.XLANG, outputFolder);

    assertThrows(
        IllegalArgumentException.class, () -> dockerfileBuilder.setBasePythonContainerImage(null));
    assertThrows(
        IllegalArgumentException.class, () -> dockerfileBuilder.setBasePythonContainerImage(""));

    assertThrows(
        IllegalArgumentException.class, () -> dockerfileBuilder.setBaseJavaContainerImage(null));
    assertThrows(
        IllegalArgumentException.class, () -> dockerfileBuilder.setBaseJavaContainerImage(""));

    assertThrows(IllegalArgumentException.class, () -> dockerfileBuilder.setPythonVersion(null));
    assertThrows(IllegalArgumentException.class, () -> dockerfileBuilder.setPythonVersion(""));

    assertThrows(NullPointerException.class, () -> dockerfileBuilder.setEntryPoint(null));
    assertThrows(IllegalArgumentException.class, () -> dockerfileBuilder.setEntryPoint(List.of()));

    assertThrows(NullPointerException.class, () -> dockerfileBuilder.setFilesToCopy(null));
    assertThrows(IllegalArgumentException.class, () -> dockerfileBuilder.setFilesToCopy(List.of()));

    assertThrows(NullPointerException.class, () -> dockerfileBuilder.setDirectoriesToCopy(null));
    assertThrows(
        IllegalArgumentException.class, () -> dockerfileBuilder.setDirectoriesToCopy(Set.of()));

    assertThrows(IllegalArgumentException.class, () -> dockerfileBuilder.setCommandSpec(null));
    assertThrows(IllegalArgumentException.class, () -> dockerfileBuilder.setCommandSpec(""));

    assertThrows(IllegalArgumentException.class, () -> dockerfileBuilder.setWorkingDirectory(null));
    assertThrows(IllegalArgumentException.class, () -> dockerfileBuilder.setWorkingDirectory(""));
  }
}
