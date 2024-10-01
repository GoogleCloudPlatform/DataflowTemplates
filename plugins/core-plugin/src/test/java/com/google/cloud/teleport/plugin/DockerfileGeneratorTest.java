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
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.metadata.Template;
import com.google.common.io.Files;
import freemarker.template.TemplateException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for class {@link DockerfileGenerator}. */
@RunWith(JUnit4.class)
public class DockerfileGeneratorTest {
  private final File outputFolder = Files.createTempDir().getAbsoluteFile();

  @Test
  public void testGeneratePythonDockerfileDefaults() throws IOException, TemplateException {
    new File(outputFolder.getAbsolutePath() + "/word-count").mkdirs();
    DockerfileGenerator.builder(
            Template.TemplateType.PYTHON, "beam_version", "word-count", outputFolder)
        .build()
        .generate();
    File outputFile = new File(outputFolder.getAbsolutePath() + "/word-count/Dockerfile");

    assertTrue(outputFile.exists());
    String fileContents = Files.toString(outputFile, StandardCharsets.UTF_8);
    assertThat(fileContents).contains("FROM " + BASE_PYTHON_CONTAINER_IMAGE);
    assertThat(fileContents).contains("ARG BEAM_VERSION=beam_version");
    assertThat(fileContents)
        .contains(String.format("ENTRYPOINT [\"%s\"]", PYTHON_LAUNCHER_ENTRYPOINT));
  }

  @Test
  public void testGeneratePythonDockerfile() throws IOException, TemplateException {
    new File(outputFolder.getAbsolutePath() + "/word-count").mkdirs();
    DockerfileGenerator.builder(
            Template.TemplateType.PYTHON, "beam_version", "word-count", outputFolder)
        .setBasePythonContainerImage("a python container image")
        .setBaseJavaContainerImage("a java container image")
        .setFilesToCopy(Map.of("main.py", Set.of("requirements.txt*")))
        .setEntryPoint("python/entry/point")
        .build()
        .generate();
    File outputFile = new File(outputFolder.getAbsolutePath() + "/word-count/Dockerfile");

    assertTrue(outputFile.exists());
    String fileContents = Files.toString(outputFile, StandardCharsets.UTF_8);
    assertThat(fileContents).contains("FROM a python container image");
    assertThat(fileContents).contains("ARG BEAM_VERSION=beam_version");
    assertThat(fileContents).contains("COPY main.py requirements.txt* /$WORKDIR/");
    assertThat(fileContents).contains("ENTRYPOINT [\"python/entry/point\"]");
  }

  @Test
  public void testGenerateXLangDockerfileDefaults() throws IOException, TemplateException {
    new File(outputFolder.getAbsolutePath() + "/containerName").mkdirs();
    new File(outputFolder.getAbsolutePath() + "/extra_libs/example").mkdirs();
    File artifactPath = new File(outputFolder.getAbsolutePath() + "/artifactPath");
    artifactPath.mkdirs();

    DockerfileGenerator.builder(
            Template.TemplateType.XLANG,
            "beam_version",
            "containerName",
            new File(outputFolder.getPath() + "/classes"))
        .build()
        .generate();
    File outputFile =
        new File(outputFolder.getAbsolutePath() + "/classes/containerName/Dockerfile");

    assertTrue(outputFile.exists());
    String fileContents = Files.toString(outputFile, StandardCharsets.UTF_8);
    assertThat(fileContents).contains("FROM " + BASE_CONTAINER_IMAGE);
    assertThat(fileContents).contains("FROM " + BASE_PYTHON_CONTAINER_IMAGE);
    assertThat(fileContents).contains("BEAM_VERSION=beam_version");
    assertThat(fileContents).contains("PY_VERSION=" + PYTHON_VERSION);
    assertThat(fileContents).contains("ENV DATAFLOW_JAVA_COMMAND_SPEC=");
    assertThat(fileContents)
        .contains(String.format("ENTRYPOINT [\"%s\"]", JAVA_LAUNCHER_ENTRYPOINT));
  }

  @Test
  public void testGenerateXLangDockerfile() throws IOException, TemplateException {
    new File(outputFolder.getAbsolutePath() + "/containerName").mkdirs();
    new File(outputFolder.getAbsolutePath() + "/extra_libs/example").mkdirs();
    File artifactPath = new File(outputFolder.getAbsolutePath() + "/artifactPath");
    artifactPath.mkdirs();

    Map<String, Set<String>> filesToCopy =
        Map.of("container-generated-metadata.json", Set.of("requirements.txt*"));
    Set<String> directoriesToCopy = Set.of("containerName", "otherDirectory");
    DockerfileGenerator.builder(
            Template.TemplateType.XLANG,
            "beam_version",
            "containerName",
            new File(outputFolder.getPath() + "/classes"))
        .setBasePythonContainerImage("a python container image")
        .setBaseJavaContainerImage("a java container image")
        .setPythonVersion("py_version")
        .setEntryPoint("java/entry/point")
        .setCommandSpec("command_spec")
        .setFilesToCopy(filesToCopy)
        .setDirectoriesToCopy(directoriesToCopy)
        .build()
        .generate();
    File outputFile =
        new File(outputFolder.getAbsolutePath() + "/classes/containerName/Dockerfile");

    assertTrue(outputFile.exists());
    String fileContents = Files.toString(outputFile, StandardCharsets.UTF_8);
    assertThat(fileContents).contains("FROM a python container image");
    assertThat(fileContents).contains("FROM a java container image");
    assertThat(fileContents)
        .contains("COPY container-generated-metadata.json requirements.txt* /$WORKDIR/");
    assertThat(fileContents).contains("COPY containerName/ /$WORKDIR/containerName/");
    assertThat(fileContents).contains("COPY otherDirectory/ /$WORKDIR/otherDirectory/");
    assertThat(fileContents).contains("=beam_version");
    assertThat(fileContents).contains("=py_version");
    assertThat(fileContents).contains("ENTRYPOINT [\"java/entry/point\"]");
    assertThat(fileContents).contains("ENV DATAFLOW_JAVA_COMMAND_SPEC=command_spec");
  }

  @Test
  public void testGenerateYamlDockerfileDefaults() throws IOException, TemplateException {
    new File(outputFolder.getAbsolutePath() + "/word-count").mkdirs();
    DockerfileGenerator.builder(
            Template.TemplateType.YAML, "beam_version", "word-count", outputFolder)
        .build()
        .generate();
    File outputFile = new File(outputFolder.getAbsolutePath() + "/word-count/Dockerfile");

    assertTrue(outputFile.exists());
    String fileContents = Files.toString(outputFile, StandardCharsets.UTF_8);
    assertThat(fileContents).contains("FROM " + BASE_CONTAINER_IMAGE);
    assertThat(fileContents).contains("FROM " + BASE_PYTHON_CONTAINER_IMAGE);
    assertThat(fileContents).contains("BEAM_VERSION=beam_version");
    assertThat(fileContents).contains("PY_VERSION=" + PYTHON_VERSION);
    assertThat(fileContents)
        .contains(String.format("ENTRYPOINT [\"%s\"]", PYTHON_LAUNCHER_ENTRYPOINT));
  }

  @Test
  public void testGenerateYamlDockerfile() throws IOException, TemplateException {
    new File(outputFolder.getAbsolutePath() + "/word-count").mkdirs();
    DockerfileGenerator.builder(
            Template.TemplateType.YAML, "beam_version", "word-count", outputFolder)
        .setBasePythonContainerImage("a python container image")
        .setBaseJavaContainerImage("a java container image")
        .setPythonVersion("py_version")
        .setEntryPoint("python/entry/point")
        .build()
        .generate();
    File outputFile = new File(outputFolder.getAbsolutePath() + "/word-count/Dockerfile");

    assertTrue(outputFile.exists());
    String fileContents = Files.toString(outputFile, StandardCharsets.UTF_8);
    assertThat(fileContents).contains("FROM a python container image");
    assertThat(fileContents).contains("FROM a java container image");
    assertThat(fileContents).contains("=beam_version");
    assertThat(fileContents).contains("=py_version");
    assertThat(fileContents)
        .doesNotContainMatch(
            "(?m)^(?!COPY main\\.py.*)(COPY(?!.*--from=).*/template.*$|COPY main\\.py.*)$");
    assertThat(fileContents).contains("ENTRYPOINT [\"python/entry/point\"]");
  }

  @Test
  public void testGenerateYamlDockerfileWithOtherFiles() throws IOException, TemplateException {
    new File(outputFolder.getAbsolutePath() + "/word-count").mkdirs();
    DockerfileGenerator.builder(
            Template.TemplateType.YAML, "beam_version", "word-count", outputFolder)
        .setBasePythonContainerImage("a python container image")
        .setBaseJavaContainerImage("a java container image")
        .setPythonVersion("py_version")
        .setEntryPoint("python/entry/point")
        .setFilesToCopy(Map.of("other_file", Set.of()))
        .build()
        .generate();
    File outputFile = new File(outputFolder.getAbsolutePath() + "/word-count/Dockerfile");

    assertTrue(outputFile.exists());
    String fileContents = Files.toString(outputFile, StandardCharsets.UTF_8);
    assertThat(fileContents).contains("FROM a python container image");
    assertThat(fileContents).contains("FROM a java container image");
    assertThat(fileContents).contains("=beam_version");
    assertThat(fileContents).contains("=py_version");
    assertThat(fileContents).contains("COPY other_file /$WORKDIR/");
    assertThat(fileContents).contains("ENTRYPOINT [\"python/entry/point\"]");
  }
}
