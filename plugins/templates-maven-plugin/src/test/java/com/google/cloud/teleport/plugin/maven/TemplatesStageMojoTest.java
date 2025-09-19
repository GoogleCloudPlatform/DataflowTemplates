/*
 * Copyright (C) 2025 Google LLC
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.plugin.model.TemplateDefinitions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TemplatesStageMojoTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private TemplatesStageMojo mojo;
  private MavenProject mavenProject;
  private File baseDir;
  private File outputClassesDir;

  @Before
  public void setUp() throws IOException {
    mojo = new TemplatesStageMojo();
    mavenProject = mock(MavenProject.class);
    baseDir = temporaryFolder.newFolder("baseDir");
    outputClassesDir = temporaryFolder.newFolder("outputClassesDir");

    when(mavenProject.getBasedir()).thenReturn(baseDir);
    mojo.project = mavenProject;
    mojo.outputClassesDirectory = outputClassesDir;

    // Needed for DockerfileGenerator
    mojo.beamVersion = "2.67.0";
    mojo.basePythonContainerImage = "python-base";
    mojo.baseContainerImage = "base";
    mojo.pythonVersion = "3.9";
    mojo.pythonTemplateLauncherEntryPoint = "/default/launcher";
  }

  @Test
  public void testGenerateFlexTemplateImagePath() {
    String containerName = "name";
    String projectId = "some-project";
    ImmutableMap<String, String> testCases =
        ImmutableMap.<String, String>builder()
            .put("", "gcr.io/some-project/name")
            .put("gcr.io", "gcr.io/some-project/name")
            .put("eu.gcr.io", "eu.gcr.io/some-project/name")
            .put(
                "us-docker.pkg.dev/other-project/other-repo",
                "us-docker.pkg.dev/other-project/other-repo/name")
            .build();
    testCases.forEach(
        (key, value) -> {
          // workaround for null key we intended to test
          if (Strings.isNullOrEmpty(key)) {
            key = null;
          }
          assertEquals(
              value,
              TemplatesStageMojo.generateFlexTemplateImagePath(
                  containerName, projectId, null, key));
        });
  }

  @Test
  public void testGenerateFlexTemplateImagePathWithDomain() {
    String containerName = "name";
    String projectId = "google.com:project";
    ImmutableMap<String, String> testCases =
        ImmutableMap.<String, String>builder()
            .put("", "gcr.io/google.com/project/name")
            .put("gcr.io", "gcr.io/google.com/project/name")
            .put("eu.gcr.io", "eu.gcr.io/google.com/project/name")
            .put(
                "us-docker.pkg.dev/other-project/other-repo",
                "us-docker.pkg.dev/other-project/other-repo/name")
            .build();
    testCases.forEach(
        (key, value) -> {
          // workaround for null key we intended to test
          if (Strings.isNullOrEmpty(key)) {
            key = null;
          }
          assertEquals(
              value,
              TemplatesStageMojo.generateFlexTemplateImagePath(
                  containerName, projectId, null, key));
        });
  }

  @Test
  public void testPrepareYamlTemplateFiles_copiesFile() throws Exception {
    // Arrange
    TemplateDefinitions definitions = mock(TemplateDefinitions.class);
    Template template = mock(Template.class);
    when(definitions.getTemplateAnnotation()).thenReturn(template);
    when(template.yamlTemplateFile()).thenReturn("my-template.yaml");

    // Create necessary files and assert directory
    File yamlDir = new File(baseDir, "src/main/yaml");
    assertTrue(yamlDir.mkdirs());
    File sourceFile = new File(yamlDir, "my-template.yaml");
    Files.writeString(sourceFile.toPath(), "yaml: content");

    // Act
    mojo.prepareYamlTemplateFiles(definitions);

    // Assert that yaml template file exists and has the correct content
    File destinationFile = new File(outputClassesDir, "template.yaml");
    assertTrue(destinationFile.exists());
    assertEquals("yaml: content", Files.readString(destinationFile.toPath()));
  }

  @Test
  public void testPrepareYamlTemplateFiles_fileNotFound_throwsException() {
    // Arrange
    TemplateDefinitions definitions = mock(TemplateDefinitions.class);
    Template template = mock(Template.class);
    when(definitions.getTemplateAnnotation()).thenReturn(template);
    when(template.yamlTemplateFile()).thenReturn("my-template.yaml");

    // No yaml template file created

    // Act and assert that yaml file not found
    MojoExecutionException exception =
        assertThrows(
            MojoExecutionException.class, () -> mojo.prepareYamlTemplateFiles(definitions));
    assertTrue(exception.getMessage().startsWith("YAML template file not found:"));
  }

  @Test
  public void testPrepareYamlDockerfile_generatesDockerfile() throws Exception {
    // Arrange
    String containerName = "my-yaml-template";
    TemplateDefinitions definitions = mock(TemplateDefinitions.class);
    Template template = mock(Template.class);
    when(definitions.getTemplateAnnotation()).thenReturn(template);
    when(template.type()).thenReturn(Template.TemplateType.YAML);
    when(template.filesToCopy()).thenReturn(new String[] {"script.py"});
    when(template.yamlTemplateFile()).thenReturn("my-template.yaml");
    when(template.entryPoint()).thenReturn(new String[] {"python", "script.py"});

    // Create necessary directories and assert
    File containerDir = new File(outputClassesDir, containerName);
    assertTrue(containerDir.mkdirs());

    // Act
    mojo.prepareYamlDockerfile(definitions, containerName);

    // Assert dockerfile exists
    File dockerfile = new File(containerDir, "Dockerfile");
    assertTrue(dockerfile.exists());

    // Assert dockerfile contents match expected
    String content = new String(Files.readAllBytes(dockerfile.toPath()), StandardCharsets.UTF_8);
    assertTrue(content.contains("FROM python-base"));
    assertTrue(content.contains("FROM base"));
    assertTrue(content.contains("COPY script.py template.yaml $WORKDIR/"));
    assertTrue(content.contains("ENTRYPOINT [\"python\", \"script.py\"]"));
  }

  @Test
  public void testPrepareYamlDockerfile_usesDefaults() throws Exception {
    // Arrange
    String containerName = "my-yaml-template-defaults";
    TemplateDefinitions definitions = mock(TemplateDefinitions.class);
    Template template = mock(Template.class);
    when(definitions.getTemplateAnnotation()).thenReturn(template);
    when(template.type()).thenReturn(Template.TemplateType.YAML);
    when(template.filesToCopy()).thenReturn(new String[] {});
    when(template.yamlTemplateFile()).thenReturn(null);
    when(template.entryPoint()).thenReturn(new String[] {});

    File containerDir = new File(outputClassesDir, containerName);
    assertTrue(containerDir.mkdirs());

    // Act
    mojo.prepareYamlDockerfile(definitions, containerName);

    // Assert dockerfile exists
    File dockerfile = new File(containerDir, "Dockerfile");
    assertTrue(dockerfile.exists());

    // Assert dockerfile contents match expected
    String content = new String(Files.readAllBytes(dockerfile.toPath()), StandardCharsets.UTF_8);
    assertTrue(content.contains("FROM python-base"));
    assertTrue(content.contains("FROM base"));
    assertFalse(content.contains("COPY template.yaml"));
    assertTrue(content.contains("ENTRYPOINT [\"/default/launcher\"]"));
  }

  @Test
  public void testPrepareYamlDockerfile_withEmptyYamlFile() throws Exception {
    // Arrange
    String containerName = "my-yaml-template-empty-yaml";
    TemplateDefinitions definitions = mock(TemplateDefinitions.class);
    Template template = mock(Template.class);
    when(definitions.getTemplateAnnotation()).thenReturn(template);
    when(template.type()).thenReturn(Template.TemplateType.YAML);
    when(template.filesToCopy()).thenReturn(new String[] {"script.py"});
    when(template.yamlTemplateFile()).thenReturn("");
    when(template.entryPoint()).thenReturn(new String[] {});

    File containerDir = new File(outputClassesDir, containerName);
    assertTrue(containerDir.mkdirs());

    // Act
    mojo.prepareYamlDockerfile(definitions, containerName);

    // Assert dockerfile exists
    File dockerfile = new File(containerDir, "Dockerfile");
    assertTrue(dockerfile.exists());

    // Assert dockerfile contents match expected
    String content = new String(Files.readAllBytes(dockerfile.toPath()), StandardCharsets.UTF_8);
    assertTrue(content.contains("COPY script.py $WORKDIR/"));
    assertFalse(content.contains("template.yaml"));
  }

  @Test
  public void testPrepareYamlDockerfile_alreadyExists() throws Exception {
    // Arrange
    String containerName = "my-yaml-template-exists";
    TemplateDefinitions definitions = mock(TemplateDefinitions.class);

    File containerDir = new File(outputClassesDir, containerName);
    assertTrue(containerDir.mkdirs());
    File dockerfile = new File(containerDir, "Dockerfile");
    String expectedContent = "existing content";
    Files.writeString(dockerfile.toPath(), expectedContent);

    // Act
    mojo.prepareYamlDockerfile(definitions, containerName);

    // Assert existing dockerfile is used
    assertEquals(
        expectedContent,
        new String(Files.readAllBytes(dockerfile.toPath()), StandardCharsets.UTF_8));
  }
}
