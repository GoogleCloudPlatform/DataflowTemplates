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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Tests for {@link TemplatesReleaseMojo}. */
@RunWith(JUnit4.class)
public class TemplatesReleaseMojoTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private TemplatesReleaseMojo mojo;
  private MavenProject mavenProject;
  private File baseDir;

  @Before
  public void setUp() throws IOException {
    mojo = new TemplatesReleaseMojo();
    mavenProject = mock(MavenProject.class);
    baseDir = temporaryFolder.newFolder();
    File outputDirectory = temporaryFolder.newFolder("output");

    when(mavenProject.getBasedir()).thenReturn(baseDir);

    mojo.project = mavenProject;
    mojo.session = mock(MavenSession.class);
    mojo.outputDirectory = outputDirectory;
    mojo.stagePrefix = "test-prefix";
    mojo.bucketName = "gs://test-bucket";
  }

  @Test
  public void testExecute_publishesYamlBlueprints() throws MojoExecutionException, IOException {
    // Arrange
    mojo.publishYamlBlueprints = true;

    // Create a fake yaml file to be uploaded
    File yamlDir = new File(baseDir, "yaml/src/main/yaml");
    yamlDir.mkdirs();
    File yamlFile = new File(yamlDir, "my-blueprint.yaml");
    String yamlContent = getYamlContent();
    Files.write(yamlFile.toPath(), yamlContent.getBytes());

    // Mock the static `StorageOptions.getDefaultInstance()` to return a mock Storage service.
    try (MockedStatic<StorageOptions> storageOptionsMock =
        Mockito.mockStatic(StorageOptions.class)) {
      Storage mockStorage = mock(Storage.class);
      StorageOptions mockStorageOptions = mock(StorageOptions.class);
      storageOptionsMock.when(StorageOptions::getDefaultInstance).thenReturn(mockStorageOptions);
      when(mockStorageOptions.getService()).thenReturn(mockStorage);

      // Act
      mojo.execute();

      // Assert
      ArgumentCaptor<BlobInfo> blobInfoCaptor = ArgumentCaptor.forClass(BlobInfo.class);
      ArgumentCaptor<byte[]> bytesCaptor = ArgumentCaptor.forClass(byte[].class);

      verify(mockStorage).create(blobInfoCaptor.capture(), bytesCaptor.capture());

      BlobInfo capturedBlobInfo = blobInfoCaptor.getValue();

      // Check bucketname
      assertEquals("test-bucket", capturedBlobInfo.getBucket());
      // Check yaml file name captured
      assertEquals("test-prefix/yaml-blueprints/my-blueprint.yaml", capturedBlobInfo.getName());

      // Check yaml content
      assertEquals(yamlContent, new String(bytesCaptor.getValue()));
    }
  }

  private static String getYamlContent() {
    return """
template:
  name: "Kafka_to_BigQuery_Yaml"
  category: "STREAMING"
  type: "YAML"
  display_name: "Kafka to BigQuery (YAML)

pipeline:
  transforms:
    - type: ReadFromKafka
      config:
        schema: |
          {{ schema}}
        format: {{ messageFormat }}
        topic: {{ kafkaReadTopics }}
        bootstrap_servers: {{ readBootstrapServers }}
        auto_offset_reset_config: 'earliest'
        error_handling:
          output: errors
    """;
  }
}
