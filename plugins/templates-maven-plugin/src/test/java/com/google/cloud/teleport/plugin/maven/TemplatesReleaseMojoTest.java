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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Collectors;
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
    baseDir = temporaryFolder.newFolder("yaml");
    File outputDirectory = temporaryFolder.newFolder("output");

    when(mavenProject.getBasedir()).thenReturn(baseDir);

    mojo.project = mavenProject;
    mojo.session = mock(MavenSession.class);
    mojo.outputDirectory = outputDirectory;
    mojo.stagePrefix = "test-prefix";
    mojo.bucketName = "gs://test-bucket";
  }

  @Test
  public void testExecute_publishesYamlBlueprintsAndCreatesManifest()
      throws MojoExecutionException, IOException {
    mojo.publishYamlBlueprints = true;
    mojo.yamlBlueprintsPath = "src/main/yaml";
    mojo.yamlBlueprintsGCSPath = "yaml-blueprints";
    mojo.yamlManifestName = "yaml-manifest.json";

    // Create a fake yaml file to be uploaded
    File yamlDir = new File(baseDir, mojo.yamlBlueprintsPath);
    yamlDir.mkdirs();
    File yamlFile1 = new File(yamlDir, "my-blueprint.yaml");
    Files.write(yamlFile1.toPath(), getYamlContent().getBytes(StandardCharsets.UTF_8));
    File yamlFile2 = new File(yamlDir, "another-blueprint.yaml");
    Files.write(yamlFile2.toPath(), getYamlContent().getBytes(StandardCharsets.UTF_8));

    // Mock the static `StorageOptions.getDefaultInstance()` to return a mock Storage service.
    try (MockedStatic<StorageOptions> storageOptionsMock =
        Mockito.mockStatic(StorageOptions.class)) {
      Storage mockStorage = mock(Storage.class);
      StorageOptions mockStorageOptions = mock(StorageOptions.class);
      storageOptionsMock.when(StorageOptions::getDefaultInstance).thenReturn(mockStorageOptions);
      when(mockStorageOptions.getService()).thenReturn(mockStorage);

      Map<String, byte[]> uploadedFiles = new HashMap<>();
      ArgumentCaptor<BlobInfo> blobInfoCaptor = ArgumentCaptor.forClass(BlobInfo.class);

      Mockito.doAnswer(
              invocation -> {
                BlobInfo blobInfo = invocation.getArgument(0);
                InputStream inputStream = invocation.getArgument(1);
                uploadedFiles.put(blobInfo.getName(), inputStream.readAllBytes());
                return null;
              })
          .when(mockStorage)
          .create(blobInfoCaptor.capture(), Mockito.any(InputStream.class));

      // Act
      mojo.execute();

      // Assert
      verify(mockStorage, Mockito.times(3))
          .create(Mockito.any(BlobInfo.class), Mockito.any(InputStream.class));

      String manifestName =
          String.join("/", mojo.stagePrefix, mojo.yamlBlueprintsGCSPath, mojo.yamlManifestName);

      assertTrue(uploadedFiles.containsKey(manifestName));
      String manifestContent = new String(uploadedFiles.get(manifestName), StandardCharsets.UTF_8);

      Gson gson = new Gson();
      Type type = new TypeToken<List<Map<String, String>>>() {}.getType();
      List<Map<String, String>> actualBlueprints = gson.fromJson(manifestContent, type);
      List<Map<String, String>> expectedBlueprints =
          new ArrayList<>(
              List.of(
                  Map.of(
                      "name",
                      yamlFile1.getName(),
                      "path",
                      String.join(
                          "/", mojo.stagePrefix, mojo.yamlBlueprintsGCSPath, yamlFile1.getName())),
                  Map.of(
                      "name",
                      yamlFile2.getName(),
                      "path",
                      String.join(
                          "/",
                          mojo.stagePrefix,
                          mojo.yamlBlueprintsGCSPath,
                          yamlFile2.getName()))));

      // Sort both lists to ensure proper comparison
      actualBlueprints.sort(Comparator.comparing(m -> m.get("name")));
      expectedBlueprints.sort(Comparator.comparing(m -> m.get("name")));

      assertEquals(expectedBlueprints, actualBlueprints);
    }
  }

  @Test
  public void testExecute_yamlBlueprintsDirectoryMissing_logsWarning()
      throws MojoExecutionException {
    mojo.publishYamlBlueprints = true;
    mojo.yamlBlueprintsPath = "a-path-that-does-not-exist";
    mojo.yamlBlueprintsGCSPath = "yaml-blueprints";

    Logger logger = Logger.getLogger(TemplatesReleaseMojo.class.getName());
    MemoryHandler memoryHandler = new MemoryHandler();
    memoryHandler.setLevel(Level.WARNING);
    logger.addHandler(memoryHandler);

    try {
      // Act
      mojo.execute();

    } finally {
      logger.removeHandler(memoryHandler);
    }

    // Assert
    boolean foundWarning =
        memoryHandler.getRecords().stream()
            .anyMatch(
                r ->
                    r.getLevel() == Level.WARNING
                        && r.getMessage()
                            .equals(
                                "YAML blueprints directory not found, skipping upload for path: "));
    assertTrue("Did not find expected warning log message.", foundWarning);
  }

  @Test
  public void testExecute_publishYamlBlueprintsFalse_skipsUpload() throws MojoExecutionException {
    mojo.publishYamlBlueprints = false;
    setupAndAssertNoFilesUploaded();
  }

  private void setupAndAssertNoFilesUploaded() throws MojoExecutionException {
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
      // Verify that no file was uploaded
      verify(mockStorage, Mockito.never())
          .create(Mockito.any(BlobInfo.class), Mockito.any(InputStream.class));
    }
  }

  @Test
  public void testExecute_uploadsAllBlueprintsWhenSomeExist()
      throws MojoExecutionException, IOException {
    mojo.publishYamlBlueprints = true;
    mojo.yamlBlueprintsPath = "src/main/yaml";
    mojo.yamlBlueprintsGCSPath = "yaml-blueprints";
    mojo.yamlManifestName = "yaml-manifest.json";

    // Create fake yaml files to be uploaded
    File yamlDir = new File(baseDir, mojo.yamlBlueprintsPath);
    yamlDir.mkdirs();
    File existingFile = new File(yamlDir, "existing-blueprint.yaml");
    Files.write(existingFile.toPath(), getYamlContent().getBytes(StandardCharsets.UTF_8));
    File newFile = new File(yamlDir, "new-blueprint.yaml");
    Files.write(newFile.toPath(), getYamlContent().getBytes(StandardCharsets.UTF_8));

    // Mock the static `StorageOptions.getDefaultInstance()` to return a mock Storage service.
    try (MockedStatic<StorageOptions> storageOptionsMock =
        Mockito.mockStatic(StorageOptions.class)) {
      Storage mockStorage = mock(Storage.class);
      StorageOptions mockStorageOptions = mock(StorageOptions.class);
      storageOptionsMock.when(StorageOptions::getDefaultInstance).thenReturn(mockStorageOptions);
      when(mockStorageOptions.getService()).thenReturn(mockStorage);

      Map<String, byte[]> uploadedFiles = new HashMap<>();
      ArgumentCaptor<BlobInfo> blobInfoCaptor = ArgumentCaptor.forClass(BlobInfo.class);

      Mockito.doAnswer(
              invocation -> {
                BlobInfo blobInfo = invocation.getArgument(0);
                InputStream inputStream = invocation.getArgument(1);
                uploadedFiles.put(blobInfo.getName(), inputStream.readAllBytes());
                return null;
              })
          .when(mockStorage)
          .create(blobInfoCaptor.capture(), Mockito.any(InputStream.class));

      // Act
      mojo.execute();

      // Assert
      // Should be called for both blueprints and the manifest.
      verify(mockStorage, Mockito.times(3))
          .create(Mockito.any(BlobInfo.class), Mockito.any(InputStream.class));

      List<BlobInfo> createdBlobs = blobInfoCaptor.getAllValues();
      List<String> createdBlobNames =
          createdBlobs.stream().map(BlobInfo::getName).collect(Collectors.toList());

      String bucket = mojo.bucketName.replace("gs://", "");
      String existingObjectName =
          String.join("/", mojo.stagePrefix, mojo.yamlBlueprintsGCSPath, existingFile.getName());
      String newObjectName =
          String.join("/", mojo.stagePrefix, mojo.yamlBlueprintsGCSPath, newFile.getName());

      String manifestName =
          String.join("/", mojo.stagePrefix, mojo.yamlBlueprintsGCSPath, mojo.yamlManifestName);
      assertTrue(createdBlobNames.contains(newObjectName));
      assertTrue(createdBlobNames.contains(manifestName));
      assertTrue(createdBlobNames.contains(existingObjectName));

      // Verify manifest contains both blueprints
      assertTrue(uploadedFiles.containsKey(manifestName));
      String manifestContent = new String(uploadedFiles.get(manifestName), StandardCharsets.UTF_8);

      Gson gson = new Gson();
      Type type = new TypeToken<List<Map<String, String>>>() {}.getType();
      List<Map<String, String>> actualBlueprints = gson.fromJson(manifestContent, type);

      assertEquals(2, actualBlueprints.size());

      List<String> blueprintNamesInManifest =
          actualBlueprints.stream().map(b -> b.get("name")).collect(Collectors.toList());
      assertTrue(blueprintNamesInManifest.contains("existing-blueprint.yaml"));
      assertTrue(blueprintNamesInManifest.contains("new-blueprint.yaml"));
    }
  }

  private static String getYamlContent() {
    return """
template:
  name: "Kafka_to_BigQuery_Yaml"
  category: "STREAMING"
  type: "YAML"
  display_name: "Kafka to BigQuery (YAML)"

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

  private static class MemoryHandler extends Handler {
    private final List<LogRecord> records = new ArrayList<>();

    @Override
    public void publish(LogRecord record) {
      records.add(record);
    }

    @Override
    public void flush() {}

    @Override
    public void close() throws SecurityException {}

    public List<LogRecord> getRecords() {
      return records;
    }
  }
}
