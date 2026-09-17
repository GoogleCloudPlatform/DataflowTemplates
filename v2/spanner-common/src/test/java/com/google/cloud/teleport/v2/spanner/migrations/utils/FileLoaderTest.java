/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.spanner.migrations.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.nio.file.Files;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileLoaderTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static void setupFileSystem() {
    FileSystems.setDefaultPipelineOptions(TestPipeline.testingPipelineOptions());
  }

  @Test
  public void testReadConfigFilePath_Success() throws Exception {
    File tempFile = tempFolder.newFile("test-config.json");
    String content = "{\"testKey\": \"testValue\"}";
    Files.writeString(tempFile.toPath(), content);

    String result = FileLoader.readConfigFilePath(tempFile.getAbsolutePath());
    assertEquals(content, result);
  }

  @Test
  public void testReadConfigFilePath_FileNotFound() {
    Exception exception =
        assertThrows(
            RuntimeException.class, () -> FileLoader.readConfigFilePath("non-existent-file.json"));
    assertEquals(
        "Failed to read configuration input file at non-existent-file.json. Make sure it is ASCII or UTF-8 encoded and contains a well-formed HOCON/JSON string.",
        exception.getMessage());
  }
}
