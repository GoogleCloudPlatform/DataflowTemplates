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
package com.google.cloud.teleport.v2.common;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.options.CommonTemplateOptions;
import com.google.cloud.teleport.v2.utils.GCSUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for CommonTemplateJvmInitializer. */
@RunWith(JUnit4.class)
public final class CommonTemplateJvmInitializerTest {

  @Rule public final TemporaryFolder destinationRoot = new TemporaryFolder();
  private static String destinationDirectory;
  private static CommonTemplateJvmInitializer jvmInitializer;
  private static String filePath;

  @Before
  public void setUp() {
    destinationDirectory = destinationRoot.getRoot().getAbsolutePath() + "/test";
    jvmInitializer = new CommonTemplateJvmInitializer();
    jvmInitializer.withDestinationDirectory(destinationDirectory);
    jvmInitializer.withFileSystemPattern("^file:\\/\\/");

    ClassLoader classLoader = this.getClass().getClassLoader();
    filePath = "file://" + classLoader.getResource("AvroConvertersTest/test_schema.json").getFile();
  }

  @Test
  public void testBeforeProcessingExtraFilesToStage_savesFilesAsExpected() {
    // Arrange
    CommonTemplateOptions options =
        TestPipeline.testingPipelineOptions().as(CommonTemplateOptions.class);
    options.setExtraFilesToStage(filePath);
    // Act
    jvmInitializer.beforeProcessing(options);
    // Assert
    assertThat(GCSUtils.getFilesInDirectory(destinationDirectory).size()).isEqualTo(1);
    assertThat(GCSUtils.getGcsFileAsString(destinationDirectory + "/test_schema.json"))
        .isEqualTo(GCSUtils.getGcsFileAsString(filePath));
  }

  @Test(expected = RuntimeException.class)
  public void testBeforeProcessingExtraFilesToStage_throwsExceptionForIllegalFiles() {
    // Arrange
    CommonTemplateOptions options =
        TestPipeline.testingPipelineOptions().as(CommonTemplateOptions.class);
    options.setExtraFilesToStage("hdfs://hadoop-file,gs://bucket/file");
    // Act
    jvmInitializer.beforeProcessing(options);
  }
}
