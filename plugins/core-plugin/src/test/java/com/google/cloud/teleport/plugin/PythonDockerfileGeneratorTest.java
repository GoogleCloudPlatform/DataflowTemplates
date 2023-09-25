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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import freemarker.template.TemplateException;
import java.io.File;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for class {@link PythonDockerfileGenerator}. */
@RunWith(JUnit4.class)
public class PythonDockerfileGeneratorTest {
  private final File outputFolder = Files.createTempDir().getAbsoluteFile();

  @Test
  public void testGenerateDockerfile() throws IOException, TemplateException {
    new File(outputFolder.getAbsolutePath() + "/word-count").mkdirs();
    PythonDockerfileGenerator.generateDockerfile("a container image", "word-count", outputFolder);
    File outputFile = new File(outputFolder.getAbsolutePath() + "/word-count/Dockerfile");

    assertTrue(outputFile.exists());
    String fileContents = Files.toString(outputFile, Charsets.UTF_8);
    assertThat(fileContents).contains("FROM a container image");
  }
}
