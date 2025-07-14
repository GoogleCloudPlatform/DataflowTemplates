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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TemplatesStageMojoTest {
  @Test
  public void testGenerateFlexTemplateImagePath() {
    String containerName = "name";
    String projectId = "some-project";
    String stagePrefix = "some-prefix";
    boolean skipStagingPart = false;
    ImmutableMap<String, String> testCases =
        ImmutableMap.<String, String>builder()
            .put("", "gcr.io/some-project/some-prefix/name")
            .put("gcr.io", "gcr.io/some-project/some-prefix/name")
            .put("eu.gcr.io", "eu.gcr.io/some-project/some-prefix/name")
            .put(
                "us-docker.pkg.dev/other-project/other-repo",
                "us-docker.pkg.dev/other-project/other-repo/some-prefix/name")
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
                  containerName, projectId, null, key, stagePrefix, skipStagingPart));
        });
  }

  @Test
  public void testGenerateFlexTemplateImagePathWithDomain() {
    String containerName = "name";
    String projectId = "google.com:project";
    String stagePrefix = "some-prefix";
    boolean skipStagingPart = false;
    ImmutableMap<String, String> testCases =
        ImmutableMap.<String, String>builder()
            .put("", "gcr.io/google.com/project/some-prefix/name")
            .put("gcr.io", "gcr.io/google.com/project/some-prefix/name")
            .put("eu.gcr.io", "eu.gcr.io/google.com/project/some-prefix/name")
            .put(
                "us-docker.pkg.dev/other-project/other-repo",
                "us-docker.pkg.dev/other-project/other-repo/some-prefix/name")
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
                  containerName, projectId, null, key, stagePrefix, skipStagingPart));
        });
  }

  @Test
  public void testGenerateFlexTemplateImagePathSkipStagingPart() {
    String containerName = "name";
    String projectId = "some-project";
    String stagePrefix = "some-prefix";
    boolean skipStagingPart = true;
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
                  containerName, projectId, null, key, stagePrefix, skipStagingPart));
        });
  }
}
