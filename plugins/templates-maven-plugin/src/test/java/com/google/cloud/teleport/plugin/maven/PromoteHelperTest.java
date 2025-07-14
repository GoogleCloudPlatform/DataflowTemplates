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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PromoteHelperTest {
  @Test
  public void testPromoteFlexTemplateImage() {
    PromoteHelper helper =
        new PromoteHelper(
            "us-docker.pkg.dev/source-project/source-repo",
            "us-docker.pkg.dev/target-project/target-repo/2020_10_10_rc00/io_to_io",
            "sha256:123",
            "fake-token");
    String[] cmds = helper.getPromoteFlexTemplateImageCmd();
    String cmd = String.join(" ", cmds);
    assertEquals(
        "wget -O- --content-on-error --header=Authorization: Bearer fake-token "
            + "--header=Content-Type: application/json "
            + "--post-data={\"source_repository\":\"projects/source-project/locations/us/repositories/source-repo\","
            + "\"source_version\":\"projects/source-project/locations/us/repositories/source-repo/packages/2020_10_10_rc00%2Fio_to_io/"
            + "versions/sha256:123\",\"attachment_behavior\":\"EXCLUDE\"} https://artifactregistry.googleapis.com/v1/"
            + "projects/target-project/locations/us/repositories/target-repo:promoteArtifact",
        cmd);
  }

  @Test
  public void testArtifactRegImageSpec() {
    ImmutableMap<String, PromoteHelper.ArtifactRegImageSpec> testCase =
        ImmutableMap.<String, PromoteHelper.ArtifactRegImageSpec>builder()
            .put(
                "gcr.io/some-project",
                new PromoteHelper.ArtifactRegImageSpec("some-project", "gcr.io", "us", null))
            .put(
                "us.gcr.io/some-project",
                new PromoteHelper.ArtifactRegImageSpec("some-project", "us.gcr.io", "us", null))
            .put(
                "gcr.io/google.com/some-project",
                new PromoteHelper.ArtifactRegImageSpec(
                    "google.com:some-project", "gcr.io", "us", null))
            .put(
                "gcr.io/some-project/some-image",
                new PromoteHelper.ArtifactRegImageSpec(
                    "some-project", "gcr.io", "us", "some-image"))
            .put(
                "gcr.io/google.com/with-domain/some-image",
                new PromoteHelper.ArtifactRegImageSpec(
                    "google.com:with-domain", "gcr.io", "us", "some-image"))
            .put(
                "eu.gcr.io/some-project/some/image",
                new PromoteHelper.ArtifactRegImageSpec(
                    "some-project", "eu.gcr.io", "eu", "some/image"))
            .put(
                "eu-docker.pkg.dev/some-project/some-repo/some-image",
                new PromoteHelper.ArtifactRegImageSpec(
                    "some-project", "some-repo", "eu", "some-image"))
            .build();
    testCase.forEach(
        (key, value) -> {
          assertEquals(value, new PromoteHelper.ArtifactRegImageSpec(key));
        });
  }

  @Test
  public void testRunCommand() throws IOException, InterruptedException {
    String[] cmd = new String[] {"echo", "abc", "\"123\"", "'456'"};
    String out = TemplatesStageMojo.runCommandCapturesOutput(cmd, null);
    assertEquals("abc \"123\" '456'\n", out);
  }
}
