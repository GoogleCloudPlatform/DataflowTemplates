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
package com.google.cloud.teleport.plugin.maven;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PromoteHelper {
  private static final Logger LOG = LoggerFactory.getLogger(PromoteHelper.class);
  private final ArtifactRegImageSpec sourceSpec;
  private final ArtifactRegImageSpec targetSpec;
  // needed for adding tag
  private final String targetPath;
  private final String sourceDigest;
  private final String token;

  /**
   * Promote the staged flex template image using MOSS promote API.
   *
   * @param sourcePath - spec for source image.
   * @param targetPath - spec for target image
   * @param sourceDigest - source image digest, e.g. sha256:xxxxx
   */
  public PromoteHelper(String sourcePath, String targetPath, String sourceDigest)
      throws IOException, InterruptedException {
    this(sourcePath, targetPath, sourceDigest, accessToken());
  }

  @VisibleForTesting
  PromoteHelper(String sourcePath, String targetPath, String sourceDigest, String token) {
    this.sourceSpec = new ArtifactRegImageSpec(sourcePath);
    this.targetSpec = new ArtifactRegImageSpec(targetPath);
    this.sourceDigest = sourceDigest;
    this.token = token;
    this.targetPath = targetPath;
  }

  /** Promote the artifact. */
  public void promote() throws IOException, InterruptedException {
    String[] promoteArtifactCmd = getPromoteFlexTemplateImageCmd();
    // promote API returns a long-running-operation
    String responseRLO = TemplatesStageMojo.runCommandCapturesOutput(promoteArtifactCmd, null);
    JsonElement parsed = JsonParser.parseString(responseRLO);
    String operation = parsed.getAsJsonObject().get("name").getAsString();
    waitForComplete(operation);
    addTag();
  }

  @VisibleForTesting
  String[] getPromoteFlexTemplateImageCmd() {
    Preconditions.checkNotNull(targetSpec.imageName, "Target image name can not be null");
    String authHeader = String.format("Authorization: Bearer %s", token);
    String contentTypeHeader = "Content-Type: application/json";
    String sourceRepo =
        String.format(
            "projects/%s/locations/%s/repositories/%s",
            sourceSpec.project, sourceSpec.location, sourceSpec.repository);
    String sourceVersion =
        String.format(
            "%s/packages/%s/versions/%s",
            sourceRepo,
            URLEncoder.encode(targetSpec.imageName, StandardCharsets.UTF_8),
            sourceDigest);
    String url =
        String.format(
            "https://artifactregistry.googleapis.com/v1/projects/%s/locations/%s/repositories/%s:promoteArtifact",
            targetSpec.project, targetSpec.location, targetSpec.repository);
    ImmutableMap<String, String> postDataCollect =
        ImmutableMap.<String, String>builder()
            .put("source_repository", sourceRepo)
            .put("source_version", sourceVersion)
            .put("attachment_behavior", "EXCLUDE")
            .build();
    String postData = new Gson().toJson(postDataCollect);
    return new String[] {
      "wget",
      "-O-",
      "--content-on-error",
      "--header=" + authHeader,
      "--header=" + contentTypeHeader,
      "--post-data=" + postData,
      url
    };
  }

  /** Wait for long-running operation to complete. */
  private void waitForComplete(String operation) {
    String[] command =
        new String[] {
          "wget",
          "-O-",
          "--content-on-error",
          String.format("--header=Authorization: Bearer %s", token),
          "https://artifactregistry.googleapis.com/v1/" + operation
        };

    RetryPolicy<?> retry =
        RetryPolicy.builder()
            .handleIf(throwable -> throwable instanceof QueryOperationRunnable.RetryableException)
            .withBackoff(Duration.ofSeconds(5), Duration.ofSeconds(30))
            .withMaxRetries(5)
            .build();

    QueryOperationRunnable runnable = new QueryOperationRunnable(command);
    Failsafe.with(retry).run(runnable);
  }

  /** Add "latest" tag after promotion. */
  private void addTag() throws IOException, InterruptedException {
    // TODO: remove this once copy tag is supported by promote API
    String[] command;
    if (targetSpec.repository.endsWith("gcr.io")) {
      // gcr.io repository needs to use `gcloud container` to add tag
      command =
          new String[] {
            "gcloud",
            "container",
            "images",
            "add-tag",
            "-q",
            String.format("%s@%s", targetPath, sourceDigest),
            String.format("%s:latest", targetPath)
          };
    } else {
      command =
          new String[] {
            "gcloud",
            "artifacts",
            "docker",
            "tags",
            "add",
            String.format("%s@%s", targetPath, sourceDigest),
            String.format("%s:latest", targetPath)
          };
    }
    TemplatesStageMojo.runCommandCapturesOutput(command, null);
  }

  private static class QueryOperationRunnable implements dev.failsafe.function.CheckedRunnable {
    String[] command;

    public QueryOperationRunnable(String[] command) {
      this.command = command;
    }

    @Override
    public void run() throws RetryableException, IOException, InterruptedException {
      String response = TemplatesStageMojo.runCommandCapturesOutput(command, null);
      JsonObject parsed = JsonParser.parseString(response).getAsJsonObject();
      if (parsed.get("done") == null || !parsed.get("done").getAsBoolean()) {
        throw new RetryableException("Operation not yet finished, will poll later.");
      }
      if (parsed.get("error") != null) {
        int errorCode = parsed.get("error").getAsJsonObject().get("code").getAsInt();
        if (errorCode == 6) {
          // already exist
          return;
        }
        throw new RuntimeException("Operation failed: " + response);
      }
    }

    public static class RetryableException extends Exception {
      public RetryableException(String message) {
        super(message);
      }
    }
  }

  private static String accessToken() throws IOException, InterruptedException {
    // do not use runCommand to avoid print token to log
    Process process =
        Runtime.getRuntime().exec(new String[] {"gcloud", "auth", "print-access-token"});
    if (process.waitFor() != 0) {
      throw new RuntimeException(
          "Error fetching access token for request: "
              + new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8));
    }
    return new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
  }

  /** Artifact registry image spec. */
  static class ArtifactRegImageSpec {
    public final String project;
    public final String repository;
    public final String location;

    public final @Nullable String imageName;

    /**
     * Construct an {@code ArtifactRegImageSpec} from an image url. Supported image urls include
     * [region.]gcr.io/projectId[/...] and region-docker.pkg.dev/projectId[/...]. See {@code
     * PromoteHelperTest#testArtifactRegImageSpec} for example image urls.
     */
    public ArtifactRegImageSpec(String imagePath) {
      String[] segments = imagePath.split("/", 3);
      if (segments.length < 3) {
        segments = new String[] {segments[0], segments[1], null};
      }
      if ("google.com".equals(segments[1])) {
        String[] repoSegments = segments[2].split("/", 2);
        segments =
            new String[] {
              segments[0],
              segments[1] + ":" + repoSegments[0],
              repoSegments.length < 2 ? null : repoSegments[1]
            };
      }
      this.project = segments[1];
      if (segments[0].endsWith("gcr.io")) {
        this.repository = segments[0];
        this.imageName = segments[2];
        if ("gcr.io".equals(segments[0])) {
          this.location = "us";
        } else {
          this.location = segments[0].substring(0, segments[0].length() - ".gcr.io".length());
        }
      } else if (segments[0].endsWith("-docker.pkg.dev")) {
        String[] repoSegments = segments[2].split("/", 2);
        this.repository = repoSegments[0];
        if (repoSegments.length > 1) {
          this.imageName = repoSegments[1];
        } else {
          this.imageName = null;
        }
        this.location = segments[0].substring(0, segments[0].length() - "-docker.pkg.dev".length());
      } else {
        throw new RuntimeException("Unsupported artifact registry image path: " + imagePath);
      }
    }

    public ArtifactRegImageSpec(
        String project, String repository, String location, @Nullable String imageName) {
      this.project = project;
      this.repository = repository;
      this.location = location;
      this.imageName = imageName;
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof ArtifactRegImageSpec t) {
        return project.equals(t.project)
            && repository.equals(t.repository)
            && location.equals(t.location)
            && Objects.equal(imageName, t.imageName);
      } else {
        return false;
      }
    }

    @Override
    public String toString() {
      return String.format(
          "ArtifactRegImageSpec(%s, %s, %s, %s)", project, repository, location, imageName);
    }
  }
}
