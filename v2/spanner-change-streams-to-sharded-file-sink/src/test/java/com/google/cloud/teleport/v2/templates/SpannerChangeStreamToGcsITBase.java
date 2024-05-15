/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.beam.it.common.utils.IORedirectUtil;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for SpannerChangeStreamToGcs integration tests. It provides helper functions related
 * to environment setup.
 */
public class SpannerChangeStreamToGcsITBase extends TemplateTestBase {

  public static String spannerDatabaseName = "";
  public static String spannerMetadataDatabaseName = "";
  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreamToGcsITBase.class);

  public GcsResourceManager createGcsResourceManager(String identifierSuffix) {
    return GcsResourceManager.builder(artifactBucketName, identifierSuffix, credentials)
        .build(); // DB name is appended with prefix to avoid clashes
  }

  public SpannerResourceManager createSpannerResourceManager() {
    return SpannerResourceManager.builder("rr-main-" + testName, PROJECT, REGION)
        .maybeUseStaticInstance()
        .build();
  }

  public void createSpannerDatabase(
      SpannerResourceManager spannerResourceManager, String spannerDdl) throws IOException {
    String ddl =
        String.join(
            " ", Resources.readLines(Resources.getResource(spannerDdl), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    String[] ddls = ddl.split(";");
    for (String d : ddls) {
      if (!d.isBlank()) {
        spannerResourceManager.executeDdlStatement(d);
      }
    }
    spannerDatabaseName = spannerResourceManager.getDatabaseId();
  }

  public void uploadSessionFileToGcs(
      GcsResourceManager gcsResourceManager, String sessionFileResourceName) throws IOException {
    gcsResourceManager.uploadArtifact(
        "input/session.json", Resources.getResource(sessionFileResourceName).getPath());
  }

  public SpannerResourceManager createSpannerMetadataResourceManager() {
    return SpannerResourceManager.builder("rr-meta-" + testName, PROJECT, REGION)
        .maybeUseStaticInstance()
        .build();
  }

  public void createSpannerMetadataDatabase(SpannerResourceManager spannerMetadataResourceManager) {
    String dummy = "create table t1(id INT64 ) primary key(id)";
    spannerMetadataResourceManager.executeDdlStatement(dummy);
    // needed to create separate metadata database
    spannerMetadataDatabaseName = spannerMetadataResourceManager.getDatabaseId();
  }

  public void createAndUploadJarToGcs(GcsResourceManager gcsResourceManager)
      throws IOException, InterruptedException {
    String[] commands = {"cd ../spanner-custom-shard", "mvn install"};

    // Join the commands with && to execute them sequentially
    String[] shellCommand = {"/bin/bash", "-c", String.join(" && ", commands)};

    Process exec = Runtime.getRuntime().exec(shellCommand);

    IORedirectUtil.redirectLinesLog(exec.getInputStream(), LOG);
    IORedirectUtil.redirectLinesLog(exec.getErrorStream(), LOG);

    if (exec.waitFor() != 0) {
      throw new RuntimeException("Error staging template, check Maven logs.");
    }
    gcsResourceManager.uploadArtifact(
        "input/customShard.jar",
        "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar");
  }

  public String getGcsFullPath(
      GcsResourceManager gcsResourceManager, String artifactId, String identifierSuffix) {
    return ArtifactUtils.getFullGcsPath(
        artifactBucketName, getClass().getSimpleName(), gcsResourceManager.runId(), artifactId);
  }
}
