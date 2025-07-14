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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.beam.it.cassandra.CassandraResourceManager;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.storage.GcsResourceManager;

/**
 * Base class for Spanner to cassandra Load tests. It provides helper functions related to
 * environment setup and assertConditions.
 */
public class SpannerToCassandraLTBase extends SpannerToSourceDbLTBase {

  public CassandraResourceManager cassandraResourceManager;
  public static final String SOURCE_SHARDS_FILE_NAME = "input/cassandra-config.conf";

  public void setupResourceManagers(
      String spannerDdlResource, String cassandraDdlResource, String artifactBucket)
      throws IOException {
    spannerResourceManager = createSpannerDatabase(spannerDdlResource);
    spannerMetadataResourceManager = createSpannerMetadataDatabase();
    cassandraResourceManager = generateKeyspaceAndBuildCassandraResource();

    gcsResourceManager =
        GcsResourceManager.builder(artifactBucket, getClass().getSimpleName(), CREDENTIALS).build();
    createCassandraSchema(cassandraResourceManager, cassandraDdlResource);
    createAndUploadCassandraConfigToGcs(gcsResourceManager, cassandraResourceManager);
    pubsubResourceManager = setUpPubSubResourceManager();
    subscriptionName =
        createPubsubResources(
            getClass().getSimpleName(),
            pubsubResourceManager,
            getGcsPath(artifactBucket, "dlq", gcsResourceManager)
                .replace("gs://" + artifactBucket, ""));
  }

  protected CassandraResourceManager generateKeyspaceAndBuildCassandraResource() {
    /* The default is Cassandra 4.1 image. TODO: Explore testing with non 4.1 tags. */

    /* Max Cassandra Keyspace is 48 characters. Base Resource Manager adds 24 characters of date-time at the end.
     * That's why we need to take a smaller subsequence of the testName.
     */
    String uniqueId = testName.substring(0, Math.min(20, testName.length()));
    return CassandraResourceManager.builder(uniqueId).build();
  }

  public void cleanupResourceManagers() {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager,
        cassandraResourceManager);
  }

  public void createAndUploadCassandraConfigToGcs(
      GcsResourceManager gcsResourceManager, CassandraResourceManager cassandraResourceManagers)
      throws IOException {

    String host = cassandraResourceManagers.getHost();
    int port = cassandraResourceManagers.getPort();
    String keyspaceName = cassandraResourceManagers.getKeyspaceName();

    String cassandraConfigContents;
    try (InputStream inputStream =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("SpannerToCassandraSourceLT/cassandra-config-template.conf")) {
      if (inputStream == null) {
        throw new FileNotFoundException(
            "Resource file not found: SpannerToCassandraSourceLT/cassandra-config-template.conf");
      }
      cassandraConfigContents = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }

    cassandraConfigContents =
        cassandraConfigContents
            .replace("##host##", host)
            .replace("##port##", Integer.toString(port))
            .replace("##keyspace##", keyspaceName);

    gcsResourceManager.createArtifact(SOURCE_SHARDS_FILE_NAME, cassandraConfigContents);
  }

  public void createCassandraSchema(
      CassandraResourceManager cassandraResourceManager, String cassandraDdlResourceFile)
      throws IOException {
    String ddl =
        String.join(
            " ",
            Resources.readLines(
                Resources.getResource(cassandraDdlResourceFile), StandardCharsets.UTF_8));

    ddl = ddl.trim();
    String[] ddls = ddl.split(";");
    for (String d : ddls) {
      if (!d.isBlank()) {
        cassandraResourceManager.executeStatement(d);
      }
    }
  }
}
