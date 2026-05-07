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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudSqlShardOrchestrator;
import org.apache.beam.it.gcp.cloudsql.CloudSqlShardOrchestrator.DatabaseType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Step 1 Validation / Setup-Only test for {@link SpannerToSourceDb} template.
 *
 * <p>Objective: Verify initial setup, DDL, schemas, GCS artifacts, and CloudSQL connectivity before
 * launching the massive backlog load test.
 *
 * <p>This setup utilizes the programmatic {@link CloudSqlShardOrchestrator} to dynamically
 * provision and manage physical instances over Private IPs inside the target VPC, completely
 * bypassing proxy requirements.
 */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDbBacklogStepLT extends SpannerToSourceDbLTBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDbBacklogStepLT.class);

  private final String spannerDdlResource = "SpannerToSourceDbBacklogLT/spanner-schema.sql";
  private final String sessionFileResource = "SpannerToSourceDbBacklogLT/session.json";

  private CloudSqlShardOrchestrator orchestrator;

  @Before
  public void setup() throws IOException {
    LOG.info(
        "Initializing resource managers for Step 1 Setup & Connectivity validation via Orchestrator...");

    String password = System.getProperty("cloudProxyPassword", "Welcome@1");
    System.setProperty("cloudProxyPassword", password);

    // Setup Spanner database and metadata database, GCS artifact resource manager, and session
    // files
    setupResourceManagers(spannerDdlResource, sessionFileResource);

    // Initialize the Cloud SQL Shard Orchestrator for dynamic GCP-level provisioning over Private
    // IP
    orchestrator =
        new CloudSqlShardOrchestrator(
            DatabaseType.MYSQL,
            CloudSqlShardOrchestrator.MYSQL_8_0,
            project,
            region,
            gcsResourceManager);

    Map<String, List<String>> shardMap = new HashMap<>();
    shardMap.put("nokill-high-resources-backlog-shard1", List.of("shard0", "shard1"));
    shardMap.put("nokill-high-resources-backlog-shard2", List.of("shard2", "shard3"));

    // Initialize the physical instances (reusing existing ones) and logical schemas
    orchestrator.initialize(shardMap, "orchestrator_shards_bulk.json");

    // Create logical table schemas inside each database shard
    LOG.info("Creating logical schemas on MySQL shards...");
    CloudSqlResourceManager manager1 =
        (CloudSqlResourceManager) orchestrator.managers.get("nokill-high-resources-backlog-shard1");
    CloudSqlResourceManager manager2 =
        (CloudSqlResourceManager) orchestrator.managers.get("nokill-high-resources-backlog-shard2");
    createLogicalTableSchema(manager1, "shard0");
    createLogicalTableSchema(manager1, "shard1");
    createLogicalTableSchema(manager2, "shard2");
    createLogicalTableSchema(manager2, "shard3");

    // Upload sharding configuration in the flat format expected by SpannerToSourceDb
    LOG.info("Generating and uploading flat sharding configuration to GCS...");
    createAndUploadShardConfigToGcs();
  }

  @After
  public void tearDown() {
    LOG.info("Cleaning up resources...");
    cleanupResourceManagers();
    if (orchestrator != null) {
      orchestrator.cleanup();
    }
  }

  @Test
  public void test1_setupAndConnectivitySanity() throws IOException {
    LOG.info("Running Step 1 Setup and Connectivity Sanity check...");

    // 1. Verify Spanner Connectivity and Table DDL
    LOG.info("Verifying Spanner database connectivity and schema...");
    assertNotNull("Spanner resource manager should be initialized", spannerResourceManager);

    String testId = "test-id-12345";
    String testPayload = "test-payload-step1";
    String testShardId = "shard_0";

    // Write a row directly to Spanner
    LOG.info("Writing a test row directly to Spanner...");
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertOrUpdateBuilder("MigrationLoadTest")
            .set("Id")
            .to(testId)
            .set("Payload")
            .to(testPayload)
            .set("migration_shard_id")
            .to(testShardId)
            .build());
    spannerResourceManager.write(mutations);

    // Read the row back from Spanner to verify
    LOG.info("Reading the test row back from Spanner...");
    List<Struct> results =
        spannerResourceManager.runQuery(
            String.format(
                "SELECT Payload FROM MigrationLoadTest WHERE migration_shard_id = '%s' AND Id = '%s'",
                testShardId, testId));
    assertNotNull("Results from Spanner should not be null", results);
    assertEquals("Should return exactly 1 row", 1, results.size());
    assertEquals(
        "Payload matches what was written to Spanner",
        testPayload,
        results.get(0).getString("Payload"));

    // Delete test row from Spanner
    LOG.info("Deleting test row from Spanner...");
    spannerResourceManager.write(
        List.of(
            Mutation.delete(
                "MigrationLoadTest", com.google.cloud.spanner.Key.of(testShardId, testId))));

    // 2. Verify GCS Artifacts (Session and Sharding configurations)
    LOG.info("Verifying GCS configuration artifacts...");
    assertNotNull("GCS resource manager should be initialized", gcsResourceManager);

    String sessionGcsPath = getGcsPath(SESSION_FILE_NAME, gcsResourceManager);
    LOG.info("Session file GCS Path: {}", sessionGcsPath);
    assertTrue("Session file should exist on GCS", sessionGcsPath.startsWith("gs://"));

    String shardGcsPath = getGcsPath(SOURCE_SHARDS_FILE_NAME, gcsResourceManager);
    LOG.info("Shard file GCS Path: {}", shardGcsPath);
    assertTrue("Shard file should exist on GCS", shardGcsPath.startsWith("gs://"));

    // 3. Verify MySQL Connectivity, DDL, and Shards
    LOG.info("Verifying CloudSQL MySQL Shard connectivity and DDL...");
    CloudSqlResourceManager manager1 =
        (CloudSqlResourceManager) orchestrator.managers.get("nokill-high-resources-backlog-shard1");
    CloudSqlResourceManager manager2 =
        (CloudSqlResourceManager) orchestrator.managers.get("nokill-high-resources-backlog-shard2");
    assertNotNull("Shard 1 resource manager should be initialized", manager1);
    assertNotNull("Shard 2 resource manager should be initialized", manager2);

    // Write and read from logical database shard0 (on Shard 1 physical instance)
    verifyMySqlLogicalShard(manager1, "shard0");
    // Write and read from logical database shard1 (on Shard 1 physical instance)
    verifyMySqlLogicalShard(manager1, "shard1");
    // Write and read from logical database shard2 (on Shard 2 physical instance)
    verifyMySqlLogicalShard(manager2, "shard2");
    // Write and read from logical database shard3 (on Shard 2 physical instance)
    verifyMySqlLogicalShard(manager2, "shard3");

    LOG.info("Step 1 Setup and Connectivity Sanity check passed successfully! All systems are GO.");
  }

  private void verifyMySqlLogicalShard(CloudSqlResourceManager manager, String dbName) {
    LOG.info("Verifying logical database: {}...", dbName);

    String testId = "test-id-" + dbName;
    String testPayload = "payload-" + dbName;

    // Insert test row
    String insertSql =
        String.format(
            "INSERT INTO %s.MigrationLoadTest (Id, Payload) VALUES ('%s', '%s')",
            dbName, testId, testPayload);
    manager.runSQLUpdate(insertSql);

    // Query test row back
    String selectSql =
        String.format("SELECT Payload FROM %s.MigrationLoadTest WHERE Id = '%s'", dbName, testId);
    List<Map<String, Object>> result = manager.runSQLQuery(selectSql);

    assertNotNull("Result from MySQL logical shard " + dbName + " should not be null", result);
    assertEquals("Should return exactly 1 row", 1, result.size());
    assertEquals(
        "Payload matches what was written to " + dbName, testPayload, result.get(0).get("Payload"));

    // Cleanup test row
    String deleteSql =
        String.format("DELETE FROM %s.MigrationLoadTest WHERE Id = '%s'", dbName, testId);
    manager.runSQLUpdate(deleteSql);
    LOG.info("Logical database {} verified successfully.", dbName);
  }

  private void createLogicalTableSchema(CloudSqlResourceManager manager, String dbName) {
    manager.runSQLUpdate(
        "CREATE TABLE IF NOT EXISTS "
            + dbName
            + ".MigrationLoadTest ("
            + "Id VARCHAR(36) NOT NULL,"
            + "Payload LONGTEXT NOT NULL,"
            + "PRIMARY KEY (Id)"
            + ") ENGINE=InnoDB");
  }

  private void createAndUploadShardConfigToGcs() throws IOException {
    CloudSqlResourceManager manager1 =
        (CloudSqlResourceManager) orchestrator.managers.get("nokill-high-resources-backlog-shard1");
    CloudSqlResourceManager manager2 =
        (CloudSqlResourceManager) orchestrator.managers.get("nokill-high-resources-backlog-shard2");

    JsonArray ja = new JsonArray();
    ja.add(createShardConfig("shard_0", "shard0", manager1));
    ja.add(createShardConfig("shard_1", "shard1", manager1));
    ja.add(createShardConfig("shard_2", "shard2", manager2));
    ja.add(createShardConfig("shard_3", "shard3", manager2));

    String shardFileContents = ja.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    gcsResourceManager.createArtifact(SOURCE_SHARDS_FILE_NAME, shardFileContents);
  }

  private JsonObject createShardConfig(
      String logicalShardId, String dbName, CloudSqlResourceManager manager) {
    Shard shard = new Shard();
    shard.setLogicalShardId(logicalShardId);
    shard.setUser(manager.getUsername());
    shard.setHost(manager.getHost());
    shard.setPassword(manager.getPassword());
    shard.setPort(String.valueOf(manager.getPort()));
    shard.setDbName(dbName);
    JsonObject jsObj = (JsonObject) new Gson().toJsonTree(shard).getAsJsonObject();
    jsObj.remove("secretManagerUri");
    return jsObj;
  }
}
