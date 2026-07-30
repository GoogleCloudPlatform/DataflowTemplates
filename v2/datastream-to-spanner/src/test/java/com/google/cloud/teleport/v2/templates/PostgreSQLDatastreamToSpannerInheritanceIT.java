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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudPostgresResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.PostgresqlSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An integration test for {@link DataStreamToSpanner} Flex template which tests migration of
 * PostgreSQL inheritance tables.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class PostgreSQLDatastreamToSpannerInheritanceIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(PostgreSQLDatastreamToSpannerInheritanceIT.class);

  private static final String POSTGRESQL_DDL_RESOURCE =
      "PostgreSQLInheritanceIT/postgresql-schema.sql";
  private static final String SPANNER_DDL_RESOURCE = "PostgreSQLInheritanceIT/spanner-schema.sql";
  private static final String PG_DIALECT_SPANNER_DDL_RESOURCE =
      "PostgreSQLInheritanceIT/pg-dialect-spanner-schema.sql";

  private static CloudPostgresResourceManager.ReplicationInfo replicationInfo;
  private static CloudPostgresResourceManager.ReplicationInfo pgDialectReplicationInfo;

  private static boolean initialized = false;
  private static CloudPostgresResourceManager postgresResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager pgDialectSpannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;

  private static HashSet<PostgreSQLDatastreamToSpannerInheritanceIT> testInstances =
      new HashSet<>();

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (PostgreSQLDatastreamToSpannerInheritanceIT.class) {
      testInstances.add(this);
      if (!initialized) {
        LOG.info("Setting up PostgreSQL resource manager...");
        postgresResourceManager = CloudPostgresResourceManager.builder(testName).build();
        LOG.info(
            "PostgreSQL resource manager created with URI: {}", postgresResourceManager.getUri());
        LOG.info("Setting up Spanner resource manager...");
        spannerResourceManager = setUpSpannerResourceManager();
        LOG.info(
            "Spanner resource manager created with instance ID: {}",
            spannerResourceManager.getInstanceId());
        LOG.info("Setting up PG dialect Spanner resource manager...");
        pgDialectSpannerResourceManager = setUpPGDialectSpannerResourceManager();
        LOG.info(
            "PG dialect Spanner resource manager created with instance ID: {}",
            pgDialectSpannerResourceManager.getInstanceId());
        LOG.info("Setting up GCS resource manager...");
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        LOG.info("GCS resource manager created with bucket: {}", gcsResourceManager.getBucket());
        LOG.info("Setting up Pub/Sub resource manager...");
        pubsubResourceManager = setUpPubSubResourceManager();
        LOG.info("Pub/Sub resource manager created.");
        LOG.info("Setting up Datastream resource manager...");
        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity("datastream-connect-2")
                .build();
        LOG.info("Datastream resource manager created");

        LOG.info("Executing PostgreSQL DDL script...");
        executeSqlScript(postgresResourceManager, POSTGRESQL_DDL_RESOURCE);

        replicationInfo = postgresResourceManager.createLogicalReplication();
        pgDialectReplicationInfo = postgresResourceManager.createLogicalReplication();

        initialized = true;
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    LOG.info("Cleaning up resources...");
    for (PostgreSQLDatastreamToSpannerInheritanceIT instance : testInstances) {
      try {
        instance.tearDownBase();
      } catch (Exception e) {
        LOG.error("Failed to tear down base for instance: {}", instance, e);
      }
    }

    // It is important to clean up Datastream before trying to drop the replication slot.
    ResourceManagerUtils.cleanResources(
        datastreamResourceManager,
        postgresResourceManager,
        spannerResourceManager,
        pgDialectSpannerResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testPostgreSqlInheritance() throws Exception {
    LOG.info("Creating Spanner DDL...");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    PostgresqlSource postgresqlSource =
        PostgresqlSource.builder(
                postgresResourceManager.getHost(),
                postgresResourceManager.getUsername(),
                postgresResourceManager.getPassword(),
                postgresResourceManager.getPort(),
                postgresResourceManager.getDatabaseName(),
                replicationInfo.getReplicationSlotName(),
                replicationInfo.getPublicationName())
            .setAllowedTables(Map.of("public", getAllowedTables()))
            .build();

    LOG.info("Launching Dataflow job...");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            "postgresql-inheritance",
            null,
            null,
            "postgresql-datastream-to-spanner-inheritance",
            spannerResourceManager,
            pubsubResourceManager,
            new HashMap<>(),
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            null,
            postgresqlSource);
    assertThatPipeline(jobInfo).isRunning();

    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();

    ConditionCheck condition = buildConditionCheck(spannerResourceManager, expectedData);
    LOG.info("Waiting for pipeline to process data...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(20)), condition);
    assertThatResult(result).meetsConditions();

    validateResult(spannerResourceManager, expectedData);
  }

  @Test
  public void testPostgreSqlInheritancePGDialect() throws Exception {
    LOG.info("Creating PG Dialect Spanner DDL...");
    createSpannerDDL(pgDialectSpannerResourceManager, PG_DIALECT_SPANNER_DDL_RESOURCE);

    PostgresqlSource postgresqlSource =
        PostgresqlSource.builder(
                postgresResourceManager.getHost(),
                postgresResourceManager.getUsername(),
                postgresResourceManager.getPassword(),
                postgresResourceManager.getPort(),
                postgresResourceManager.getDatabaseName(),
                pgDialectReplicationInfo.getReplicationSlotName(),
                pgDialectReplicationInfo.getPublicationName())
            .setAllowedTables(Map.of("public", getAllowedTables()))
            .build();

    LOG.info("Launching Dataflow job...");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            "postgresql-inheritance-pg-dialect",
            null,
            null,
            "postgresql-datastream-to-spanner-inheritance-pg-dialect",
            pgDialectSpannerResourceManager,
            pubsubResourceManager,
            new HashMap<>(),
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            null,
            postgresqlSource);
    assertThatPipeline(jobInfo).isRunning();

    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();

    ConditionCheck condition = buildConditionCheck(pgDialectSpannerResourceManager, expectedData);
    LOG.info("Waiting for pipeline to process data...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(20)), condition);
    assertThatResult(result).meetsConditions();

    validateResult(pgDialectSpannerResourceManager, expectedData);
  }

  private void validateResult(
      SpannerResourceManager resourceManager, Map<String, List<Map<String, Object>>> expectedData) {
    for (Map.Entry<String, List<Map<String, Object>>> entry : expectedData.entrySet()) {
      String tableName = entry.getKey();
      LOG.info("Asserting table: {}", tableName);

      // Read all columns from the table to assert
      List<Struct> rows;
      if (tableName.equals("parent_table")) {
        rows = resourceManager.readTableRecords(tableName, "id", "name");
      } else if (tableName.equals("child_table")) {
        rows = resourceManager.readTableRecords(tableName, "id", "name", "age");
      } else {
        rows = resourceManager.readTableRecords(tableName, "id", "name", "age", "city");
      }

      for (Struct row : rows) {
        LOG.info("Found row: {}", row.toString());
      }
      SpannerAsserts.assertThatStructs(rows)
          .hasRecordsUnorderedCaseInsensitiveColumns(entry.getValue());
    }
  }

  private List<String> getAllowedTables() {
    return List.of("parent_table", "child_table", "grandchild_table");
  }

  private ConditionCheck buildConditionCheck(
      SpannerResourceManager resourceManager, Map<String, List<Map<String, Object>>> expectedData) {

    ConditionCheck combinedCondition = null;
    for (Map.Entry<String, List<Map<String, Object>>> entry : expectedData.entrySet()) {
      String tableName = entry.getKey();
      int numRows = entry.getValue().size();
      ConditionCheck c =
          SpannerRowsCheck.builder(resourceManager, tableName).setMinRows(numRows).build();
      if (combinedCondition == null) {
        combinedCondition = c;
      } else {
        combinedCondition = combinedCondition.and(c);
      }
    }
    return combinedCondition;
  }

  private Map<String, List<Map<String, Object>>> getExpectedData() {
    HashMap<String, List<Map<String, Object>>> result = new HashMap<>();

    // According to PostgreSQL logical replication, inserts to child_table replicate as child_table
    // events.
    // The parent_table only receives its own events. So the tables are independent in replication.

    List<Map<String, Object>> parentRows = new ArrayList<>();
    Map<String, Object> parentRow1 = new HashMap<>();
    parentRow1.put("id", 1L);
    parentRow1.put("name", "Parent Row 1");
    parentRows.add(parentRow1);
    result.put("parent_table", parentRows);

    List<Map<String, Object>> childRows = new ArrayList<>();
    Map<String, Object> childRow1 = new HashMap<>();
    childRow1.put("id", 2L);
    childRow1.put("name", "Child Row 1");
    childRow1.put("age", 10L);
    childRows.add(childRow1);
    result.put("child_table", childRows);

    List<Map<String, Object>> grandchildRows = new ArrayList<>();
    Map<String, Object> grandchildRow1 = new HashMap<>();
    grandchildRow1.put("id", 3L);
    grandchildRow1.put("name", "Grandchild Row 1");
    grandchildRow1.put("age", 5L);
    grandchildRow1.put("city", "New York");
    grandchildRows.add(grandchildRow1);
    result.put("grandchild_table", grandchildRows);

    return result;
  }
}
