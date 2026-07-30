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
 * PostgreSQL partitioned tables (List, Range, Hash).
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class PostgreSQLDatastreamToSpannerPartitionedIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(PostgreSQLDatastreamToSpannerPartitionedIT.class);

  private static final String POSTGRESQL_DDL_RESOURCE =
      "PostgreSQLPartitionedIT/postgresql-schema.sql";
  private static final String SPANNER_DDL_RESOURCE = "PostgreSQLPartitionedIT/spanner-schema.sql";
  private static final String PG_DIALECT_SPANNER_DDL_RESOURCE =
      "PostgreSQLPartitionedIT/pg-dialect-spanner-schema.sql";

  private static CloudPostgresResourceManager.ReplicationInfo replicationInfo;
  private static CloudPostgresResourceManager.ReplicationInfo pgDialectReplicationInfo;

  private static boolean initialized = false;
  private static CloudPostgresResourceManager postgresResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager pgDialectSpannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;

  private static HashSet<PostgreSQLDatastreamToSpannerPartitionedIT> testInstances =
      new HashSet<>();

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (PostgreSQLDatastreamToSpannerPartitionedIT.class) {
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

        // Alter publications to publish via partition root so Datastream sees the parent table
        postgresResourceManager.runSQLUpdate(
            "ALTER PUBLICATION "
                + replicationInfo.getPublicationName()
                + " SET (publish_via_partition_root = true);");
        postgresResourceManager.runSQLUpdate(
            "ALTER PUBLICATION "
                + pgDialectReplicationInfo.getPublicationName()
                + " SET (publish_via_partition_root = true);");

        initialized = true;
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    LOG.info("Cleaning up resources...");
    for (PostgreSQLDatastreamToSpannerPartitionedIT instance : testInstances) {
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
  public void testPostgreSqlPartitioned() throws Exception {
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
            "postgresql-partitioned",
            null,
            null,
            "postgresql-datastream-to-spanner-partitioned",
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
  public void testPostgreSqlPartitionedPGDialect() throws Exception {
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
            "postgresql-partitioned-pg-dialect",
            null,
            null,
            "postgresql-datastream-to-spanner-partitioned-pg-dialect",
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
      if (tableName.equals("employees_list")) {
        rows = resourceManager.readTableRecords(tableName, "id", "name", "department");
      } else if (tableName.equals("measurements_range")) {
        rows = resourceManager.readTableRecords(tableName, "id", "city_id", "logdate", "peaktemp");
      } else {
        rows = resourceManager.readTableRecords(tableName, "order_id", "customer_id", "amount");
      }

      for (Struct row : rows) {
        LOG.info("Found row: {}", row.toString());
      }
      SpannerAsserts.assertThatStructs(rows)
          .hasRecordsUnorderedCaseInsensitiveColumns(entry.getValue());
    }
  }

  private List<String> getAllowedTables() {
    return List.of("measurements_range", "employees_list", "orders_hash");
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

    // 1. Range Partitioning
    List<Map<String, Object>> measurementsRangeRows = new ArrayList<>();
    Map<String, Object> measurementRow1 = new HashMap<>();
    measurementRow1.put("id", 1L);
    measurementRow1.put("city_id", 1L);
    measurementRow1.put("logdate", com.google.cloud.Date.parseDate("2006-02-15"));
    measurementRow1.put("peaktemp", 33L);
    measurementsRangeRows.add(measurementRow1);

    Map<String, Object> measurementRow2 = new HashMap<>();
    measurementRow2.put("id", 2L);
    measurementRow2.put("city_id", 2L);
    measurementRow2.put("logdate", com.google.cloud.Date.parseDate("2006-03-15"));
    measurementRow2.put("peaktemp", 35L);
    measurementsRangeRows.add(measurementRow2);

    Map<String, Object> measurementRow3 = new HashMap<>();
    measurementRow3.put("id", 3L);
    measurementRow3.put("city_id", 3L);
    measurementRow3.put("logdate", com.google.cloud.Date.parseDate("2006-05-10"));
    measurementRow3.put("peaktemp", 40L);
    measurementsRangeRows.add(measurementRow3);

    Map<String, Object> measurementRow4 = new HashMap<>();
    measurementRow4.put("id", 4L);
    measurementRow4.put("city_id", 4L);
    measurementRow4.put("logdate", com.google.cloud.Date.parseDate("2006-02-20"));
    measurementRow4.put("peaktemp", 30L);
    measurementsRangeRows.add(measurementRow4);
    result.put("measurements_range", measurementsRangeRows);

    // 2. List Partitioning
    List<Map<String, Object>> employeesListRows = new ArrayList<>();
    Map<String, Object> empRow1 = new HashMap<>();
    empRow1.put("id", 1L);
    empRow1.put("name", "Alice");
    empRow1.put("department", "Engineering");
    employeesListRows.add(empRow1);

    Map<String, Object> empRow2 = new HashMap<>();
    empRow2.put("id", 2L);
    empRow2.put("name", "Bob");
    empRow2.put("department", "Sales");
    employeesListRows.add(empRow2);

    Map<String, Object> empRow3 = new HashMap<>();
    empRow3.put("id", 3L);
    empRow3.put("name", "Charlie");
    empRow3.put("department", "Marketing");
    employeesListRows.add(empRow3);
    result.put("employees_list", employeesListRows);

    // 3. Hash Partitioning
    List<Map<String, Object>> ordersHashRows = new ArrayList<>();
    Map<String, Object> orderRow1 = new HashMap<>();
    orderRow1.put("order_id", 1L);
    orderRow1.put("customer_id", 101L);
    orderRow1.put("amount", 500L);
    ordersHashRows.add(orderRow1);

    Map<String, Object> orderRow2 = new HashMap<>();
    orderRow2.put("order_id", 2L);
    orderRow2.put("customer_id", 102L);
    orderRow2.put("amount", 600L);
    ordersHashRows.add(orderRow2);

    Map<String, Object> orderRow3 = new HashMap<>();
    orderRow3.put("order_id", 3L);
    orderRow3.put("customer_id", 103L);
    orderRow3.put("amount", 700L);
    ordersHashRows.add(orderRow3);
    result.put("orders_hash", ordersHashRows);

    return result;
  }
}
