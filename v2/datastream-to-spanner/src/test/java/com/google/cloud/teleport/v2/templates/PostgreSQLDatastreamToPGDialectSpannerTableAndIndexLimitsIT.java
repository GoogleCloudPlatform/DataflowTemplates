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

import static com.google.common.truth.Truth.assertThat;
import static java.util.Map.entry;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An integration test for {@link DataStreamToSpanner} Flex template which verifies the table and
 * index data limits when migrating from a PostgreSQL database.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class PostgreSQLDatastreamToPGDialectSpannerTableAndIndexLimitsIT
    extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(PostgreSQLDatastreamToPGDialectSpannerTableAndIndexLimitsIT.class);

  private static final String POSTGRESQL_DDL_RESOURCE =
      "PostgreSQLTableAndIndexLimitsIT/postgresql-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "PostgreSQLTableAndIndexLimitsIT/pg-dialect-spanner-schema.sql";

  private static final String LARGE_PK_TABLE = "large_pk_table";
  private static final String LARGE_IDX_TABLE = "large_idx_table";
  private static final String LARGE_CELL_TABLE = "large_cell_table";
  private static final String WIDE_ROW_TABLE = "wide_row";
  private static final List<String> TABLES =
      List.of(LARGE_PK_TABLE, LARGE_IDX_TABLE, LARGE_CELL_TABLE, WIDE_ROW_TABLE);

  private static CloudPostgresResourceManager postgresResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;
  private static HashSet<PostgreSQLDatastreamToPGDialectSpannerTableAndIndexLimitsIT>
      testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  private static CloudPostgresResourceManager.ReplicationInfo replicationInfo;

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (PostgreSQLDatastreamToPGDialectSpannerTableAndIndexLimitsIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        LOG.info("Setting up PostgreSQL resource manager...");
        postgresResourceManager = CloudPostgresResourceManager.builder(testName).build();
        LOG.info(
            "PostgreSQL resource manager created with URI: {}", postgresResourceManager.getUri());
        LOG.info("Executing PostgreSQL DDL script...");
        executeSqlScript(postgresResourceManager, POSTGRESQL_DDL_RESOURCE);

        LOG.info("Setting up Spanner resource manager...");
        spannerResourceManager = setUpPGDialectSpannerResourceManager();
        LOG.info(
            "Spanner resource manager created with instance ID: {}",
            spannerResourceManager.getInstanceId());

        LOG.info("Creating Spanner DDL...");
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

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
                .setPrivateConnectivity(getPrivateConnectivity())
                .build();
        LOG.info("Datastream resource manager created");

        replicationInfo = postgresResourceManager.createLogicalReplication();

        PostgresqlSource postgresqlSource =
            PostgresqlSource.builder(
                    postgresResourceManager.getHost(),
                    postgresResourceManager.getUsername(),
                    postgresResourceManager.getPassword(),
                    postgresResourceManager.getPort(),
                    postgresResourceManager.getDatabaseName(),
                    replicationInfo.getReplicationSlotName(),
                    replicationInfo.getPublicationName())
                .setAllowedTables(Map.of("public", TABLES))
                .build();

        LOG.info("Launching Dataflow job...");
        jobInfo =
            launchDataflowJob(
                "postgresql-table-and-index-limits-pg-dialect",
                null,
                null,
                "datastream-to-spanner-table-and-index-limits-pg-dialect",
                spannerResourceManager,
                pubsubResourceManager,
                new HashMap<>(),
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                null,
                postgresqlSource);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    LOG.info("Cleaning up resources...");
    for (PostgreSQLDatastreamToPGDialectSpannerTableAndIndexLimitsIT instance : testInstances) {
      instance.tearDownBase();
    }

    // It is important to clean up Datastream before trying to drop the replication slot.
    ResourceManagerUtils.cleanResources(datastreamResourceManager);

    ResourceManagerUtils.cleanResources(
        postgresResourceManager, spannerResourceManager, gcsResourceManager, pubsubResourceManager);
  }

  @Test
  public void testPrimaryKeySize() {
    assertThatPipeline(jobInfo).isRunning();

    LOG.info("Waiting for pipeline to process data for key size test...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                SpannerRowsCheck.builder(spannerResourceManager, LARGE_PK_TABLE)
                    .setMinRows(1)
                    .build());
    assertThatResult(result).meetsConditions();
    List<Struct> rows = null;
    try {
      rows =
          spannerResourceManager.readTableRecords(
              LARGE_PK_TABLE, List.of("pk_col1", "pk_col2", "pk_col3", "value_col"));
    } catch (Exception e) {
      LOG.error("Exception while reading spanner rows from {}", LARGE_PK_TABLE, e);
      throw e;
    }

    rows.forEach(
        row -> {
          assertThat(row.getString("pk_col1")).isEqualTo("A".repeat(4092));
          assertThat(row.getString("pk_col2")).isEqualTo("B".repeat(4092));
          assertThat(row.getString("pk_col3")).isEqualTo("C".repeat(8));
          assertThat(row.getString("value_col"))
              .isEqualTo("Primary key with size exactly equal to 8192 bytes");
        });
  }

  @Test
  public void testIndexSize() {
    assertThatPipeline(jobInfo).isRunning();

    LOG.info("Waiting for pipeline to process data for key size test...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                SpannerRowsCheck.builder(spannerResourceManager, LARGE_IDX_TABLE)
                    .setMinRows(1)
                    .build());
    assertThatResult(result).meetsConditions();
    List<Struct> rows = null;
    try {
      rows =
          spannerResourceManager.readTableRecords(
              LARGE_IDX_TABLE, List.of("pk_col", "idx_col1", "idx_col2", "value_col"));
    } catch (Exception e) {
      LOG.error("Exception while reading spanner rows from {}", LARGE_IDX_TABLE, e);
      throw e;
    }

    rows.forEach(
        row -> {
          assertThat(row.getLong("pk_col")).isEqualTo(1L);
          assertThat(row.getString("idx_col1")).isEqualTo("A".repeat(4091));
          assertThat(row.getString("idx_col2")).isEqualTo("B".repeat(4091));
          assertThat(row.getString("value_col"))
              .isEqualTo(
                  "Index key with size less than or equal to 8192 bytes (including PK size)");
        });
  }

  @Test
  public void testCellSize() {
    assertThatPipeline(jobInfo).isRunning();

    LOG.info("Waiting for pipeline to process data for cell size test...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                SpannerRowsCheck.builder(spannerResourceManager, LARGE_CELL_TABLE)
                    .setMinRows(1)
                    .build());
    assertThatResult(result).meetsConditions();
    List<Struct> rows = null;
    try {
      rows =
          spannerResourceManager.readTableRecords(
              LARGE_CELL_TABLE, List.of("id", "max_string_col_to_bytes", "max_string_col_to_str"));
    } catch (Exception e) {
      LOG.error("Exception while reading spanner rows from {}", LARGE_CELL_TABLE, e);
      throw e;
    }
    rows.forEach(
        row -> {
          assertThat(row.getLong("id")).isEqualTo(1L);
          assertThat(
              row.getBytes("max_string_col_to_bytes")
                  .equals("a".repeat(10485760).getBytes(StandardCharsets.UTF_8)));
          assertThat(row.getString("max_string_col_to_str")).isEqualTo("a".repeat(2621440));
        });
  }

  @Test
  @Ignore("Causes OOMs with low-provisioned VMs/DBs")
  public void testRowSize() {
    assertThatPipeline(jobInfo).isRunning();

    insertMaxRowSizeData();

    LOG.info("Waiting for pipeline to process data for row size test...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                SpannerRowsCheck.builder(spannerResourceManager, WIDE_ROW_TABLE)
                    .setMinRows(1)
                    .build());
    assertThatResult(result).meetsConditions();
    List<Struct> rows = null;
    try {
      rows = spannerResourceManager.readTableRecords(WIDE_ROW_TABLE, List.of("id"));
    } catch (Exception e) {
      LOG.error("Exception while reading spanner rows from {}", WIDE_ROW_TABLE, e);
      throw e;
    }
    SpannerAsserts.assertThatStructs(rows)
        .hasRecordsUnorderedCaseInsensitiveColumns(List.of(Map.ofEntries(entry("id", 1))));
  }

  private void insertMaxRowSizeData() {
    postgresResourceManager.runSQLUpdate(
        String.format(
            "INSERT INTO %s VALUES (1%s);", WIDE_ROW_TABLE, ", REPEAT('a', 10485760)".repeat(160)));
    postgresResourceManager.runSQLUpdate(
        String.format(
            "INSERT INTO %s VALUES (2%s);", WIDE_ROW_TABLE, ", REPEAT('a', 11534336)".repeat(160)));
  }
}
