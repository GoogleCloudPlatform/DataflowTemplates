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

import static java.util.Map.entry;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.MySQLSource;
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
 * index data limits when migrating from a MySQL database.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
@Ignore("Temporarily disabled for maintenance")
public class MySQLDatastreamToPGDialectSpannerTableAndIndexLimitsIT
    extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(MySQLDatastreamToPGDialectSpannerTableAndIndexLimitsIT.class);

  private static final String MYSQL_DDL_RESOURCE = "MySQLTableAndIndexLimitsIT/mysql-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "MySQLTableAndIndexLimitsIT/pg-dialect-spanner-schema.sql";
  private static final String SESSION_FILE = "MySQLTableAndIndexLimitsIT/pg-dialect-session.json";

  private static final String LARGE_KEY_TABLE = "LargeKey";
  private static final String LARGE_CELL_TABLE = "LargeCell";
  private static final String WIDE_ROW_TABLE = "WideRow";
  private static final List<String> TABLES =
      List.of(LARGE_KEY_TABLE, LARGE_CELL_TABLE, WIDE_ROW_TABLE);

  private static CloudMySQLResourceManager mySQLResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;
  private static HashSet<MySQLDatastreamToPGDialectSpannerTableAndIndexLimitsIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (MySQLDatastreamToPGDialectSpannerTableAndIndexLimitsIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        LOG.info("Setting up MySQL resource manager...");
        mySQLResourceManager = CloudMySQLResourceManager.builder(testName).build();
        LOG.info("MySQL resource manager created with URI: {}", mySQLResourceManager.getUri());
        LOG.info("Setting up Spanner resource manager...");
        spannerResourceManager = setUpPGDialectSpannerResourceManager();
        LOG.info(
            "Spanner resource manager created with instance ID: {}",
            spannerResourceManager.getInstanceId());
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

        LOG.info("Executing MySQL DDL script...");
        executeSqlScript(mySQLResourceManager, MYSQL_DDL_RESOURCE);
        LOG.info("Creating Spanner DDL...");
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

        LOG.info("Generating session file content...");
        String sessionFileContent =
            generateSessionFile(
                1,
                mySQLResourceManager.getDatabaseName(),
                spannerResourceManager.getDatabaseId(),
                TABLES,
                SESSION_FILE);
        MySQLSource mySQLSource =
            MySQLSource.builder(
                    mySQLResourceManager.getHost(),
                    mySQLResourceManager.getUsername(),
                    mySQLResourceManager.getPassword(),
                    mySQLResourceManager.getPort())
                .setAllowedTables(Map.of(mySQLResourceManager.getDatabaseName(), TABLES))
                .build();

        LOG.info("Launching Dataflow job...");
        jobInfo =
            launchDataflowJob(
                "mysql-table-index-limits-pg-dial",
                null,
                null,
                "datastream-to-pg-dialect-spanner-table-and-index-limits",
                spannerResourceManager,
                pubsubResourceManager,
                Map.of("dlqMaxRetryCount", "1"),
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                sessionFileContent,
                mySQLSource);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    LOG.info("Cleaning up resources...");
    for (MySQLDatastreamToPGDialectSpannerTableAndIndexLimitsIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        mySQLResourceManager,
        spannerResourceManager,
        gcsResourceManager,
        pubsubResourceManager,
        datastreamResourceManager);
  }

  @Test
  public void testKeySize() {
    assertThatPipeline(jobInfo).isRunning();

    LOG.info("Waiting for pipeline to process data for key size test...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                SpannerRowsCheck.builder(
                        spannerResourceManager, String.format("\"%s\"", LARGE_KEY_TABLE))
                    .setMinRows(1)
                    .build());
    assertThatResult(result).meetsConditions();
    List<Struct> rows = null;
    try {
      rows =
          spannerResourceManager.readTableRecords(
              LARGE_KEY_TABLE,
              List.of("pk_col1", "pk_col2", "pk_col3", "col1", "col2", "col3", "value_col"));
    } catch (Exception e) {
      LOG.error("Exception while reading spanner rows from {}", LARGE_KEY_TABLE, e);
      throw e;
    }
    SpannerAsserts.assertThatStructs(rows)
        .hasRecordsUnorderedCaseInsensitiveColumns(
            List.of(
                Map.ofEntries(
                    // `assertThatStructs` converts the `rows` structs to records which eventually
                    // calls
                    // `com.google.cloud.spanner.Value.StringImpl::valueToString`, which truncates
                    // strings to 33 characters
                    entry("pk_col1", "A".repeat(33) + "..."),
                    entry("pk_col2", "B".repeat(33) + "..."),
                    entry("pk_col3", "C".repeat(33) + "..."),
                    entry("col1", "A".repeat(33) + "..."),
                    entry("col2", "B".repeat(33) + "..."),
                    entry("col3", "C".repeat(33) + "..."),
                    entry("value_col", "3072 bytes of total size of table..."))));
  }

  @Test
  public void testCellSize() {
    assertThatPipeline(jobInfo).isRunning();

    LOG.info("Waiting for pipeline to process data for cell size test...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                SpannerRowsCheck.builder(
                        spannerResourceManager, String.format("\"%s\"", LARGE_CELL_TABLE))
                    .setMinRows(1)
                    .build());
    assertThatResult(result).meetsConditions();
    List<Struct> rows = null;
    try {
      rows = spannerResourceManager.readTableRecords(LARGE_CELL_TABLE, List.of("id"));
    } catch (Exception e) {
      LOG.error("Exception while reading spanner rows from {}", LARGE_CELL_TABLE, e);
      throw e;
    }
    SpannerAsserts.assertThatStructs(rows)
        .hasRecordsUnorderedCaseInsensitiveColumns(List.of(Map.ofEntries(entry("id", 3))));
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
                SpannerRowsCheck.builder(
                        spannerResourceManager, String.format("\"%s\"", WIDE_ROW_TABLE))
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
    mySQLResourceManager.runSQLUpdate(
        String.format(
            "INSERT INTO %s VALUES (1%s);", WIDE_ROW_TABLE, ", REPEAT('a', 10485760)".repeat(160)));
    mySQLResourceManager.runSQLUpdate(
        String.format(
            "INSERT INTO %s VALUES (2%s);", WIDE_ROW_TABLE, ", REPEAT('a', 11534336)".repeat(160)));
  }
}
