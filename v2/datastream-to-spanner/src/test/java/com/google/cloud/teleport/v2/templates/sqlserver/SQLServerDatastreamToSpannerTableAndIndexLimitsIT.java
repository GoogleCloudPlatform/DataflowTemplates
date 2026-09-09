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
package com.google.cloud.teleport.v2.templates.sqlserver;

import static java.util.Map.entry;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.cloudsql.CloudSqlServerResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.SqlServerSource;
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
 * An integration test for {@link DataStreamToSpanner} Flex template which verifies the table and
 * index data limits when migrating from a SQL Server database.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class SQLServerDatastreamToSpannerTableAndIndexLimitsIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SQLServerDatastreamToSpannerTableAndIndexLimitsIT.class);

  private static final String SQLSERVER_DDL_RESOURCE =
      "sqlserver/SQLServerDatastreamToSpannerTableAndIndexLimitsIT/sqlserver-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/SQLServerDatastreamToSpannerTableAndIndexLimitsIT/spanner-schema.sql";
  private static final String SESSION_FILE =
      "sqlserver/SQLServerDatastreamToSpannerTableAndIndexLimitsIT/session.json";

  private static final String LARGE_KEY_TABLE = "LargeKey";
  private static final String LARGE_CELL_TABLE = "LargeCell";
  private static final String WIDE_ROW_TABLE = "WideRow";
  private static final List<String> TABLES =
      List.of(LARGE_KEY_TABLE, LARGE_CELL_TABLE, WIDE_ROW_TABLE);

  private static CloudSqlServerResourceManager msSqlResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;
  private static HashSet<SQLServerDatastreamToSpannerTableAndIndexLimitsIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SQLServerDatastreamToSpannerTableAndIndexLimitsIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        LOG.info("Setting up SQL Server resource manager...");
        msSqlResourceManager = setUpSqlServerResourceManager();
        LOG.info("SQL Server resource manager created with URI: {}", msSqlResourceManager.getUri());
        LOG.info("Setting up Spanner resource manager...");
        spannerResourceManager = setUpSpannerResourceManager();
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
        datastreamResourceManager = setUpDatastreamResourceManager();
        LOG.info("Datastream resource manager created");

        LOG.info("Executing SQL Server DDL script...");
        executeSqlScript(msSqlResourceManager, SQLSERVER_DDL_RESOURCE);
        LOG.info("Creating Spanner DDL...");
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

        LOG.info("Generating session file content...");
        String sessionFileContent =
            generateSessionFile(
                1,
                msSqlResourceManager.getDatabaseName(),
                spannerResourceManager.getDatabaseId(),
                TABLES,
                SESSION_FILE);

        Map<String, String> jobParams = new HashMap<>();
        jobParams.put("dlqMaxRetryCount", "1");
        jobParams.put("datastreamSourceType", "sqlserver");

        SqlServerSource sqlServerSource =
            SqlServerSource.builder(
                    msSqlResourceManager.getHost(),
                    msSqlResourceManager.getUsername(),
                    msSqlResourceManager.getPassword(),
                    msSqlResourceManager.getPort(),
                    msSqlResourceManager.getDatabaseName())
                .setAllowedTables(Map.of("dbo", TABLES))
                .build();

        LOG.info("Launching Dataflow job...");
        jobInfo =
            launchDataflowJob(
                "sqlserver-table-and-index-limits",
                null,
                null,
                "sqlserver-datastream-to-spanner-table-and-index-limits",
                spannerResourceManager,
                pubsubResourceManager,
                jobParams,
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                sessionFileContent,
                sqlServerSource);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    LOG.info("Cleaning up resources...");
    for (SQLServerDatastreamToSpannerTableAndIndexLimitsIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        msSqlResourceManager,
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
                createConfig(jobInfo, Duration.ofMinutes(15)),
                SpannerRowsCheck.builder(spannerResourceManager, LARGE_KEY_TABLE)
                    .setMinRows(1)
                    .build());
    assertThatResult(result).meetsConditions();
    List<Struct> rows =
        spannerResourceManager.readTableRecords(
            LARGE_KEY_TABLE,
            List.of("pk_col1", "pk_col2", "pk_col3", "col1", "col2", "col3", "value_col"));

    SpannerAsserts.assertThatStructs(rows)
        .hasRecordsUnorderedCaseInsensitiveColumns(
            List.of(
                Map.ofEntries(
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
                createConfig(jobInfo, Duration.ofMinutes(15)),
                SpannerRowsCheck.builder(spannerResourceManager, LARGE_CELL_TABLE)
                    .setMinRows(1)
                    .build());
    assertThatResult(result).meetsConditions();
    List<Struct> rows = spannerResourceManager.readTableRecords(LARGE_CELL_TABLE, List.of("id"));
    SpannerAsserts.assertThatStructs(rows)
        .hasRecordsUnorderedCaseInsensitiveColumns(List.of(Map.ofEntries(entry("id", 3L))));
  }
}
