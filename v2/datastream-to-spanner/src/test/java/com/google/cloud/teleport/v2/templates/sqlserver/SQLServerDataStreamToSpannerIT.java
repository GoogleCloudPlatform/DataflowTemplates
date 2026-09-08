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
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudSqlServerResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.SqlServerSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main functional integration test for standard datastream-to-spanner live streaming CDC migrations
 * with SQL Server.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class SQLServerDataStreamToSpannerIT extends DataStreamToSpannerITBase {

  private static final Logger LOG = LoggerFactory.getLogger(SQLServerDataStreamToSpannerIT.class);

  private static final String SQLSERVER_DDL_RESOURCE =
      "sqlserver/SQLServerDataStreamToSpannerIT/sqlserver-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/SQLServerDataStreamToSpannerIT/spanner-schema.sql";
  private static final String SESSION_RESOURCE =
      "sqlserver/SQLServerDataStreamToSpannerIT/session.json";

  private static final String ROW_ID = "id";
  private static final String NAME = "name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String ENTRY_ADDED = "entry_added";
  private static final List<String> COLUMNS = List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED);

  private CloudSqlServerResourceManager sqlServerResourceManager;
  private SpannerResourceManager spannerResourceManager;
  private DatastreamResourceManager datastreamResourceManager;
  private PubsubResourceManager pubsubResourceManager;
  private GcsResourceManager gcsResourceManager;

  @Before
  public void setUp() throws IOException {
    sqlServerResourceManager = setUpSqlServerResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
    pubsubResourceManager = setUpPubSubResourceManager();
    DatastreamResourceManager.Builder datastreamBuilder =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider);
    if (System.getProperty("privateConnectivity") != null) {
      datastreamBuilder.setPrivateConnectivity(System.getProperty("privateConnectivity"));
    }
    datastreamResourceManager = datastreamBuilder.build();
    gcsResourceManager = setUpSpannerITGcsResourceManager();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        sqlServerResourceManager,
        spannerResourceManager,
        datastreamResourceManager,
        pubsubResourceManager,
        gcsResourceManager);
  }

  @Test
  public void testDataStreamSqlServerToSpanner() throws Exception {
    LOG.info("Executing SQL Server DDL...");
    executeSqlScript(sqlServerResourceManager, SQLSERVER_DDL_RESOURCE);

    LOG.info("Creating Spanner DDL...");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    String sessionFileContent =
        generateSessionFile(
            2,
            sqlServerResourceManager.getDatabaseName(),
            spannerResourceManager.getDatabaseId(),
            List.of("source_table1", "source_table2"),
            SESSION_RESOURCE);

    SqlServerSource sqlServerSource =
        SqlServerSource.builder(
                sqlServerResourceManager.getHost(),
                sqlServerResourceManager.getUsername(),
                sqlServerResourceManager.getPassword(),
                sqlServerResourceManager.getPort(),
                sqlServerResourceManager.getDatabaseName())
            .setAllowedTables(Map.of("dbo", List.of("source_table1", "source_table2")))
            .build();
    LOG.info(
        "SQL Server details: host={}, resolvedHost={}, port={}, db={}, user={}, passLength={}",
        sqlServerResourceManager.getHost(),
        sqlServerSource.hostname(),
        sqlServerSource.port(),
        sqlServerSource.database(),
        sqlServerResourceManager.getUsername(),
        sqlServerResourceManager.getPassword() != null
            ? sqlServerResourceManager.getPassword().length()
            : 0);

    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            testName,
            SESSION_RESOURCE,
            null,
            testName,
            spannerResourceManager,
            pubsubResourceManager,
            new HashMap<>(),
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            sessionFileContent,
            sqlServerSource);
    assertThatPipeline(jobInfo).isRunning();

    // Insert initial data
    insertSqlServerData("source_table1", 1, "Alice", 30, "Y", "2026-01-01");
    insertSqlServerData("source_table2", 1, "Bob", 40, "N", "2026-01-02");

    ConditionCheck condition =
        SpannerRowsCheck.builder(spannerResourceManager, "source_table1")
            .setMinRows(1)
            .build()
            .and(
                SpannerRowsCheck.builder(spannerResourceManager, "source_table2")
                    .setMinRows(1)
                    .build());

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(JOB_START_PROCESSING_WAIT_MINUTES)),
                condition);
    assertThatResult(result).meetsConditions();

    List<Struct> rows1 = spannerResourceManager.readTableRecords("source_table1", COLUMNS);
    SpannerAsserts.assertThatStructs(rows1)
        .hasRecordsUnorderedCaseInsensitiveColumns(
            List.of(
                Map.of(
                    ROW_ID, 1L,
                    NAME, "Alice",
                    AGE, 30L,
                    MEMBER, "Y",
                    ENTRY_ADDED, "2026-01-01")));

    List<Struct> rows2 = spannerResourceManager.readTableRecords("source_table2", COLUMNS);
    SpannerAsserts.assertThatStructs(rows2)
        .hasRecordsUnorderedCaseInsensitiveColumns(
            List.of(
                Map.of(
                    ROW_ID, 1L,
                    NAME, "Bob",
                    AGE, 40L,
                    MEMBER, "N",
                    ENTRY_ADDED, "2026-01-02")));
  }

  private void insertSqlServerData(
      String tableName, int id, String name, int age, String member, String entryAdded) {
    sqlServerResourceManager.runSQLUpdate(
        String.format(
            "INSERT INTO %s (id, name, age, member, entry_added) VALUES (%d, '%s', %d, '%s', '%s')",
            tableName, id, name, age, member, entryAdded));
  }
}
