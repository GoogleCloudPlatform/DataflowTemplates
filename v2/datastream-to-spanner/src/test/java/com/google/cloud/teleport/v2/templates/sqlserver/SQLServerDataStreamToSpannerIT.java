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
 * with SQL Server: an initial wave of INSERTs followed by UPDATEs and DELETEs, verified on two
 * tables mapped through a session file.
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
    // Cancel the Dataflow job ourselves (tearDownBase in cleanUp) before deleting the resources it
    // uses; the base class @After would otherwise run after this class's @After.
    skipBaseCleanup = true;
    sqlServerResourceManager = setUpSqlServerResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
    pubsubResourceManager = setUpPubSubResourceManager();
    datastreamResourceManager = setUpDatastreamResourceManager();
    gcsResourceManager = setUpSpannerITGcsResourceManager();
  }

  @After
  public void cleanUp() throws IOException {
    tearDownBase();
    // Datastream streams are deleted first, before the CDC-enabled source database is dropped.
    ResourceManagerUtils.cleanResources(
        datastreamResourceManager,
        sqlServerResourceManager,
        spannerResourceManager,
        pubsubResourceManager,
        gcsResourceManager);
  }

  @Test
  public void testDataStreamSqlServerToSpanner() throws Exception {
    LOG.info("Executing SQL Server DDL...");
    executeSqlScript(sqlServerResourceManager, SQLSERVER_DDL_RESOURCE);
    sqlServerResourceManager.enableCdcOnAllTables();

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

    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            testName,
            null,
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

    // 1. Initial wave of inserts (two rows per table).
    insertSqlServerData("source_table1", 1, "Alice", 30, "Y", "2026-01-01");
    insertSqlServerData("source_table1", 2, "Carol", 35, "N", "2026-01-03");
    insertSqlServerData("source_table2", 1, "Bob", 40, "N", "2026-01-02");
    insertSqlServerData("source_table2", 2, "Dave", 45, "Y", "2026-01-04");

    ConditionCheck insertCondition =
        SpannerRowsCheck.builder(spannerResourceManager, "source_table1")
            .setMinRows(2)
            .setMaxRows(2)
            .build()
            .and(
                SpannerRowsCheck.builder(spannerResourceManager, "source_table2")
                    .setMinRows(2)
                    .setMaxRows(2)
                    .build());
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(JOB_START_PROCESSING_WAIT_MINUTES)),
                insertCondition);
    assertThatResult(result).meetsConditions();

    assertTableContents(
        "source_table1",
        row(1L, "Alice", 30L, "Y", "2026-01-01"),
        row(2L, "Carol", 35L, "N", "2026-01-03"));
    assertTableContents(
        "source_table2",
        row(1L, "Bob", 40L, "N", "2026-01-02"),
        row(2L, "Dave", 45L, "Y", "2026-01-04"));

    // 2. Second wave: update row 1 and delete row 2 in each table.
    sqlServerResourceManager.runSQLUpdate(
        "UPDATE source_table1 SET name = 'ALICE', age = 31, member = 'N' WHERE id = 1");
    sqlServerResourceManager.runSQLUpdate("DELETE FROM source_table1 WHERE id = 2");
    sqlServerResourceManager.runSQLUpdate(
        "UPDATE source_table2 SET name = 'BOB', age = 41, member = 'Y' WHERE id = 1");
    sqlServerResourceManager.runSQLUpdate("DELETE FROM source_table2 WHERE id = 2");

    // A row count alone is also met before the UPDATE is applied, so wait for the final state.
    ConditionCheck cdcCondition =
        rowPresentCheck(
                "source_table1 CDC wave applied",
                "SELECT id FROM source_table1 WHERE id = 1 AND name = 'ALICE' AND age = 31"
                    + " AND member = 'N'"
                    + " AND NOT EXISTS (SELECT 1 FROM source_table1 WHERE id = 2)")
            .and(
                rowPresentCheck(
                    "source_table2 CDC wave applied",
                    "SELECT id FROM source_table2 WHERE id = 1 AND name = 'BOB' AND age = 41"
                        + " AND member = 'Y'"
                        + " AND NOT EXISTS (SELECT 1 FROM source_table2 WHERE id = 2)"));
    result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(10)), cdcCondition);
    assertThatResult(result).meetsConditions();

    assertTableContents("source_table1", row(1L, "ALICE", 31L, "N", "2026-01-01"));
    assertTableContents("source_table2", row(1L, "BOB", 41L, "Y", "2026-01-02"));
  }

  private static Map<String, Object> row(
      long id, String name, long age, String member, String entryAdded) {
    return Map.of(ROW_ID, id, NAME, name, AGE, age, MEMBER, member, ENTRY_ADDED, entryAdded);
  }

  @SafeVarargs
  private void assertTableContents(String tableName, Map<String, Object>... expectedRows) {
    List<Struct> rows = spannerResourceManager.readTableRecords(tableName, COLUMNS);
    SpannerAsserts.assertThatStructs(rows)
        .hasRecordsUnorderedCaseInsensitiveColumns(List.of(expectedRows));
  }

  /** A condition that passes once the given query returns at least one row from Spanner. */
  private ConditionCheck rowPresentCheck(String description, String query) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return description;
      }

      @Override
      protected CheckResult check() {
        int found = spannerResourceManager.runQuery(query).size();
        return new CheckResult(
            found > 0, String.format("%s: query returned %d row(s)", description, found));
      }
    };
  }

  private void insertSqlServerData(
      String tableName, int id, String name, int age, String member, String entryAdded) {
    sqlServerResourceManager.runSQLUpdate(
        String.format(
            "INSERT INTO %s (id, name, age, member, entry_added) VALUES (%d, '%s', %d, '%s', '%s')",
            tableName, id, name, age, member, entryAdded));
  }
}
