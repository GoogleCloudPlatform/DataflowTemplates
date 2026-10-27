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
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An integration test for {@link DataStreamToSpanner} Flex template which tests use-cases where a
 * session file is required, using a real Datastream stream on a SQL Server source.
 *
 * <p>This is the SQL Server port of {@code DataStreamToSpannerSessionIT}. The SMT session file
 * ({@code sqlserver-session.json}) keeps the same semantics as the MySQL reference:
 *
 * <ul>
 *   <li>{@code Category}: column {@code name} is renamed to {@code full_name} and column {@code
 *       last_update} is dropped.
 *   <li>{@code Books}: the source table has no primary key; Spanner uses the synthetic primary key
 *       {@code synth_id} and has an extra column {@code extraCol1} that does not exist at the
 *       source.
 *   <li>{@code Genre}: present at the source but dropped from the Spanner schema.
 * </ul>
 *
 * <p>SQL Server specific deviations:
 *
 * <ul>
 *   <li>Datastream reports the SQL Server schema name ({@code dbo}) as {@code _metadata_schema}, so
 *       the session file's {@code SrcSchema.Schema} entries are {@code dbo} and the file is
 *       uploaded as-is (no placeholder substitution), exactly like the static MySQL reference.
 *   <li>The dropped {@code last_update} column is {@code datetimeoffset} instead of {@code
 *       timestamp} because SQL Server {@code datetime*} values are not yet handled by the pipeline.
 *   <li>SQL Server CDC with {@code @supports_net_changes = 1} requires a primary key, so {@code
 *       Books} has a PRIMARY KEY on {@code id} on the source side; the session file still maps it
 *       to the synthetic {@code synth_id} key on Spanner, which is what this scenario exercises.
 *   <li>Instead of uploading pre-generated Avro files, the backfill rows are inserted into SQL
 *       Server before the stream is started and the CDC wave is applied with DML afterwards.
 *   <li>A {@code Genre} row is streamed as well to verify that events for a table dropped in the
 *       session file are ignored by the pipeline without affecting the other tables.
 * </ul>
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class SQLServerDataStreamToSpannerSessionIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SQLServerDataStreamToSpannerSessionIT.class);

  private static final String SQLSERVER_DDL_RESOURCE =
      "sqlserver/SQLServerDataStreamToSpannerSessionIT/sqlserver-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/SQLServerDataStreamToSpannerSessionIT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "sqlserver/SQLServerDataStreamToSpannerSessionIT/sqlserver-session.json";
  private static final String GCS_PATH_PREFIX = "sqlserver-session-it";

  private static final String TABLE1 = "Category";
  private static final String TABLE2 = "Books";
  private static final String TABLE3 = "Genre";

  private static boolean initialized = false;
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static CloudSqlServerResourceManager msSqlResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;

  private static final Set<SQLServerDataStreamToSpannerSessionIT> testInstances =
      ConcurrentHashMap.newKeySet();

  /**
   * Setup resource managers, seed the source tables and launch the Dataflow job once during the
   * execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    // Prevent cleaning up of dataflow job after a test method is executed.
    skipBaseCleanup = true;
    synchronized (SQLServerDataStreamToSpannerSessionIT.class) {
      testInstances.add(this);
      if (!initialized) {
        LOG.info("Setting up SQL Server test container resource manager...");
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
        msSqlResourceManager.enableCdcOnAllTables();

        LOG.info("Inserting backfill rows into SQL Server...");
        insertBackfillRows();

        LOG.info("Creating Spanner DDL...");
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

        SqlServerSource sqlServerSource =
            SqlServerSource.builder(
                    msSqlResourceManager.getHost(),
                    msSqlResourceManager.getUsername(),
                    msSqlResourceManager.getPassword(),
                    msSqlResourceManager.getPort(),
                    msSqlResourceManager.getDatabaseName())
                .setAllowedTables(Map.of("dbo", List.of(TABLE1, TABLE2, TABLE3)))
                .build();

        LOG.info("Launching Dataflow job...");
        jobInfo =
            launchDataflowJob(
                "sqlserver-session",
                SESSION_FILE_RESOURCE,
                null,
                GCS_PATH_PREFIX,
                spannerResourceManager,
                pubsubResourceManager,
                new HashMap<>(),
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                null,
                sqlServerSource);
        assertThatPipeline(jobInfo).isRunning();

        initialized = true;
      }
    }
  }

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    LOG.info("Cleaning up resources...");
    // A failing teardown of one instance must not prevent the other instances' Dataflow jobs and
    // the shared resource managers below from being cleaned up.
    for (SQLServerDataStreamToSpannerSessionIT instance : testInstances) {
      try {
        instance.tearDownBase();
      } catch (Exception e) {
        LOG.error("Failed to tear down test instance {}", instance.testName, e);
      }
    }

    // Datastream streams must be deleted before the CDC-enabled source database is dropped.
    ResourceManagerUtils.cleanResources(datastreamResourceManager);

    ResourceManagerUtils.cleanResources(
        msSqlResourceManager, spannerResourceManager, gcsResourceManager, pubsubResourceManager);
  }

  /** Test checks for the following use-cases: 1. Drop Column. 2. Rename Column. 3. Drop Table */
  @Test
  public void migrationTestWithRenameAndDrops() {
    // 1. Wait for the backfill rows (inserted before the stream was started) to land in Spanner.
    LOG.info("Waiting for Category backfill rows...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(JOB_START_PROCESSING_WAIT_MINUTES)),
                SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                    .setMinRows(2)
                    .setMaxRows(2)
                    .build());

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    assertCategoryTableBackfillContents();

    // 2. Apply the CDC wave on the source and wait for it to propagate.
    LOG.info("Applying CDC changes to Category...");
    msSqlResourceManager.runSQLUpdate(
        "UPDATE Category SET name = 'abc1', last_update = '2024-01-02 00:00:00 +00:00'"
            + " WHERE category_id = 2");
    msSqlResourceManager.runSQLUpdate("DELETE FROM Category WHERE category_id = 1");
    msSqlResourceManager.runSQLUpdate(
        "INSERT INTO Category (category_id, name, last_update)"
            + " VALUES (3, 'def', '2024-01-03 00:00:00 +00:00')");
    msSqlResourceManager.runSQLUpdate(
        "INSERT INTO Category (category_id, name, last_update) VALUES (4, 'ghi', NULL)");

    // A row count alone is also satisfied before the UPDATE of category 2 is applied, so
    // additionally wait for the renamed value to be visible.
    LOG.info("Waiting for Category CDC rows...");
    result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(8)),
                SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                    .setMinRows(3)
                    .setMaxRows(3)
                    .build()
                    .and(
                        rowPresentCheck(
                            "Category 2 updated",
                            "SELECT category_id FROM Category"
                                + " WHERE category_id = 2 AND full_name = 'abc1'")));

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    // Sleep for cutover time to wait till all CDCs propagate.
    // A real world customer also has a small cut over time to reach consistency.
    try {
      Thread.sleep(CUTOVER_MILLIS);
    } catch (InterruptedException e) {
    }
    assertCategoryTableCdcContents();
  }

  @Test
  public void migrationTestWithSyntheticPKAndExtraColumn() {
    // Wait for the Books backfill rows (inserted before the stream was started) to land in
    // Spanner.
    LOG.info("Waiting for Books backfill rows...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(JOB_START_PROCESSING_WAIT_MINUTES)),
                SpannerRowsCheck.builder(spannerResourceManager, TABLE2)
                    .setMinRows(3)
                    .setMaxRows(3)
                    .build());

    // Assert Conditions
    assertThatResult(result).meetsConditions();
    assertBooksBackfillContents();
  }

  /** Seeds the source tables so that the rows are migrated by the Datastream backfill. */
  private static void insertBackfillRows() {
    // Category: the `last_update` column is dropped in the session file.
    msSqlResourceManager.runSQLUpdate(
        "INSERT INTO Category (category_id, name, last_update)"
            + " VALUES (1, 'xyz', '2024-01-01 00:00:00 +00:00')");
    msSqlResourceManager.runSQLUpdate(
        "INSERT INTO Category (category_id, name, last_update) VALUES (2, 'abc', NULL)");

    // Books: no primary key at the source, synthetic PK on Spanner.
    msSqlResourceManager.runSQLUpdate(
        "INSERT INTO Books (id, title, author_id) VALUES (1, 'The Lord of the Rings', 1)");
    msSqlResourceManager.runSQLUpdate(
        "INSERT INTO Books (id, title, author_id) VALUES (2, 'Pride and Prejudice', 2)");
    msSqlResourceManager.runSQLUpdate(
        "INSERT INTO Books (id, title, author_id)"
            + " VALUES (3, 'The Hitchhikers Guide to the Galaxy', 3)");

    // Genre: dropped table, its events must be ignored by the pipeline.
    msSqlResourceManager.runSQLUpdate(
        "INSERT INTO Genre (genre_id, name) VALUES (1, 'Science Fiction')");
  }

  private void assertCategoryTableBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row1 = new HashMap<>();
    row1.put("category_id", 1);
    row1.put("full_name", "xyz");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("category_id", 2);
    row2.put("full_name", "abc");

    events.add(row1);
    events.add(row2);

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Category"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertCategoryTableCdcContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row1 = new HashMap<>();
    row1.put("category_id", 2);
    row1.put("full_name", "abc1");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("category_id", 3);
    row2.put("full_name", "def");

    Map<String, Object> row3 = new HashMap<>();
    row3.put("category_id", 4);
    row3.put("full_name", "ghi");

    events.add(row1);
    events.add(row2);
    events.add(row3);

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Category"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertBooksBackfillContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("id", 1);
    row.put("title", "The Lord of the Rings");
    row.put("author_id", 1);
    events.add(row);

    row = new HashMap<>();
    row.put("id", 2);
    row.put("title", "Pride and Prejudice");
    row.put("author_id", 2);
    events.add(row);

    row = new HashMap<>();
    row.put("id", 3);
    row.put("title", "The Hitchhikers Guide to the Galaxy");
    row.put("author_id", 3);
    events.add(row);

    ImmutableList<Struct> synthIds = spannerResourceManager.runQuery("select synth_id from Books");

    Assert.assertEquals(3, synthIds.size());
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("select id, title, author_id from Books"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  /** A condition that passes once the given query returns at least one row from Spanner. */
  private static ConditionCheck rowPresentCheck(String description, String query) {
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
}
