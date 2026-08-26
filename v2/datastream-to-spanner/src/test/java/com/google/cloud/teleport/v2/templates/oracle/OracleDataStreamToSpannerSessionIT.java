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
package com.google.cloud.teleport.v2.templates.oracle;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.datastream.OracleSource;
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

/**
 * An integration test for {@link DataStreamToSpanner} Flex template which tests use-cases where a
 * session file is required.
 */
@Category({TemplateIntegrationTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class OracleDataStreamToSpannerSessionIT extends DataStreamToSpannerITBase {

  private static final String TABLE1 = "Category";
  private static final String TABLE2 = "Books";
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static HashSet<OracleDataStreamToSpannerSessionIT> testInstances = new HashSet<>();
  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  public static GcsResourceManager gcsResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;
  public static CloudOracleResourceManager oracleResourceManager;

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleDataStreamToSpannerSessionIT/oracle-google_standard_sql-spanner-schema.sql";
  private static final String ORACLE_DDL_RESOURCE =
      "oracle/OracleDataStreamToSpannerSessionIT/oracle-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "oracle/OracleDataStreamToSpannerSessionIT/oracle-session.json";

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (OracleDataStreamToSpannerSessionIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = setUpSpannerResourceManager();
        pubsubResourceManager = setUpPubSubResourceManager();
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        oracleResourceManager = setUpOracleResourceManager();

        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
        try {
          oracleResourceManager.runSQLUpdate("DROP TABLE \"Category\"");
        } catch (Exception e) {
        }
        try {
          oracleResourceManager.runSQLUpdate("DROP TABLE \"Books\"");
        } catch (Exception e) {
        }
        executeSqlScript(oracleResourceManager, ORACLE_DDL_RESOURCE);

        // Pre-insert books
        oracleResourceManager.runSQLUpdate(
            "INSERT INTO \"Books\" (\"id\", \"title\", \"author_id\") VALUES(1, 'The Lord of the"
                + " Rings', 1)");
        oracleResourceManager.runSQLUpdate(
            "INSERT INTO \"Books\" (\"id\", \"title\", \"author_id\") VALUES(2, 'Pride and"
                + " Prejudice', 2)");
        oracleResourceManager.runSQLUpdate(
            "INSERT INTO \"Books\" (\"id\", \"title\", \"author_id\") VALUES(3, 'The Hitchhikers"
                + " Guide to the Galaxy', 3)");

        // Pre-insert categories
        oracleResourceManager.runSQLUpdate(
            "INSERT INTO \"Category\" (\"category_id\", \"name\", \"last_update\") VALUES(1, 'xyz',"
                + " CURRENT_TIMESTAMP)");
        oracleResourceManager.runSQLUpdate(
            "INSERT INTO \"Category\" (\"category_id\", \"name\", \"last_update\") VALUES(2, 'abc',"
                + " CURRENT_TIMESTAMP)");

        flushOracleLogs();

        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity(System.getProperty("privateConnectivity"))
                .build();

        JDBCSource jdbcSource =
            OracleSource.builder(
                    System.getProperty("cloudOracleHost"),
                    System.getProperty("cloudProxyUsername", "system"),
                    System.getProperty("cloudProxyPassword", "TestPassword123"),
                    1521,
                    oracleResourceManager.getDatabaseName())
                .setAllowedTables(
                    new HashMap<>() {
                      {
                        put(
                            oracleResourceManager.getUsername().toUpperCase(),
                            Arrays.asList("Category", "Books"));
                      }
                    })
                .build();

        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                SESSION_FILE_RESOURCE,
                null,
                "OracleSessionIT",
                spannerResourceManager,
                pubsubResourceManager,
                new HashMap<>() {
                  {
                    put("inputFileFormat", "avro");
                  }
                },
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                null,
                jdbcSource);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (OracleDataStreamToSpannerSessionIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        pubsubResourceManager,
        gcsResourceManager,
        datastreamResourceManager,
        oracleResourceManager);
  }

  private void flushOracleLogs() {
    try (Connection conn =
            DriverManager.getConnection(
                "jdbc:oracle:thin:@//"
                    + System.getProperty("cloudOracleHost", "localhost")
                    + ":1521/FREE",
                "system",
                "TestPassword123");
        Statement stmt = conn.createStatement()) {
      stmt.execute("ALTER SYSTEM SWITCH LOGFILE");
    } catch (SQLException e) {
      throw new RuntimeException("Failed to flush Oracle logs", e);
    }
  }

  @Test
  public void migrationTestWithRenameAndDrops() {
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(JOB_START_PROCESSING_WAIT_MINUTES)),
                conditionCheck);

    assertThatResult(result).meetsConditions();

    assertCategoryTableBackfillContents();

    oracleResourceManager.runSQLUpdate(
        "INSERT INTO \"Category\" (\"category_id\", \"name\", \"last_update\") VALUES(3, 'def',"
            + " CURRENT_TIMESTAMP)");
    oracleResourceManager.runSQLUpdate(
        "INSERT INTO \"Category\" (\"category_id\", \"name\", \"last_update\") VALUES(4, 'ghi',"
            + " CURRENT_TIMESTAMP)");
    oracleResourceManager.runSQLUpdate(
        "UPDATE \"Category\" SET \"name\"='abc1' WHERE \"category_id\"=2");
    oracleResourceManager.runSQLUpdate("DELETE FROM \"Category\" WHERE \"category_id\"=1");

    flushOracleLogs();

    conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                        .setMinRows(3)
                        .setMaxRows(3)
                        .build()))
            .build();

    result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(8)), conditionCheck);

    assertThatResult(result).meetsConditions();

    try {
      Thread.sleep(CUTOVER_MILLIS);
    } catch (InterruptedException e) {
    }
    assertCategoryTableCdcContents();
  }

  @Test
  public void migrationTestWithSyntheticPKAndExtraColumn() {
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE2)
                        .setMinRows(3)
                        .setMaxRows(3)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(JOB_START_PROCESSING_WAIT_MINUTES)),
                conditionCheck);

    assertThatResult(result).meetsConditions();
    assertBooksBackfillContents();
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
}
