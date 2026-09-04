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
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
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
 * An integration test using separate shadow table database for {@link DataStreamToSpanner} Flex
 * template which tests use-cases where a session file is required.
 */
@Category({TemplateIntegrationTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class OracleSeparateShadowTableDatabaseSessionIT extends DataStreamToSpannerITBase {

  private static final String TABLE1 = "Category";
  private static final String TABLE2 = "Books";
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static HashSet<OracleSeparateShadowTableDatabaseSessionIT> testInstances =
      new HashSet<>();

  public static DatastreamResourceManager datastreamResourceManager;
  public static CloudOracleResourceManager cloudOracleSysUser;
  public static CloudOracleResourceManager cloudOracleResourceManager;
  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager shadowSpannerResourceManager;
  public static GcsResourceManager gcsResourceManager;
  public static String oracleUser;

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleSeparateShadowTableDatabaseSessionIT/spanner-schema.sql";
  private static final String ORACLE_DDL_RESOURCE =
      "oracle/OracleSeparateShadowTableDatabaseSessionIT/oracle-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "oracle/OracleSeparateShadowTableDatabaseSessionIT/oracle-session.json";

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (OracleSeparateShadowTableDatabaseSessionIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity(
                    System.getProperty("privateConnectivity", "datastream-connect-2"))
                .build();

        cloudOracleResourceManager = setUpOracleResourceManager();
        try {
          cloudOracleResourceManager.runSQLUpdate(
              String.format("DROP TABLE \"%s\" CASCADE CONSTRAINTS", TABLE1));
        } catch (Exception e) {
        }
        try {
          cloudOracleResourceManager.runSQLUpdate(
              String.format("DROP TABLE \"%s\" CASCADE CONSTRAINTS", TABLE2));
        } catch (Exception e) {
        }
        executeSqlScript(cloudOracleResourceManager, ORACLE_DDL_RESOURCE);

        spannerResourceManager = setUpSpannerResourceManager();
        shadowSpannerResourceManager = setUpShadowSpannerResourceManager();
        pubsubResourceManager = setUpPubSubResourceManager();
        gcsResourceManager = setUpSpannerITGcsResourceManager();

        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

        OracleSource jdbcSource =
            OracleSource.builder(
                    cloudOracleResourceManager.getHost(),
                    cloudOracleResourceManager.getUsername(),
                    cloudOracleResourceManager.getPassword(),
                    cloudOracleResourceManager.getPort(),
                    cloudOracleResourceManager.getDatabaseName())
                .setAllowedTables(
                    Map.of(
                        cloudOracleResourceManager.getUsername().toUpperCase(),
                        List.of(TABLE1, TABLE2)))
                .build();

        Map<String, String> jobParams = new HashMap<>();
        jobParams.put("shadowTableSpannerInstanceId", shadowSpannerResourceManager.getInstanceId());
        jobParams.put("shadowTableSpannerDatabaseId", shadowSpannerResourceManager.getDatabaseId());

        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                SESSION_FILE_RESOURCE,
                null,
                "OracleSeparateShadowTableDatabaseSessionIT",
                spannerResourceManager,
                pubsubResourceManager,
                jobParams,
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                null,
                jdbcSource);
      }
    }
  }

  public static void setUpOracleUser(
      CloudOracleResourceManager sysUser, String user, String password) {
    sysUser.runSQLUpdate(
        String.format("CREATE USER %s IDENTIFIED BY %s CONTAINER=ALL", user, password));
    sysUser.runSQLUpdate(String.format("GRANT DBA TO %s CONTAINER=ALL", user));
    sysUser.runSQLUpdate(
        String.format("GRANT EXECUTE ON SYS.DBMS_LOGMNR TO %s CONTAINER=ALL", user));
    sysUser.runSQLUpdate(String.format("ALTER USER %s QUOTA 50m ON SYSTEM CONTAINER=ALL", user));

    // Add c##datastream grants based on requirements
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (OracleSeparateShadowTableDatabaseSessionIT instance : testInstances) {
      try {
        instance.tearDownBase();
      } catch (Exception e) {
        // Ignore UnsupportedOperationException from Thread.stop() on Java 21+
      }
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        pubsubResourceManager,
        shadowSpannerResourceManager,
        cloudOracleSysUser,
        cloudOracleResourceManager,
        datastreamResourceManager,
        gcsResourceManager);
  }

  @Test
  public void migrationTestWithRenameAndDrops() {
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeInitialCategoryData(),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(45)), conditionCheck);

    assertThatResult(result).meetsConditions();
    assertCategoryTableBackfillContents();

    conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeCdcCategoryData(),
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
                    writeBooksData(),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE2)
                        .setMinRows(3)
                        .setMaxRows(3)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(45)), conditionCheck);

    assertThatResult(result).meetsConditions();
    assertBooksBackfillContents();
  }

  private ConditionCheck writeInitialCategoryData() {
    return new ConditionCheck() {
      private boolean executed = false;

      @Override
      protected String getDescription() {
        return "Write initial Category rows";
      }

      @Override
      protected CheckResult check() {
        if (executed) {
          return new CheckResult(true, "Sent category initial");
        }
        try {
          cloudOracleResourceManager.runSQLUpdate(
              String.format(
                  "INSERT INTO \"%s\" (\"category_id\", \"full_name\") VALUES (1, 'xyz')", TABLE1));
          cloudOracleResourceManager.runSQLUpdate(
              String.format(
                  "INSERT INTO \"%s\" (\"category_id\", \"full_name\") VALUES (2, 'abc')", TABLE1));
          cloudOracleResourceManager.runSQLUpdate("COMMIT");
          flushOracleRedoLogs(cloudOracleSysUser);
          executed = true;
          return new CheckResult(true, "Sent category initial");
        } catch (Exception e) {
          return new CheckResult(false, e.getMessage());
        }
      }
    };
  }

  private ConditionCheck writeCdcCategoryData() {
    return new ConditionCheck() {
      private boolean executed = false;

      @Override
      protected String getDescription() {
        return "Write cdc Category rows";
      }

      @Override
      protected CheckResult check() {
        if (executed) {
          return new CheckResult(true, "Sent category cdc");
        }
        try {
          cloudOracleResourceManager.runSQLUpdate(
              String.format("DELETE FROM \"%s\" WHERE \"category_id\" = 1", TABLE1));
          cloudOracleResourceManager.runSQLUpdate(
              String.format(
                  "UPDATE \"%s\" SET \"full_name\" = 'abc1' WHERE \"category_id\" = 2", TABLE1));
          cloudOracleResourceManager.runSQLUpdate(
              String.format(
                  "INSERT INTO \"%s\" (\"category_id\", \"full_name\") VALUES (3, 'def')", TABLE1));
          cloudOracleResourceManager.runSQLUpdate(
              String.format(
                  "INSERT INTO \"%s\" (\"category_id\", \"full_name\") VALUES (4, 'ghi')", TABLE1));
          cloudOracleResourceManager.runSQLUpdate("COMMIT");
          flushOracleRedoLogs(cloudOracleSysUser);
          executed = true;
          return new CheckResult(true, "Sent category cdc");
        } catch (Exception e) {
          return new CheckResult(false, e.getMessage());
        }
      }
    };
  }

  private ConditionCheck writeBooksData() {
    return new ConditionCheck() {
      private boolean executed = false;

      @Override
      protected String getDescription() {
        return "Write Books rows";
      }

      @Override
      protected CheckResult check() {
        if (executed) {
          return new CheckResult(true, "Sent books");
        }
        try {
          cloudOracleResourceManager.runSQLUpdate(
              String.format(
                  "INSERT INTO \"%s\" (\"id\", \"title\", \"author_id\") VALUES (1, 'The Lord of"
                      + " the Rings', 1)",
                  TABLE2));
          cloudOracleResourceManager.runSQLUpdate(
              String.format(
                  "INSERT INTO \"%s\" (\"id\", \"title\", \"author_id\") VALUES (2, 'Pride and"
                      + " Prejudice', 2)",
                  TABLE2));
          cloudOracleResourceManager.runSQLUpdate(
              String.format(
                  "INSERT INTO \"%s\" (\"id\", \"title\", \"author_id\") VALUES (3, 'The"
                      + " Hitchhikers Guide to the Galaxy', 3)",
                  TABLE2));
          cloudOracleResourceManager.runSQLUpdate("COMMIT");
          flushOracleRedoLogs(cloudOracleSysUser);
          executed = true;
          return new CheckResult(true, "Sent books");
        } catch (Exception e) {
          return new CheckResult(false, e.getMessage());
        }
      }
    };
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
