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

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
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

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class OracleSeparateShadowTableDatabaseEventsIT extends DataStreamToSpannerITBase {

  private static final String TABLE1 = "Users";
  private static final String TABLE2 = "Movie";

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleSeparateShadowTableDatabaseEventsIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";

  private static HashSet<OracleSeparateShadowTableDatabaseEventsIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager shadowSpannerResourceManager;
  public static GcsResourceManager gcsResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;
  public static SpannerOracleResourceManager oracleResourceManager;
  private static String oracleUser;

  @Before
  public void setUp() throws Exception {
    skipBaseCleanup = true;
    synchronized (OracleSeparateShadowTableDatabaseEventsIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity("datastream-connect-2")
                .build();

        spannerResourceManager = setUpSpannerResourceManager();
        shadowSpannerResourceManager = setUpShadowSpannerResourceManager();
        pubsubResourceManager = setUpPubSubResourceManager();
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

        oracleResourceManager = SharedOracleLiveITInstance.getInstance();
        oracleUser = SharedOracleLiveITInstance.setupOracleIsolatedUser();

        executeOracleSqlFileScript(
            oracleResourceManager,
            "oracle/OracleSeparateShadowTableDatabaseEventsIT/oracle-schema.sql",
            oracleUser);

        OracleSource jdbcSource =
            OracleSource.builder(
                    oracleResourceManager.getHost(),
                    oracleUser,
                    SharedOracleLiveITInstance.ORACLE_PASSWORD,
                    oracleResourceManager.getPort(),
                    oracleResourceManager.getDatabaseName())
                .setAllowedTables(
                    Map.of(
                        oracleUser.toUpperCase(),
                        List.of("Movie", "Users", "Authors", "Articles", "Books")))
                .build();

        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                null,
                null,
                "OracleSeparateShadowTableDatabaseEventsIT",
                spannerResourceManager,
                pubsubResourceManager,
                new HashMap<>() {
                  {
                    put(
                        "shadowTableSpannerInstanceId",
                        shadowSpannerResourceManager.getInstanceId());
                    put(
                        "shadowTableSpannerDatabaseId",
                        shadowSpannerResourceManager.getDatabaseId());
                    put("inputFileFormat", "avro");
                    put("workerMachineType", "n1-standard-4");
                    put("datastreamSourceType", "oracle");
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
    for (OracleSeparateShadowTableDatabaseEventsIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        pubsubResourceManager,
        shadowSpannerResourceManager,
        gcsResourceManager,
        datastreamResourceManager);
    SharedOracleLiveITInstance.dropUser(oracleUser);
  }

  @Test
  public void migrationTestWithUpdatesAndDeletes() {
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeUsersInitialData(),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build(),
                    writeUsersNextData(),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                        .setMinRows(3)
                        .setMaxRows(3)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(45)), conditionCheck);

    assertThatResult(result).meetsConditions();

    try {
      Thread.sleep(CUTOVER_MILLIS);
    } catch (InterruptedException e) {
    }
    assertUsersTableContents();
  }

  @Test
  public void migrationTestWithInsertsOnly() {
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeMovieInitialData(),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE2)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(45)), conditionCheck);

    assertThatResult(result).meetsConditions();
    assertMovieTableContents();
  }

  @Test
  public void interleavedAndFKAndIndexTest() {
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeArticlesInitialData(),
                    SpannerRowsCheck.builder(spannerResourceManager, "Articles")
                        .setMinRows(4)
                        .setMaxRows(4)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, "Books")
                        .setMinRows(4)
                        .setMaxRows(4)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, "Authors")
                        .setMinRows(4)
                        .setMaxRows(4)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(45)), conditionCheck);

    assertThatResult(result).meetsConditions();

    assertAuthorsTable();
    assertBooksTable();
    assertArticlesTable();
  }

  private ConditionCheck writeUsersInitialData() {
    return new ConditionCheck() {
      boolean executed = false;

      @Override
      protected String getDescription() {
        return "Write initial Users data";
      }

      @Override
      protected CheckResult check() {
        if (!executed) {
          try {
            executeOracleSql(
                oracleResourceManager,
                "INSERT INTO \"Users\"(\"id\",\"name\",\"age\",\"subscribed\",\"plan\",\"startDate\")"
                    + " VALUES (1, 'Tester Kumar', 30, 0, 'A', TO_DATE('2023-01-01', 'YYYY-MM-DD'))",
                oracleUser);
            executeOracleSql(
                oracleResourceManager,
                "INSERT INTO \"Users\"(\"id\",\"name\",\"age\",\"subscribed\",\"plan\",\"startDate\")"
                    + " VALUES (3, 'Tester Gupta', 50, 0, 'Z', TO_DATE('2023-06-07', 'YYYY-MM-DD'))",
                oracleUser);
            SharedOracleLiveITInstance.flushRedoLogs();
            executed = true;
          } catch (Exception e) {
            return new CheckResult(false, e.getMessage());
          }
        }
        return new CheckResult(true, "Sent initial Users data");
      }
    };
  }

  private ConditionCheck writeUsersNextData() {
    return new ConditionCheck() {
      boolean executed = false;

      @Override
      protected String getDescription() {
        return "Write next Users data";
      }

      @Override
      protected CheckResult check() {
        if (!executed) {
          try {
            executeOracleSql(
                oracleResourceManager,
                "INSERT INTO \"Users\"(\"id\",\"name\",\"age\",\"subscribed\",\"plan\",\"startDate\")"
                    + " VALUES (4, 'Tester', 38, 1, 'D', TO_DATE('2023-09-10', 'YYYY-MM-DD'))",
                oracleUser);
            SharedOracleLiveITInstance.flushRedoLogs();
            executed = true;
          } catch (Exception e) {
            return new CheckResult(false, e.getMessage());
          }
        }
        return new CheckResult(true, "Sent next Users data");
      }
    };
  }

  private ConditionCheck writeMovieInitialData() {
    return new ConditionCheck() {
      boolean executed = false;

      @Override
      protected String getDescription() {
        return "Write Movie data";
      }

      @Override
      protected CheckResult check() {
        if (!executed) {
          try {
            executeOracleSql(
                oracleResourceManager,
                "INSERT INTO \"Movie\"(\"id\",\"name\",\"startTime\",\"actor\") VALUES (1, 'movie1',"
                    + " TO_TIMESTAMP('2023-01-01 12:12:12', 'YYYY-MM-DD HH24:MI:SS'), 12345.09876)",
                oracleUser);
            executeOracleSql(
                oracleResourceManager,
                "INSERT INTO \"Movie\"(\"id\",\"name\",\"startTime\",\"actor\") VALUES (2, 'movie2',"
                    + " TO_TIMESTAMP('2023-11-25 17:10:12', 'YYYY-MM-DD HH24:MI:SS'), 931.5123)",
                oracleUser);
            SharedOracleLiveITInstance.flushRedoLogs();
            executed = true;
          } catch (Exception e) {
            return new CheckResult(false, e.getMessage());
          }
        }
        return new CheckResult(true, "Sent Movie data");
      }
    };
  }

  private ConditionCheck writeArticlesInitialData() {
    return new ConditionCheck() {
      boolean executed = false;

      @Override
      protected String getDescription() {
        return "Write Articles data";
      }

      @Override
      protected CheckResult check() {
        if (!executed) {
          try {
            executeOracleSql(
                oracleResourceManager,
                "INSERT INTO \"Authors\"(\"author_id\",\"name\") VALUES (1, 'a1')",
                oracleUser);
            executeOracleSql(
                oracleResourceManager,
                "INSERT INTO \"Authors\"(\"author_id\",\"name\") VALUES (2, 'a2')",
                oracleUser);
            executeOracleSql(
                oracleResourceManager,
                "INSERT INTO \"Authors\"(\"author_id\",\"name\") VALUES (3, 'a3')",
                oracleUser);
            executeOracleSql(
                oracleResourceManager,
                "INSERT INTO \"Authors\"(\"author_id\",\"name\") VALUES (4, 'a4')",
                oracleUser);

            executeOracleSql(
                oracleResourceManager,
                "INSERT INTO \"Articles\"(\"id\",\"name\",\"published_date\",\"author_id\") VALUES"
                    + " (1, 'Article001', TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1)",
                oracleUser);
            executeOracleSql(
                oracleResourceManager,
                "INSERT INTO \"Articles\"(\"id\",\"name\",\"published_date\",\"author_id\") VALUES"
                    + " (2, 'Article002', TO_DATE('2024-01-01', 'YYYY-MM-DD'), 1)",
                oracleUser);
            executeOracleSql(
                oracleResourceManager,
                "INSERT INTO \"Articles\"(\"id\",\"name\",\"published_date\",\"author_id\") VALUES"
                    + " (3, 'Article004', TO_DATE('2024-01-01', 'YYYY-MM-DD'), 4)",
                oracleUser);
            executeOracleSql(
                oracleResourceManager,
                "INSERT INTO \"Articles\"(\"id\",\"name\",\"published_date\",\"author_id\") VALUES"
                    + " (4, 'Article005', TO_DATE('2024-01-01', 'YYYY-MM-DD'), 3)",
                oracleUser);

            executeOracleSql(
                oracleResourceManager,
                "INSERT INTO \"Books\"(\"id\",\"title\",\"author_id\") VALUES (1, 'Book005', 3)",
                oracleUser);
            executeOracleSql(
                oracleResourceManager,
                "INSERT INTO \"Books\"(\"id\",\"title\",\"author_id\") VALUES (2, 'Book002', 3)",
                oracleUser);
            executeOracleSql(
                oracleResourceManager,
                "INSERT INTO \"Books\"(\"id\",\"title\",\"author_id\") VALUES (3, 'Book004', 4)",
                oracleUser);
            executeOracleSql(
                oracleResourceManager,
                "INSERT INTO \"Books\"(\"id\",\"title\",\"author_id\") VALUES (4, 'Book005', 2)",
                oracleUser);
            SharedOracleLiveITInstance.flushRedoLogs();
            executed = true;
          } catch (Exception e) {
            return new CheckResult(false, e.getMessage());
          }
        }
        return new CheckResult(true, "Sent Articles data");
      }
    };
  }

  private void assertUsersTableContents() {
    List<Map<String, Object>> events = new ArrayList<>();
    events.add(
        Map.of(
            "id",
            1,
            "name",
            "Tester Kumar",
            "age",
            30,
            "subscribed",
            false,
            "plan",
            "A",
            "startDate",
            Date.parseDate("2023-01-01")));
    events.add(
        Map.of(
            "id",
            3,
            "name",
            "Tester Gupta",
            "age",
            50,
            "subscribed",
            false,
            "plan",
            "Z",
            "startDate",
            Date.parseDate("2023-06-07")));
    events.add(
        Map.of(
            "id",
            4,
            "name",
            "Tester",
            "age",
            38,
            "subscribed",
            true,
            "plan",
            "D",
            "startDate",
            Date.parseDate("2023-09-10")));
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("select * from Users where id in (1, 3, 4)"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertMovieTableContents() {
    List<Map<String, Object>> events = new ArrayList<>();
    events.add(
        Map.of(
            "id",
            1,
            "name",
            "movie1",
            "startTime",
            Timestamp.parseTimestamp("2023-01-01T12:12:12Z")));
    events.add(
        Map.of(
            "id",
            2,
            "name",
            "movie2",
            "startTime",
            Timestamp.parseTimestamp("2023-11-25T17:10:12Z")));
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select id, name, startTime from Movie where id in (1, 2)"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);

    ImmutableList<Struct> numericVals =
        spannerResourceManager.runQuery("select actor from Movie order by id");
    Assert.assertEquals(12345.09876, numericVals.get(0).getBigDecimal(0).doubleValue(), 0.00000001);
    Assert.assertEquals(931.5123, numericVals.get(1).getBigDecimal(0).doubleValue(), 0.00000001);
  }

  private void assertAuthorsTable() {
    List<Map<String, Object>> events = new ArrayList<>();
    events.add(Map.of("author_id", 1, "name", "a1"));
    events.add(Map.of("author_id", 2, "name", "a2"));
    events.add(Map.of("author_id", 3, "name", "a3"));
    events.add(Map.of("author_id", 4, "name", "a4"));
    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Authors"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertBooksTable() {
    List<Map<String, Object>> events = new ArrayList<>();
    events.add(Map.of("id", 1, "title", "Book005", "author_id", 3));
    events.add(Map.of("id", 2, "title", "Book002", "author_id", 3));
    events.add(Map.of("id", 3, "title", "Book004", "author_id", 4));
    events.add(Map.of("id", 4, "title", "Book005", "author_id", 2));
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("select * from Books@{FORCE_INDEX=author_id_6}"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private void assertArticlesTable() {
    List<Map<String, Object>> events = new ArrayList<>();
    events.add(
        Map.of(
            "id",
            1,
            "name",
            "Article001",
            "published_date",
            Date.parseDate("2024-01-01"),
            "author_id",
            1));
    events.add(
        Map.of(
            "id",
            2,
            "name",
            "Article002",
            "published_date",
            Date.parseDate("2024-01-01"),
            "author_id",
            1));
    events.add(
        Map.of(
            "id",
            3,
            "name",
            "Article004",
            "published_date",
            Date.parseDate("2024-01-01"),
            "author_id",
            4));
    events.add(
        Map.of(
            "id",
            4,
            "name",
            "Article005",
            "published_date",
            Date.parseDate("2024-01-01"),
            "author_id",
            3));
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("select * from Articles@{FORCE_INDEX=author_id}"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }
}
