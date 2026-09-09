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

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class OracleSeparateShadowTableDatabaseMixedIT extends DataStreamToSpannerITBase {
  private static final String SESSION_FILE_RESOURCE =
      "oracle/OracleDataStreamToSpannerMixedIT/oracle-session.json";

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleSeparateShadowTableDatabaseMixedIT/oracle-google_standard_sql-spanner-schema.sql";

  private static HashSet<OracleSeparateShadowTableDatabaseMixedIT> testInstances = new HashSet<>();
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
    synchronized (OracleSeparateShadowTableDatabaseMixedIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity(System.getProperty("privateConnectivity"))
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
            "oracle/OracleSeparateShadowTableDatabaseMixedIT/oracle-schema.sql",
            oracleUser);

        OracleSource jdbcSource =
            OracleSource.builder(
                    oracleResourceManager.getHost(),
                    oracleUser,
                    SharedOracleLiveITInstance.ORACLE_PASSWORD,
                    oracleResourceManager.getPort(),
                    oracleResourceManager.getDatabaseName())
                .setAllowedTables(
                    Map.of(oracleUser.toUpperCase(), List.of("Authors", "Books", "Genre")))
                .build();

        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                SESSION_FILE_RESOURCE,
                null,
                "OracleSeparateShadowTableDatabaseMixedIT",
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
    for (OracleSeparateShadowTableDatabaseMixedIT instance : testInstances) {
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
  public void mixedMigrationTest() {
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeInitialData(),
                    SpannerRowsCheck.builder(spannerResourceManager, "Authors")
                        .setMinRows(4)
                        .setMaxRows(4)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, "Books")
                        .setMinRows(3)
                        .setMaxRows(3)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, "Genre")
                        .setMinRows(1)
                        .setMaxRows(1)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(45)), conditionCheck);

    assertThatResult(result).meetsConditions();

    assertAuthorsTableContents();
    assertBooksTableContents();
  }

  private ConditionCheck writeInitialData() {
    return new ConditionCheck() {
      boolean executed = false;

      @Override
      protected String getDescription() {
        return "Write JDBC data";
      }

      @Override
      protected CheckResult check() {
        if (!executed) {

          List<Map<String, Object>> authorRows = new ArrayList<>();
          authorRows.add(Map.of("author_id", 4, "full_name", "Stephen King"));
          authorRows.add(Map.of("author_id", 1, "full_name", "Jane Austen"));
          authorRows.add(Map.of("author_id", 2, "full_name", "Charles Dickens"));
          authorRows.add(Map.of("author_id", 3, "full_name", "Leo Tolstoy"));

          List<Map<String, Object>> bookRows = new ArrayList<>();
          bookRows.add(Map.of("id", 1, "title", "Pride and Prejudice", "author_id", 1));
          bookRows.add(Map.of("id", 2, "title", "Oliver Twist", "author_id", 2));
          bookRows.add(Map.of("id", 3, "title", "War and Peace", "author_id", 3));

          List<Map<String, Object>> genreRows = new ArrayList<>();
          genreRows.add(Map.of("genre_id", 1, "name", "Fiction"));

          for (Map<String, Object> r : authorRows) {
            try {
              executeOracleSql(
                  oracleResourceManager,
                  String.format(
                      "INSERT INTO \"Authors\"(\"author_id\",\"name\") VALUES (%d, '%s')",
                      r.get("author_id"), r.get("full_name")),
                  oracleUser);
            } catch (Exception e) {
              return new CheckResult(false, "Failed to insert");
            }
          }
          for (Map<String, Object> r : bookRows) {
            try {
              executeOracleSql(
                  oracleResourceManager,
                  String.format(
                      "INSERT INTO \"Books\"(\"id\",\"title\",\"author_id\") VALUES (%d, '%s', %d)",
                      r.get("id"), r.get("title"), r.get("author_id")),
                  oracleUser);
            } catch (Exception e) {
              return new CheckResult(false, "Failed to insert");
            }
          }
          for (Map<String, Object> r : genreRows) {
            try {
              executeOracleSql(
                  oracleResourceManager,
                  String.format(
                      "INSERT INTO \"Genre\"(\"genre_id\",\"name\") VALUES (%d, '%s')",
                      r.get("genre_id"), r.get("name")),
                  oracleUser);
            } catch (Exception e) {
              return new CheckResult(false, "Failed to insert");
            }
          }

          try {
            executeOracleSql(oracleResourceManager, "COMMIT", oracleUser);
          } catch (Exception e) {
            return new CheckResult(false, "Failed to insert");
          }
          SharedOracleLiveITInstance.flushRedoLogs();
          executed = true;
        }
        return new CheckResult(true, "Sent to Oracle.");
      }
    };
  }

  private void assertAuthorsTableContents() {
    List<Map<String, Object>> authorEvents = new ArrayList<>();
    authorEvents.add(Map.of("author_id", 4, "full_name", "Stephen King"));
    authorEvents.add(Map.of("author_id", 1, "full_name", "Jane Austen"));
    authorEvents.add(Map.of("author_id", 2, "full_name", "Charles Dickens"));
    authorEvents.add(Map.of("author_id", 3, "full_name", "Leo Tolstoy"));
    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Authors"))
        .hasRecordsUnorderedCaseInsensitiveColumns(authorEvents);
  }

  private void assertBooksTableContents() {
    List<Map<String, Object>> bookEvents = new ArrayList<>();
    bookEvents.add(Map.of("id", 1, "title", "Pride and Prejudice"));
    bookEvents.add(Map.of("id", 2, "title", "Oliver Twist"));
    bookEvents.add(Map.of("id", 3, "title", "War and Peace"));
    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select id, title from Books"))
        .hasRecordsUnorderedCaseInsensitiveColumns(bookEvents);
  }
}
