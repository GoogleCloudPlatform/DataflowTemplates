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
public class OracleSeparateShadowTableDatabaseStringOverridesIT extends DataStreamToSpannerITBase {

  private static final String ORACLE_TABLE = "person1";
  private static final String SPANNER_TABLE = "human1";

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleSeparateShadowTableDatabaseStringOverridesIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";

  private static PipelineLauncher.LaunchInfo jobInfo;
  private static HashSet<OracleSeparateShadowTableDatabaseStringOverridesIT> testInstances =
      new HashSet<>();

  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager shadowSpannerResourceManager;
  public static GcsResourceManager gcsResourceManager;
  public static SpannerOracleResourceManager oracleResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;
  private static String oracleUser;

  @Before
  public void setUp() throws Exception {
    skipBaseCleanup = true;
    synchronized (OracleSeparateShadowTableDatabaseStringOverridesIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity(
                    System.getProperty("privateConnectivity", "datastream-connect-2"))
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
            "oracle/OracleSeparateShadowTableDatabaseStringOverridesIT/oracle-schema.sql",
            oracleUser);

        OracleSource jdbcSource =
            OracleSource.builder(
                    oracleResourceManager.getHost(),
                    oracleUser,
                    SharedOracleLiveITInstance.ORACLE_PASSWORD,
                    oracleResourceManager.getPort(),
                    oracleResourceManager.getDatabaseName())
                .setAllowedTables(Map.of(oracleUser.toUpperCase(), List.of("person1")))
                .build();

        Map<String, String> overridesMap = new HashMap<>();
        overridesMap.put("inputFileFormat", "avro");
        overridesMap.put("datastreamSourceType", "oracle");
        overridesMap.put("tableOverrides", "[{person1, human1}]");
        overridesMap.put("columnOverrides", "[{person1.first_name1, person1.name1}]");
        overridesMap.put(
            "shadowTableSpannerInstanceId", shadowSpannerResourceManager.getInstanceId());
        overridesMap.put(
            "shadowTableSpannerDatabaseId", shadowSpannerResourceManager.getDatabaseId());

        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                null,
                null,
                "OracleSeparateShadowTableDatabaseStringOverridesIT",
                spannerResourceManager,
                pubsubResourceManager,
                overridesMap,
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
    for (OracleSeparateShadowTableDatabaseStringOverridesIT instance : testInstances) {
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
  public void migrationTestWithRenameTableAndColumns() {
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    new org.apache.beam.it.conditions.ConditionCheck() {
                      boolean executed = false;

                      @Override
                      protected String getDescription() {
                        return "Insert records into Oracle";
                      }

                      @Override
                      protected CheckResult check() {
                        if (!executed) {
                          try {
                            executeOracleSql(
                                oracleResourceManager,
                                "INSERT INTO \"person1\" (\"first_name1\", \"last_name1\") VALUES"
                                    + " ('John', 'Doe')",
                                oracleUser);
                            executeOracleSql(
                                oracleResourceManager,
                                "INSERT INTO \"person1\" (\"first_name1\", \"last_name1\") VALUES"
                                    + " ('Alice', 'Johnson')",
                                oracleUser);
                            executeOracleSql(oracleResourceManager, "COMMIT", oracleUser);
                            SharedOracleLiveITInstance.flushRedoLogs();
                            executed = true;
                          } catch (Exception e) {
                            return new CheckResult(false, e.getMessage());
                          }
                        }
                        return new CheckResult(true, "Inserted successfully");
                      }
                    },
                    SpannerRowsCheck.builder(spannerResourceManager, SPANNER_TABLE)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(45)), conditionCheck);

    assertThatResult(result).meetsConditions();
    assertHumanTableContents();
  }

  private void assertHumanTableContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row1 = new HashMap<>();
    row1.put("name1", "John");
    row1.put("last_name1", "Doe");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("name1", "Alice");
    row2.put("last_name1", "Johnson");

    events.add(row1);
    events.add(row2);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("select name1, last_name1 from human1"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }
}
