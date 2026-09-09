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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.io.Resources;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class OracleSeparateShadowTableDatabaseFileOverridesIT extends DataStreamToSpannerITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(OracleSeparateShadowTableDatabaseFileOverridesIT.class);

  private static final String ORACLE_SCHEMA =
      "oracle/OracleSeparateShadowTableDatabaseFileOverridesIT/oracle-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleSeparateShadowTableDatabaseFileOverridesIT/oracle-google_standard_sql-spanner-schema.sql";
  private static final String OVERRIDE_FILE =
      "oracle/OracleSeparateShadowTableDatabaseFileOverridesIT/override.json";
  private static final String GCS_PATH_PREFIX = "OracleFileOverridesIT";
  private static final String ORACLE_TABLE = "person1";
  private static final String SPANNER_TABLE = "human1";
  private static SpannerOracleResourceManager oracleResourceManager;
  private static SpannerResourceManager shadowSpannerResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;
  private static boolean initialized = false;
  private static String oracleUser;

  private static HashSet<OracleSeparateShadowTableDatabaseFileOverridesIT> testInstances =
      new HashSet<>();

  @Before
  public void setUp() throws Exception {
    skipBaseCleanup = true;
    synchronized (OracleSeparateShadowTableDatabaseFileOverridesIT.class) {
      testInstances.add(this);
      if (!initialized) {
        oracleResourceManager = SharedOracleLiveITInstance.getInstance();
        oracleUser = SharedOracleLiveITInstance.setupOracleIsolatedUser();
        LOG.info("Provisioned isolated user: " + oracleUser);

        shadowSpannerResourceManager = setUpShadowSpannerResourceManager();
        spannerResourceManager = setUpSpannerResourceManager();
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        pubsubResourceManager = setUpPubSubResourceManager();
        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity("datastream-connect-2")
                .build();

        executeOracleSqlFileScript(oracleResourceManager, ORACLE_SCHEMA, oracleUser);
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

        gcsResourceManager.uploadArtifact(
            GCS_PATH_PREFIX + "/override.json", Resources.getResource(OVERRIDE_FILE).getPath());

        initialized = true;
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (OracleSeparateShadowTableDatabaseFileOverridesIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        shadowSpannerResourceManager,
        gcsResourceManager,
        pubsubResourceManager,
        datastreamResourceManager);
    SharedOracleLiveITInstance.dropUser(oracleUser);
  }

  @Test
  public void migrationTestWithRenameTableAndColumns() throws Exception {
    OracleSource oracleSource =
        OracleSource.builder(
                oracleResourceManager.getHost(),
                oracleUser,
                SharedOracleLiveITInstance.ORACLE_PASSWORD,
                oracleResourceManager.getPort(),
                oracleResourceManager.getDatabaseName())
            .setAllowedTables(Map.of(oracleUser.toUpperCase(), List.of(ORACLE_TABLE)))
            .build();

    Map<String, String> overridesMap = new HashMap<>();
    overridesMap.put("inputFileFormat", "avro");
    overridesMap.put("shadowTableSpannerInstanceId", shadowSpannerResourceManager.getInstanceId());
    overridesMap.put("shadowTableSpannerDatabaseId", shadowSpannerResourceManager.getDatabaseId());
    overridesMap.put(
        "schemaOverridesFilePath",
        getGcsPath(GCS_PATH_PREFIX + "/override.json", gcsResourceManager));
    overridesMap.put("workerMachineType", "n1-standard-4");

    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            GCS_PATH_PREFIX,
            spannerResourceManager,
            pubsubResourceManager,
            overridesMap,
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            null,
            oracleSource);

    assertThatPipeline(jobInfo).isRunning();

    ConditionCheck sendDataCondition =
        new ConditionCheck() {
          @Override
          public String getDescription() {
            return "Insert data into Oracle and flush logs";
          }

          @Override
          protected CheckResult check() {
            try {
              executeOracleSql(
                  oracleResourceManager,
                  "INSERT INTO \"person1\" (\"ID\", \"first_name1\", \"last_name1\") VALUES (1,"
                      + " 'John', 'Doe')",
                  oracleUser);
              executeOracleSql(
                  oracleResourceManager,
                  "INSERT INTO \"person1\" (\"ID\", \"first_name1\", \"last_name1\") VALUES (2,"
                      + " 'Alice', 'Johnson')",
                  oracleUser);
              SharedOracleLiveITInstance.flushRedoLogs();
              return new CheckResult(true, "Data inserted and logs flushed");
            } catch (Exception e) {
              LOG.error("Failed to insert data or flush logs", e);
              return new CheckResult(false, e.getMessage());
            }
          }
        };

    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    sendDataCondition,
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
