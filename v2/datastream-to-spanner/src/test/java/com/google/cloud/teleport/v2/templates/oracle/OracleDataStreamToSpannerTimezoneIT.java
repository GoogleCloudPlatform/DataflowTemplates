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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
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

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class OracleDataStreamToSpannerTimezoneIT extends DataStreamToSpannerITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(OracleDataStreamToSpannerTimezoneIT.class);

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleDataStreamToSpannerTimezoneIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";
  private static final String ORACLE_DDL_RESOURCE =
      "oracle/OracleDataStreamToSpannerTimezoneIT/oracle-schema.sql";

  private static final String TABLE1 = "DateData";

  private static HashSet<OracleDataStreamToSpannerTimezoneIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  public static GcsResourceManager gcsResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;
  private static String oracleUser;

  @Before
  public void setUp() throws Exception {
    skipBaseCleanup = true;
    synchronized (OracleDataStreamToSpannerTimezoneIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        try {
          setUpSharedResources();
        } catch (Exception e) {
          LOG.error("Setup failed, cleaning up the resources created so far.", e);
          try {
            cleanUpSharedResources(List.of(this));
          } catch (RuntimeException cleanupError) {
            e.addSuppressed(cleanupError);
          }
          throw e;
        }
      }
    }
  }

  /** Creates the shared resources and launches the Dataflow job (sets {@link #jobInfo}). */
  private void setUpSharedResources() throws Exception {
    spannerResourceManager = setUpSpannerResourceManager();
    pubsubResourceManager = setUpPubSubResourceManager();
    gcsResourceManager = setUpSpannerITGcsResourceManager();

    oracleUser = SharedOracleLiveITContainer.setupOracleIsolatedUser();

    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity("datastream-connect-2")
            .build();

    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    SharedOracleLiveITContainer.executeSqlScript(oracleUser, ORACLE_DDL_RESOURCE);
    SharedOracleLiveITContainer.flushRedoLogs();

    OracleSource oracleSource =
        SharedOracleLiveITContainer.oracleSourceBuilder(oracleUser)
            .setAllowedTables(Map.of(oracleUser.toUpperCase(), List.of("DateData")))
            .build();

    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            "TimezoneIT",
            spannerResourceManager,
            pubsubResourceManager,
            new HashMap<>() {
              {
                put("inputFileFormat", "avro");
                put("workerMachineType", "n1-standard-4");
              }
            },
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            null,
            oracleSource);
  }

  @AfterClass
  public static void cleanUp() {
    cleanUpSharedResources(testInstances);
  }

  /**
   * Cleans up the Dataflow jobs of the given test instances, the shared resource managers and the
   * isolated Oracle user, then resets them so that a later setup (after a failed one) starts clean.
   */
  private static void cleanUpSharedResources(
      Collection<OracleDataStreamToSpannerTimezoneIT> instances) {
    try {
      SharedOracleLiveITContainer.cleanUpTestResources(
          instances,
          Arrays.asList(oracleUser),
          spannerResourceManager,
          pubsubResourceManager,
          gcsResourceManager,
          datastreamResourceManager);
    } finally {
      spannerResourceManager = null;
      pubsubResourceManager = null;
      gcsResourceManager = null;
      datastreamResourceManager = null;
      oracleUser = null;
    }
  }

  @Test
  public void testTimezoneHandling() {
    ConditionCheck insertData =
        new ConditionCheck() {
          @Override
          protected String getDescription() {
            return "Insert data into Oracle";
          }

          @Override
          protected CheckResult check() {
            try {
              SharedOracleLiveITContainer.executeSql(
                  oracleUser,
                  "INSERT INTO \"DateData\" (\"id\", \"timestamp_column\", \"datetime_column\")"
                      + " VALUES (1, TIMESTAMP '2024-02-02 10:00:00.000000', TIMESTAMP '2024-02-02"
                      + " 20:00:00.000000')");
              SharedOracleLiveITContainer.executeSql(
                  oracleUser,
                  "INSERT INTO \"DateData\" (\"id\", \"timestamp_column\", \"datetime_column\")"
                      + " VALUES (2, TIMESTAMP '2024-02-02 20:00:00.000000', TIMESTAMP '2024-02-03"
                      + " 06:00:00.000000')");
              SharedOracleLiveITContainer.flushRedoLogs();
              return new CheckResult(true, "Data inserted successfully");
            } catch (Exception e) {
              LOG.error("Failed to insert data into Oracle", e);
              return new CheckResult(false, e.getMessage());
            }
          }
        };

    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    insertData,
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE1)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build()))
            .build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(15)), conditionCheck);

    assertThatResult(result).meetsConditions();
    assertUsersBackfillContents();
  }

  private void assertUsersBackfillContents() {
    List<Map<String, Object>> expectedRows = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("id", 1);
    row.put("timestamp_column", "2024-02-02T10:00:00Z");
    row.put("datetime_column", "2024-02-02T20:00:00Z");
    expectedRows.add(row);

    row = new HashMap<>();
    row.put("id", 2);
    row.put("timestamp_column", "2024-02-02T20:00:00Z");
    row.put("datetime_column", "2024-02-03T06:00:00Z");
    expectedRows.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select id, timestamp_column, datetime_column from DateData"))
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedRows);
  }
}
