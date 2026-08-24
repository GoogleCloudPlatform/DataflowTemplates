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
import org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager;
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
  public static CloudOracleResourceManager oracleResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;
  public static CloudOracleResourceManager cloudOracleSysUser;

  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (OracleDataStreamToSpannerTimezoneIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = setUpSpannerResourceManager();
        pubsubResourceManager = setUpPubSubResourceManager();
        gcsResourceManager = setUpSpannerITGcsResourceManager();

        oracleResourceManager = setUpOracleResourceManager();
        CloudOracleResourceManager.Builder sysBuilder =
            CloudOracleResourceManager.builder(testName);
        sysBuilder.setPassword("TestPassword123");
        sysBuilder.setHost("" + System.getProperty("hostIp") + "");
        sysBuilder.setPort(1521);
        sysBuilder.setUsername("system");
        sysBuilder.setDatabaseName("XEPDB1");
        cloudOracleSysUser = (CloudOracleResourceManager) sysBuilder.build();

        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity("datastream-connect-2")
                .build();

        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

        try {
          oracleResourceManager.runSQLUpdate("DROP TABLE \"DateData\"");
        } catch (Exception e) {
          // ignore
        }

        executeSqlScript(oracleResourceManager, ORACLE_DDL_RESOURCE);

        // Explicit Log flush
        flushOracleRedoLogs(cloudOracleSysUser);

        OracleSource oracleSource =
            OracleSource.builder(
                    oracleResourceManager.getHost(),
                    oracleResourceManager.getUsername(),
                    oracleResourceManager.getPassword(),
                    oracleResourceManager.getPort(),
                    oracleResourceManager.getDatabaseName())
                .setAllowedTables(
                    Map.of(oracleResourceManager.getUsername().toUpperCase(), List.of("DateData")))
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
                  }
                },
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                null,
                oracleSource);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (OracleDataStreamToSpannerTimezoneIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        pubsubResourceManager,
        gcsResourceManager,
        oracleResourceManager,
        cloudOracleSysUser,
        datastreamResourceManager);
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
              oracleResourceManager.runSQLUpdate(
                  "INSERT INTO \"DateData\" (\"id\", \"timestamp_column\", \"datetime_column\") VALUES (1, TIMESTAMP '2024-02-02 10:00:00.000000', TIMESTAMP '2024-02-02 20:00:00.000000')");
              oracleResourceManager.runSQLUpdate(
                  "INSERT INTO \"DateData\" (\"id\", \"timestamp_column\", \"datetime_column\") VALUES (2, TIMESTAMP '2024-02-02 20:00:00.000000', TIMESTAMP '2024-02-03 06:00:00.000000')");

              flushOracleRedoLogs(cloudOracleSysUser);
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
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(JOB_START_PROCESSING_WAIT_MINUTES)),
                conditionCheck);

    assertThatResult(result).meetsConditions();
    assertUsersBackfillContents();
  }

  private void assertUsersBackfillContents() {
    List<Map<String, Object>> expectedRows = new ArrayList<>();

    Map<String, Object> row = new HashMap<>();
    row.put("id", 1);
    row.put("timestamp_column", "2024-02-02T00:00:00Z");
    row.put("datetime_column", "2024-02-02T10:00:00Z");
    expectedRows.add(row);

    row = new HashMap<>();
    row.put("id", 2);
    row.put("timestamp_column", "2024-02-02T10:00:00Z");
    row.put("datetime_column", "2024-02-02T20:00:00Z");
    expectedRows.add(row);

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery(
                "select id, timestamp_column, datetime_column from DateData"))
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedRows);
  }
}
