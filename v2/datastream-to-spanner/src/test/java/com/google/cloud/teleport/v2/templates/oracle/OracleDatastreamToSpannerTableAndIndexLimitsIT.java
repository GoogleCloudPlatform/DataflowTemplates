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

import static java.util.Map.entry;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
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
public class OracleDatastreamToSpannerTableAndIndexLimitsIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(OracleDatastreamToSpannerTableAndIndexLimitsIT.class);

  private static final String ORACLE_DDL_RESOURCE =
      "oracle/OracleDatastreamToSpannerTableAndIndexLimitsIT/oracle-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleDatastreamToSpannerTableAndIndexLimitsIT/oracle-google_standard_sql-spanner-schema.sql";
  private static final String SESSION_FILE =
      "oracle/OracleDatastreamToSpannerTableAndIndexLimitsIT/session.json";

  private static final String LARGE_KEY_TABLE = "LargeKey";
  private static final String LARGE_CELL_TABLE = "LargeCell";
  private static final List<String> TABLES = List.of(LARGE_KEY_TABLE, LARGE_CELL_TABLE);

  private static CloudOracleResourceManager oracleResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;
  private static HashSet<OracleDatastreamToSpannerTableAndIndexLimitsIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (OracleDatastreamToSpannerTableAndIndexLimitsIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        LOG.info("Setting up Oracle resource manager...");
        oracleResourceManager = setUpOracleResourceManager();
        LOG.info("Oracle resource manager created with URI: {}", oracleResourceManager.getUri());

        try {
        } catch (Exception e) {
        }
        try {
        } catch (Exception e) {
        }

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
        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity("datastream-connect-2")
                .build();
        LOG.info("Datastream resource manager created");

        LOG.info("Executing Oracle DDL script...");
        executeSqlScript(oracleResourceManager, ORACLE_DDL_RESOURCE);

        LOG.info("Creating Spanner DDL...");
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

        // Pre-insert testing data for LargeCell
        try (Connection conn =
                DriverManager.getConnection(
                    oracleResourceManager.getUri(),
                    oracleResourceManager.getUsername(),
                    oracleResourceManager.getPassword());
            PreparedStatement pstmt =
                conn.prepareStatement(
                    "INSERT INTO \"LargeCell\" (\"id\", \"max_string_col_to_bytes\","
                        + " \"max_string_col_to_str\") VALUES (1, ?, ?)"); ) {

          byte[] bytes = new byte[4000];
          Arrays.fill(bytes, (byte) 'b');
          pstmt.setBytes(1, bytes);
          pstmt.setString(2, new String(bytes, StandardCharsets.UTF_8));
          pstmt.executeUpdate();
        } catch (Exception e) {
          LOG.error("Failed to insert LargeCell locally: ", e);
          throw new RuntimeException("Failed to insert LargeCell", e);
        }

        // Force a log switch and insert to LargeCell.
        try (Connection conn =
                DriverManager.getConnection(
                    "jdbc:oracle:thin:@"
                        + oracleResourceManager.getHost()
                        + ":"
                        + oracleResourceManager.getPort()
                        + "XEPDB1",
                    "system",
                    "TestPassword123");
            Statement stmt = conn.createStatement(); ) {
          flushOracleRedoLogs(null);
        } catch (Exception e) {
          LOG.warn("Failed to switch log file natively: ", e);
        }

        LOG.info("Generating session file content...");
        String sessionFileContent =
            generateSessionFile(
                1,
                oracleResourceManager.getDatabaseName(),
                spannerResourceManager.getDatabaseId(),
                TABLES,
                SESSION_FILE);
        OracleSource oracleSource =
            OracleSource.builder(
                    oracleResourceManager.getHost(),
                    oracleResourceManager.getUsername(),
                    oracleResourceManager.getPassword(),
                    oracleResourceManager.getPort(),
                    oracleResourceManager.getDatabaseName())
                .setAllowedTables(Map.of(oracleResourceManager.getUsername().toUpperCase(), TABLES))
                .build();

        LOG.info("Launching Dataflow job...");
        jobInfo =
            launchDataflowJob(
                "oracle-table-and-index-limits",
                null,
                null,
                "datastream-to-spanner-table-and-index-limits",
                spannerResourceManager,
                pubsubResourceManager,
                Map.of("dlqMaxRetryCount", "1", "inputFileFormat", "avro"),
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                sessionFileContent,
                oracleSource);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    LOG.info("Cleaning up resources...");
    for (OracleDatastreamToSpannerTableAndIndexLimitsIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        oracleResourceManager,
        spannerResourceManager,
        gcsResourceManager,
        pubsubResourceManager,
        datastreamResourceManager);
  }

  @Test
  public void testKeySize() {
    assertThatPipeline(jobInfo).isRunning();

    LOG.info("Waiting for pipeline to process data for key size test...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                SpannerRowsCheck.builder(spannerResourceManager, LARGE_KEY_TABLE)
                    .setMinRows(1)
                    .build());
    assertThatResult(result).meetsConditions();
    List<Struct> rows = null;
    try {
      rows =
          spannerResourceManager.readTableRecords(LARGE_KEY_TABLE, List.of("pk_col1", "pk_col2"));
    } catch (Exception e) {
      LOG.error("Exception while reading spanner rows from {}", LARGE_KEY_TABLE, e);
      throw e;
    }
    SpannerAsserts.assertThatStructs(rows)
        .hasRecordsUnorderedCaseInsensitiveColumns(
            List.of(
                Map.ofEntries(
                    // `com.google.cloud.spanner.Value.StringImpl::valueToString`, which truncates
                    // strings to 33 characters
                    entry("pk_col1", "A".repeat(33) + "..."),
                    entry("pk_col2", "B".repeat(33) + "..."))));
  }

  @Test
  public void testCellSize() {
    assertThatPipeline(jobInfo).isRunning();

    LOG.info("Waiting for pipeline to process data for cell size test...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                SpannerRowsCheck.builder(spannerResourceManager, LARGE_CELL_TABLE)
                    .setMinRows(1)
                    .build());
    assertThatResult(result).meetsConditions();
    List<Struct> rows = null;
    try {
      rows = spannerResourceManager.readTableRecords(LARGE_CELL_TABLE, List.of("id"));
    } catch (Exception e) {
      LOG.error("Exception while reading spanner rows from {}", LARGE_CELL_TABLE, e);
      throw e;
    }
    SpannerAsserts.assertThatStructs(rows)
        .hasRecordsUnorderedCaseInsensitiveColumns(List.of(Map.ofEntries(entry("id", 1))));
  }
}
