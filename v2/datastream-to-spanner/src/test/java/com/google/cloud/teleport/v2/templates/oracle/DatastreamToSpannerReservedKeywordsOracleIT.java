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
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
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
public class DatastreamToSpannerReservedKeywordsOracleIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatastreamToSpannerReservedKeywordsOracleIT.class);

  private static final String ORACLE_DDL_RESOURCE =
      "oracle/DatastreamToSpannerReservedKeywordsOracleIT/oracle-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/DatastreamToSpannerReservedKeywordsOracleIT/oracle-google_standard_sql-spanner-schema.sql";
  private static final String SESSION_FILE =
      "oracle/DatastreamToSpannerReservedKeywordsOracleIT/session.json";

  private static final HashSet<DatastreamToSpannerReservedKeywordsOracleIT> testInstances =
      new HashSet<>();

  private static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;
  private static String oracleUser;

  /**
   * Setup resource managers once for all the tests of the class. They are shared by the test
   * instances and cleaned up in {@link #cleanUp()}.
   */
  @Before
  public void setUp() throws Exception {
    skipBaseCleanup = true;
    synchronized (DatastreamToSpannerReservedKeywordsOracleIT.class) {
      testInstances.add(this);
      if (spannerResourceManager == null) {
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

  /** Creates the shared resources (sets {@link #spannerResourceManager}). */
  private void setUpSharedResources() throws Exception {
    LOG.info("Setting up Oracle isolated user...");
    oracleUser = SharedOracleLiveITContainer.setupOracleIsolatedUser();

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
  }

  /**
   * Cancels the Dataflow jobs of all the test instances first, then cleans up the resource managers
   * the jobs were reading from and drops the isolated Oracle user.
   */
  @AfterClass
  public static void cleanUp() {
    LOG.info("Cleaning up resources...");
    cleanUpSharedResources(testInstances);
  }

  /**
   * Cleans up the Dataflow jobs of the given test instances, the shared resource managers and the
   * isolated Oracle user, then resets them so that a later setup (after a failed one) starts clean.
   */
  private static void cleanUpSharedResources(
      Collection<DatastreamToSpannerReservedKeywordsOracleIT> instances) {
    try {
      SharedOracleLiveITContainer.cleanUpTestResources(
          instances,
          Arrays.asList(oracleUser),
          spannerResourceManager,
          gcsResourceManager,
          pubsubResourceManager,
          datastreamResourceManager);
    } finally {
      spannerResourceManager = null;
      gcsResourceManager = null;
      pubsubResourceManager = null;
      datastreamResourceManager = null;
      oracleUser = null;
    }
  }

  @Test
  public void testOracleReservedKeywords() throws Exception {
    LOG.info("Executing Oracle DDL script...");
    SharedOracleLiveITContainer.executeSqlScript(oracleUser, ORACLE_DDL_RESOURCE);
    SharedOracleLiveITContainer.flushRedoLogs();

    LOG.info("Creating Spanner DDL...");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    LOG.info("Generating session file content...");
    String sessionFileContent =
        generateSessionFile(
            1,
            SharedOracleLiveITContainer.getInstance().getDatabaseName(),
            spannerResourceManager.getDatabaseId(),
            List.of("true"), // Table name
            SESSION_FILE);

    OracleSource oracleSource =
        SharedOracleLiveITContainer.oracleSourceBuilder(oracleUser)
            .setAllowedTables(Map.of(oracleUser.toUpperCase(), List.of("true")))
            .build();

    Map<String, String> jobParams = new HashMap<>();
    jobParams.put("inputFileFormat", "avro");
    jobParams.put("workerMachineType", "n1-standard-4");

    LOG.info("Launching Dataflow job...");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            "oracle-reserved-keywords",
            null,
            null,
            "datastream-to-spanner-reserved-keywords",
            spannerResourceManager,
            pubsubResourceManager,
            jobParams,
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            sessionFileContent,
            oracleSource);
    assertThatPipeline(jobInfo).isRunning();

    LOG.info("Waiting for pipeline to process data...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)),
                SpannerRowsCheck.builder(spannerResourceManager, "`true`").setMinRows(1).build());
    assertThatResult(result).meetsConditions();

    List<Struct> rows = null;
    try {
      rows = spannerResourceManager.readTableRecords("true", List.of("id", "ALL", "AND", "AS"));
    } catch (Exception e) {
      LOG.error("Exception while reading spanner rows from `true`", e);
      throw e;
    }

    SpannerAsserts.assertThatStructs(rows)
        .hasRecordsUnorderedCaseInsensitiveColumns(
            List.of(
                Map.ofEntries(
                    entry("id", 1), entry("ALL", "all"), entry("AND", "and"), entry("AS", "as"))));
  }
}
