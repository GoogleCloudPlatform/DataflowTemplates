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
import java.time.Duration;
import java.util.HashMap;
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
import org.junit.After;
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

  private CloudOracleResourceManager oracleResourceManager;
  private SpannerResourceManager spannerResourceManager;
  private GcsResourceManager gcsResourceManager;
  private PubsubResourceManager pubsubResourceManager;
  private DatastreamResourceManager datastreamResourceManager;

  @Before
  public void setUp() throws IOException {
    LOG.info("Setting up Oracle resource manager...");
    oracleResourceManager = setUpOracleResourceManager();

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
            .setPrivateConnectivity("datastream-connect-2") /* from original */
            .build();
    LOG.info("Datastream resource manager created");
  }

  @After
  public void cleanUp() {
    LOG.info("Cleaning up resources...");
    ResourceManagerUtils.cleanResources(
        oracleResourceManager,
        spannerResourceManager,
        gcsResourceManager,
        pubsubResourceManager,
        datastreamResourceManager);
  }

  @Test
  public void testOracleReservedKeywords() throws Exception {
    LOG.info("Executing Oracle DDL script...");
    try {
    } catch (Exception e) {
      LOG.info("Table true does not exist or could not be dropped: " + e.getMessage());
    }
    executeSqlScript(oracleResourceManager, ORACLE_DDL_RESOURCE);

    flushOracleRedoLogs(oracleResourceManager);

    LOG.info("Creating Spanner DDL...");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    LOG.info("Generating session file content...");
    String sessionFileContent =
        generateSessionFile(
            1,
            oracleResourceManager.getDatabaseName(),
            spannerResourceManager.getDatabaseId(),
            List.of("true"), // Table name
            SESSION_FILE);

    OracleSource oracleSource =
        OracleSource.builder(
                oracleResourceManager.getHost(),
                oracleResourceManager.getUsername(),
                oracleResourceManager.getPassword(),
                oracleResourceManager.getPort(),
                oracleResourceManager.getDatabaseName())
            .setAllowedTables(
                Map.of(oracleResourceManager.getUsername().toUpperCase(), List.of("true")))
            .build();

    Map<String, String> jobParams = new HashMap<>();
    jobParams.put("inputFileFormat", "avro");

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
                createConfig(jobInfo, Duration.ofMinutes(20)),
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
