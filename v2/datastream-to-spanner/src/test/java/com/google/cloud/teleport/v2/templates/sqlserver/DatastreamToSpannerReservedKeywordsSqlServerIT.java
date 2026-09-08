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
package com.google.cloud.teleport.v2.templates.sqlserver;

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
import org.apache.beam.it.gcp.cloudsql.CloudSqlServerResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.SqlServerSource;
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

/**
 * An integration test for {@link DataStreamToSpanner} Flex template which tests a basic migration
 * on a simple schema with SQL Server reserved keywords.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DatastreamToSpannerReservedKeywordsSqlServerIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatastreamToSpannerReservedKeywordsSqlServerIT.class);

  private static final String SQLSERVER_DDL_RESOURCE =
      "sqlserver/DatastreamToSpannerReservedKeywordsSqlServerIT/sqlserver-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "sqlserver/DatastreamToSpannerReservedKeywordsSqlServerIT/spanner-schema.sql";
  private static final String SESSION_FILE =
      "sqlserver/DatastreamToSpannerReservedKeywordsSqlServerIT/session.json";

  private CloudSqlServerResourceManager msSqlResourceManager;
  private SpannerResourceManager spannerResourceManager;
  private GcsResourceManager gcsResourceManager;
  private PubsubResourceManager pubsubResourceManager;
  private DatastreamResourceManager datastreamResourceManager;

  @Before
  public void setUp() throws IOException {
    LOG.info("Setting up SQL Server resource manager...");
    msSqlResourceManager = setUpSqlServerResourceManager();
    LOG.info("SQL Server resource manager created with URI: {}", msSqlResourceManager.getUri());
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
    datastreamResourceManager = setUpDatastreamResourceManager();
    LOG.info("Datastream resource manager created");
  }

  @After
  public void cleanUp() {
    LOG.info("Cleaning up resources...");
    ResourceManagerUtils.cleanResources(
        msSqlResourceManager,
        spannerResourceManager,
        gcsResourceManager,
        pubsubResourceManager,
        datastreamResourceManager);
  }

  @Test
  public void testSqlServerReservedKeywords() throws Exception {
    LOG.info("Executing SQL Server DDL script...");
    executeSqlScript(msSqlResourceManager, SQLSERVER_DDL_RESOURCE);
    LOG.info("Creating Spanner DDL...");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    LOG.info("Generating session file content...");
    String sessionFileContent =
        generateSessionFile(
            1,
            msSqlResourceManager.getDatabaseName(),
            spannerResourceManager.getDatabaseId(),
            List.of("true"),
            SESSION_FILE);

    Map<String, String> jobParams = new HashMap<>();
    jobParams.put("dlqMaxRetryCount", "1");
    jobParams.put("datastreamSourceType", "sqlserver");

    SqlServerSource sqlServerSource =
        SqlServerSource.builder(
                msSqlResourceManager.getHost(),
                msSqlResourceManager.getUsername(),
                msSqlResourceManager.getPassword(),
                msSqlResourceManager.getPort(),
                msSqlResourceManager.getDatabaseName())
            .setAllowedTables(Map.of("dbo", List.of("true")))
            .build();

    LOG.info("Launching Dataflow job...");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            "sqlserver-reserved-keywords",
            null,
            null,
            "sqlserver-datastream-to-spanner-reserved-keywords",
            spannerResourceManager,
            pubsubResourceManager,
            jobParams,
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            sessionFileContent,
            sqlServerSource);
    assertThatPipeline(jobInfo).isRunning();

    LOG.info("Waiting for pipeline to process data for keywords test...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)),
                SpannerRowsCheck.builder(spannerResourceManager, "`true`").setMinRows(1).build());
    assertThatResult(result).meetsConditions();
    List<Struct> rows =
        spannerResourceManager.readTableRecords("true", List.of("id", "ALL", "AND", "AS"));

    SpannerAsserts.assertThatStructs(rows)
        .hasRecordsUnorderedCaseInsensitiveColumns(
            List.of(
                Map.of(
                    "id", 1L,
                    "ALL", "all",
                    "AND", "and",
                    "AS", "as")));
  }
}
