/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudPostgresResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.apache.beam.it.gcp.datastream.PostgresqlSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
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
 * on a simple schema with reserved keywords.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DatastreamToSpannerReservedKeywordsIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatastreamToSpannerReservedKeywordsIT.class);

  private static final String MYSQL_DDL_RESOURCE = "ReservedKeywordsIT/mysql-schema.sql";
  private static final String POSTGRESQL_DDL_RESOURCE = "ReservedKeywordsIT/postgresql-schema.sql";
  private static final String SPANNER_DDL_RESOURCE = "ReservedKeywordsIT/spanner-schema.sql";
  private static final String SESSION_FILE = "ReservedKeywordsIT/session.json";

  private CloudMySQLResourceManager mySQLResourceManager;
  private CloudPostgresResourceManager postgresResourceManager;
  private SpannerResourceManager spannerResourceManager;
  private GcsResourceManager gcsResourceManager;
  private PubsubResourceManager pubsubResourceManager;
  private DatastreamResourceManager datastreamResourceManager;

  @Before
  public void setUp() throws IOException {
    LOG.info("Setting up MySQL resource manager...");
    mySQLResourceManager = CloudMySQLResourceManager.builder(testName).build();
    LOG.info("MySQL resource manager created with URI: {}", mySQLResourceManager.getUri());
    LOG.info("Setting up Postgres resource manager...");
    postgresResourceManager = CloudPostgresResourceManager.builder(testName).build();
    LOG.info("Postgres resource manager created with URI: {}", postgresResourceManager.getUri());
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
            .build();
    LOG.info("Datastream resource manager created");
  }

  @After
  public void cleanUp() {
    LOG.info("Cleaning up resources...");
    ResourceManagerUtils.cleanResources(
        mySQLResourceManager,
        postgresResourceManager,
        spannerResourceManager,
        gcsResourceManager,
        pubsubResourceManager,
        datastreamResourceManager);
  }

  @Test
  public void testMySqlReservedKeywords() throws Exception {
    LOG.info("Executing MySQL DDL script...");
    executeSqlScript(mySQLResourceManager, MYSQL_DDL_RESOURCE);
    LOG.info("Creating Spanner DDL...");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    LOG.info("Generating session file content...");
    String sessionFileContent =
        generateSessionFile(
            1,
            mySQLResourceManager.getDatabaseName(),
            spannerResourceManager.getDatabaseId(),
            List.of("true"),
            SESSION_FILE);
    MySQLSource mySQLSource =
        MySQLSource.builder(
                mySQLResourceManager.getHost(),
                mySQLResourceManager.getUsername(),
                mySQLResourceManager.getPassword(),
                mySQLResourceManager.getPort())
            .setAllowedTables(Map.of(mySQLResourceManager.getDatabaseName(), List.of("true")))
            .build();

    LOG.info("Launching Dataflow job...");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            "mysql-reserved-keywords",
            SESSION_FILE,
            null,
            "datastream-to-spanner-reserved-keywords",
            spannerResourceManager,
            pubsubResourceManager,
            new HashMap<>(),
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            sessionFileContent,
            mySQLSource);
    assertThatPipeline(jobInfo).isRunning();

    LOG.info("Waiting for pipeline to process data...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(jobInfo, Duration.ofMinutes(20)),
                SpannerRowsCheck.builder(spannerResourceManager, "true").setMinRows(1).build());
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void testPostgresReservedKeywords() throws Exception {
    LOG.info("Executing Postgres DDL script...");
    executeSqlScript(postgresResourceManager, POSTGRESQL_DDL_RESOURCE);
    LOG.info("Creating Spanner DDL...");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    LOG.info("Generating session file content...");
    String sessionFileContent =
        generateSessionFile(
            1,
            postgresResourceManager.getDatabaseName(),
            spannerResourceManager.getDatabaseId(),
            List.of("true"),
            SESSION_FILE);
    PostgresqlSource postgresqlSource =
        PostgresqlSource.builder(
                postgresResourceManager.getHost(),
                postgresResourceManager.getUsername(),
                postgresResourceManager.getPassword(),
                postgresResourceManager.getPort(),
                postgresResourceManager.getDatabaseName(),
                "replication_slot",
                "publication")
            .setAllowedTables(Map.of("public", List.of("true")))
            .build();

    LOG.info("Launching Dataflow job...");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            "postgres-reserved-keywords",
            SESSION_FILE,
            null,
            "datastream-to-spanner-reserved-keywords",
            spannerResourceManager,
            pubsubResourceManager,
            new HashMap<>(),
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            sessionFileContent,
            postgresqlSource);
    assertThatPipeline(jobInfo).isRunning();

    LOG.info("Waiting for pipeline to process data...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(jobInfo, Duration.ofMinutes(20)),
                SpannerRowsCheck.builder(spannerResourceManager, "true").setMinRows(1).build());
    assertThatResult(result).meetsConditions();
  }

  private void executeSqlScript(CloudMySQLResourceManager resourceManager, String resourceName)
      throws IOException {
    String ddl =
        String.join(
            " ", Resources.readLines(Resources.getResource(resourceName), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    List<String> ddls = Arrays.stream(ddl.split(";")).toList();
    for (String d : ddls) {
      if (!d.isBlank()) {
        resourceManager.runSQLUpdate(d);
      }
    }
  }

  private void executeSqlScript(CloudPostgresResourceManager resourceManager, String resourceName)
      throws IOException {
    String ddl =
        String.join(
            " ", Resources.readLines(Resources.getResource(resourceName), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    List<String> ddls = Arrays.stream(ddl.split(";")).toList();
    for (String d : ddls) {
      if (!d.isBlank()) {
        resourceManager.runSQLUpdate(d);
      }
    }
  }
}
