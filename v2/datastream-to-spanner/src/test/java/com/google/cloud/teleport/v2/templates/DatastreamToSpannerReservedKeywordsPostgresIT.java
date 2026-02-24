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

import static java.util.Map.entry;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
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
import org.apache.beam.it.gcp.cloudsql.CloudPostgresResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.PostgresqlSource;
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
 * on a simple schema with reserved keywords.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DatastreamToSpannerReservedKeywordsPostgresIT extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatastreamToSpannerReservedKeywordsPostgresIT.class);

  private static final String POSTGRESQL_DDL_RESOURCE =
      "ReservedKeywordsPostgresIT/postgresql-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "ReservedKeywordsPostgresIT/spanner-schema.sql";
  private static final String SESSION_FILE = "ReservedKeywordsPostgresIT/session.json";

  private CloudPostgresResourceManager postgresResourceManager;
  private SpannerResourceManager spannerResourceManager;
  private GcsResourceManager gcsResourceManager;
  private PubsubResourceManager pubsubResourceManager;
  private DatastreamResourceManager datastreamResourceManager;
  private String publicationName;
  private String replicationSlotName;

  @Before
  public void setUp() throws IOException {
    LOG.info("Setting up Postgres resource manager...");
    postgresResourceManager = CloudPostgresResourceManager.builder(testName).build();
    LOG.info("Postgres resource manager created with URI: {}", postgresResourceManager.getUri());
    LOG.info("Setting up Spanner resource manager...");
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.POSTGRESQL).build();
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

  @After
  public void cleanUp() {
    LOG.info("Cleaning up resources...");

    // It is important to clean up Datastream before trying to drop the replication slot.
    ResourceManagerUtils.cleanResources(datastreamResourceManager);

    ResourceManagerUtils.cleanResources(
        postgresResourceManager, spannerResourceManager, gcsResourceManager, pubsubResourceManager);
  }

  @Test
  public void testPostgresReservedKeywords() throws Exception {
    LOG.info("Executing Postgres DDL script...");
    executeSqlScript(postgresResourceManager, POSTGRESQL_DDL_RESOURCE);

    CloudPostgresResourceManager.ReplicationInfo replicationInfo =
        postgresResourceManager.createLogicalReplication();
    this.publicationName = replicationInfo.getPublicationName();
    this.replicationSlotName = replicationInfo.getReplicationSlotName();

    LOG.info("Creating Spanner DDL...");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    LOG.info("Generating session file content...");
    String sessionFileContent =
        Resources.toString(Resources.getResource(SESSION_FILE), StandardCharsets.UTF_8);
    sessionFileContent =
        sessionFileContent
            .replaceAll("SP_DATABASE", spannerResourceManager.getDatabaseId())
            .replaceAll("SRC_DATABASE", "public");
    PostgresqlSource postgresqlSource =
        PostgresqlSource.builder(
                postgresResourceManager.getHost(),
                postgresResourceManager.getUsername(),
                postgresResourceManager.getPassword(),
                postgresResourceManager.getPort(),
                postgresResourceManager.getDatabaseName(),
                this.replicationSlotName,
                this.publicationName)
            .setAllowedTables(Map.of("public", List.of("true")))
            .build();

    LOG.info("Launching Dataflow job...");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            "postgres-reserved-keywords",
            null,
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
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(20)),
                SpannerRowsCheck.builder(spannerResourceManager, "\"true\"").setMinRows(1).build());
    assertThatResult(result).meetsConditions();
    List<Struct> rows = null;
    try {
      // Unlike SpannerRowsCheck which translates to a DML, readTableRecords leads to `rpc.read`,
      // which expects raw identifiers.
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

  private void executeSqlScript(CloudPostgresResourceManager resourceManager, String resourceName)
      throws IOException {
    // Read DDL and replace placeholders
    String ddl =
        String.join(
            " ", Resources.readLines(Resources.getResource(resourceName), StandardCharsets.UTF_8));

    // Execute script
    ddl = ddl.trim();
    List<String> ddls = Arrays.stream(ddl.split(";")).toList();
    for (String d : ddls) {
      if (!d.isBlank()) {
        try {
          if (d.toLowerCase().trim().startsWith("select")) {
            resourceManager.runSQLQuery(d);
          } else {
            resourceManager.runSQLUpdate(d);
          }
        } catch (Exception e) {
          LOG.error("Exception while executing PG ddl {}", d, e);
          throw e;
        }
      }
    }
  }
}
