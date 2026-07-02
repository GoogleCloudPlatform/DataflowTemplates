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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.truth.Truth.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.dtsx.astra.sdk.db.AstraDBOpsClient;
import com.dtsx.astra.sdk.db.DbOpsClient;
import com.dtsx.astra.sdk.db.domain.Database;
import com.dtsx.astra.sdk.db.domain.DatabaseCreationRequest;
import com.dtsx.astra.sdk.db.domain.DatabaseStatusType;
import com.dtsx.astra.sdk.utils.ApiLocator;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.AstraConnectionConfig;
import com.google.gson.Gson;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link com.google.cloud.teleport.v2.templates.SourceDbToSpanner} from Astra
 * DB.
 */
@RunWith(JUnit4.class)
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(SourceDbToSpanner.class)
public class AstraDbToSpannerIT extends SourceDbToSpannerITBase implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(AstraDbToSpannerIT.class);

  private static final long NUM_ROWS = 50L;
  private static final String ASTRA_DB = "dataflow_integration_tests";
  private static final String ASTRA_DB_REGION = TestProperties.region();
  private static final String ASTRA_KS = "beam";

  private static final String[][] SCIENTISTS = {
    new String[] {"phys", "Einstein"},
    new String[] {"bio", "Darwin"},
    new String[] {"phys", "Copernicus"},
    new String[] {"bio", "Pasteur"},
    new String[] {"bio", "Curie"}
  };

  private String astraTable;

  private static DbOpsClient dbClient;
  private SpannerResourceManager spannerResourceManager;

  @Before
  public void setup() throws Exception {
    astraTable = "scientist_" + testId.replaceAll("-", "_");

    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION).maybeUseStaticInstance().build();

    // Create Spanner table
    String spannerDdl =
        String.format(
            "CREATE TABLE %s ("
                + " person_department STRING(MAX),"
                + " person_id INT64,"
                + " person_name STRING(MAX),"
                + ") PRIMARY KEY(person_department, person_id)",
            astraTable);
    spannerResourceManager.executeDdlStatement(spannerDdl);

    // Setup Astra Db
    createOrResumeAstraDatabase();
    // Setup Astra Data
    createAndPopulateTables();
    LOGGER.info("Initialization Successful.");
  }

  @Test
  public void testAstraDbToSpanner() throws IOException {
    // Generate shard.json
    AstraConnectionConfig astraConfig = new AstraConnectionConfig();
    astraConfig.setAstraToken(dbClient.getToken());
    astraConfig.setDatabaseId(dbClient.getDatabaseId());
    astraConfig.setKeySpace(ASTRA_KS);
    astraConfig.setAstraDbRegion(dbClient.get().getInfo().getRegion());

    String configContents = new Gson().toJson(astraConfig);
    artifactClient.createArtifact("input/shard.json", configContents);
    String sourceConfigURL = getGcsPath("input/shard.json", artifactClient);

    Map<String, String> jobParameters = new HashMap<>();
    jobParameters.put("sourceDbDialect", "ASTRA_DB");
    jobParameters.put("sourceConfigURL", sourceConfigURL);
    jobParameters.put("outputDirectory", getGcsPath("output", artifactClient));
    jobParameters.put("tables", astraTable);
    jobParameters.put(
        "ipConfiguration", "WORKER_IP_UNSPECIFIED"); // Require internet access for Astra API

    PipelineLauncher.LaunchInfo info =
        launchDataflowJob(testName, null, null, null, spannerResourceManager, jobParameters, null);
    LOGGER.info("Pipeline is now running.");

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult(result).isLaunchFinished();
    LOGGER.info("Destination Table has been populated.");

    // Verify the row count matches what was inserted
    List<Struct> rows =
        spannerResourceManager.readTableRecords(
            astraTable, List.of("person_department", "person_id", "person_name"));
    assertThat(rows).hasSize(Math.toIntExact(NUM_ROWS));

    // Verify row data content using SpannerAsserts like other ITs
    List<Map<String, Object>> expectedData = getExpectedData();

    SpannerAsserts.assertThatStructs(rows).hasRecordsUnorderedCaseInsensitiveColumns(expectedData);
  }

  private List<Map<String, Object>> getExpectedData() {
    List<Map<String, Object>> expectedData = new ArrayList<>();
    for (int i = 0; i < NUM_ROWS; i++) {
      int index = i % SCIENTISTS.length;
      Map<String, Object> expectedRow = new HashMap<>();
      expectedRow.put("person_department", SCIENTISTS[index][0]);
      expectedRow.put("person_id", i);
      expectedRow.put("person_name", SCIENTISTS[index][1]);
      expectedData.add(expectedRow);
    }
    return expectedData;
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager);

    if (dbClient != null) {
      try (CqlSession astraSession =
          CqlSession.builder()
              .withCloudSecureConnectBundle(
                  new ByteArrayInputStream(dbClient.downloadDefaultSecureConnectBundle()))
              .withAuthCredentials("token", dbClient.getToken())
              .withKeyspace(ASTRA_KS)
              .build()) {
        astraSession.execute(String.format("DROP TABLE IF EXISTS %s.%s;", ASTRA_KS, astraTable));
      } catch (Exception e) {
        LOGGER.warn("Failed to drop Astra table", e);
      }
    }
  }

  private static String test() {
    return "AstraCS:" + HASH;
  }

  @SuppressWarnings("BusyWait")
  private void createOrResumeAstraDatabase() throws InterruptedException {
    AstraDBOpsClient databasesClient = new AstraDBOpsClient(test());
    if (databasesClient.findByName(ASTRA_DB).findAny().isEmpty()) {
      LOGGER.info("Create a new Database {}", ASTRA_DB);
      databasesClient.create(
          DatabaseCreationRequest.builder()
              .name(ASTRA_DB)
              .keyspace(ASTRA_KS)
              .cloudRegion(ASTRA_DB_REGION)
              .build());
    } else {
      LOGGER.info("Database {} exists in source organization.", ASTRA_DB);
    }
    dbClient = databasesClient.databaseByName(ASTRA_DB);
    if (dbClient.get().getStatus() == DatabaseStatusType.HIBERNATED) {
      resumeDb(dbClient.get());
      LOGGER.info("Resuming as DB was Hibernated.");
    }
    while (dbClient.get().getStatus() != DatabaseStatusType.ACTIVE) {
      Thread.sleep(5000);
      LOGGER.info("Waiting for DB to be ACTIVE.");
    }
  }

  private void resumeDb(Database db) {
    try {
      HttpClient.newBuilder()
          .version(HttpClient.Version.HTTP_2)
          .connectTimeout(Duration.ofSeconds(20))
          .build()
          .send(
              HttpRequest.newBuilder()
                  .timeout(Duration.ofSeconds(20))
                  .uri(
                      URI.create(
                          ApiLocator.getApiRestEndpoint(db.getId(), db.getInfo().getRegion())
                              + "/v2/schemas/keyspace"))
                  .timeout(Duration.ofSeconds(20))
                  .header("Content-Type", "application/json")
                  .header("X-Cassandra-Token", test())
                  .GET()
                  .build(),
              HttpResponse.BodyHandlers.ofString());
    } catch (Exception e) {
      throw new IllegalStateException("Cannot resume database", e);
    }
  }

  private void createAndPopulateTables() {
    try (CqlSession astraSession =
        CqlSession.builder()
            .withCloudSecureConnectBundle(
                new ByteArrayInputStream(dbClient.downloadDefaultSecureConnectBundle()))
            .withAuthCredentials("token", dbClient.getToken())
            .withKeyspace(ASTRA_KS)
            .build()) {
      astraSession.execute(
          String.format(
              "CREATE TABLE IF NOT EXISTS %s.%s(person_department text, person_id int, person_name text, PRIMARY KEY"
                  + "((person_department), person_id));",
              ASTRA_KS, astraTable));
      for (int i = 0; i < NUM_ROWS; i++) {
        int index = i % SCIENTISTS.length;
        String insertStr =
            String.format(
                "INSERT INTO %s.%s(person_department, person_id, person_name) values("
                    + "'%s', %d, '%s');",
                ASTRA_KS, astraTable, SCIENTISTS[index][0], i, SCIENTISTS[index][1]);
        astraSession.execute(insertStr);
      }
    }
  }

  private static final String HASH =
      "AIpXbGsYPQCXtrwExZvOktGw:3d5bae1547a667608f10ab2d2e89a90b936f8ff8a3e9111efe23fc818ef344fd";
}
