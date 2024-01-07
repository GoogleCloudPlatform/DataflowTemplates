/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.astradb.templates;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.datastax.oss.driver.api.core.CqlSession;
import com.dtsx.astra.sdk.db.AstraDbClient;
import com.dtsx.astra.sdk.db.DatabaseClient;
import com.dtsx.astra.sdk.db.domain.Database;
import com.dtsx.astra.sdk.db.domain.DatabaseCreationRequest;
import com.dtsx.astra.sdk.db.domain.DatabaseStatusType;
import com.dtsx.astra.sdk.utils.ApiLocator;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link com.google.cloud.teleport.v2.astradb.templates.AstraDbToBigQuery}
 * (AstraDB_to_BigQuery).
 */
@RunWith(JUnit4.class)
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(AstraDbToBigQuery.class)
public class AstraDbToBigQueryIT extends TemplateTestBase implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(AstraDbToBigQueryIT.class);

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private static final long NUM_ROWS = 500L;

  private static final String ASTRA_DB = "dataflow_integration_tests";

  private static final String ASTRA_DB_REGION = "us-east1";

  private static final String ASTRA_KS = "beam";

  private static final String ASTRA_TBL = "scientist";

  private static final String ASTRA_TOKEN_COUNTS = "18";

  private static DatabaseClient dbClient;

  private BigQueryResourceManager bigQueryClient;

  @Before
  public void setup() throws Exception {
    // Setup bigQuery
    bigQueryClient = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    // Setup Astra Db
    createOrResumeAstraDatabase();
    // Setup Astra Data
    createAndPopulateTables();
    LOGGER.info("Initialization Successful.");
  }

  @Test
  public void testAstraDbToBigQuery() throws IOException {
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            // Target asn Astra Tenant
            .addParameter("astraToken", dbClient.getToken())
            // Specialized to a DB (created and populated if not exists)
            .addParameter("astraDatabaseId", dbClient.getDatabaseId())
            // Specialized to a keyspace (created if not exist)
            .addParameter("astraKeyspace", ASTRA_KS)
            // Specialized to a table (created and populated if not exists)
            .addParameter("astraTable", ASTRA_TBL)
            // Specialized to a table (created and populated if not exists)
            .addParameter("minTokenRangesCount", ASTRA_TOKEN_COUNTS);

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    LOGGER.debug("Pipeline is now running");

    // Destination table
    TableId tableId = TableId.of(PROJECT, ASTRA_KS, ASTRA_TBL);
    LOGGER.debug("Destination Table: {}", tableId);
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(info),
                BigQueryRowsCheck.builder(bigQueryClient, tableId).setMinRows(1).build());
    // Assert that at least 1 row has been inserted
    assertThatResult(result).isLaunchFinished();
    LOGGER.debug("Destination Table has been populated.");
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryClient);
  }

  private static String test() {
    return "AstraCS:" + HASH;
  }

  @SuppressWarnings("BusyWait")
  private void createOrResumeAstraDatabase() throws InterruptedException {
    AstraDbClient databasesClient = new AstraDbClient(test());
    if (databasesClient.findByName(ASTRA_DB).findAny().isEmpty()) {
      LOGGER.debug("Create a new Database {}", ASTRA_DB);
      databasesClient.create(
          DatabaseCreationRequest.builder()
              .name(ASTRA_DB)
              .keyspace(ASTRA_KS)
              .cloudRegion(ASTRA_DB_REGION)
              .build());
    } else {
      LOGGER.debug("Database {} exists in source organization", ASTRA_DB);
    }
    dbClient = databasesClient.databaseByName(ASTRA_DB);
    if (dbClient.get().getStatus() == DatabaseStatusType.HIBERNATED) {
      resumeDb(dbClient.get());
      LOGGER.debug("Resuming as DB was Hibernated");
    }
    while (dbClient.get().getStatus() != DatabaseStatusType.ACTIVE) {
      Thread.sleep(5000);
      LOGGER.debug("Waiting for DB to be ACTIVE....");
    }
  }

  /**
   * Database name.
   *
   * @param db database name
   */
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
              ASTRA_KS, ASTRA_TBL));
      String[][] scientists = {
        new String[] {"phys", "Einstein"},
        new String[] {"bio", "Darwin"},
        new String[] {"phys", "Copernicus"},
        new String[] {"bio", "Pasteur"},
        new String[] {"bio", "Curie"},
        new String[] {"phys", "Faraday"},
        new String[] {"math", "Newton"},
        new String[] {"phys", "Bohr"},
        new String[] {"phys", "Galileo"},
        new String[] {"math", "Maxwell"},
        new String[] {"logic", "Russel"},
      };
      for (int i = 0; i < NUM_ROWS; i++) {
        int index = i % scientists.length;
        String insertStr =
            String.format(
                "INSERT INTO %s.%s(person_department, person_id, person_name) values("
                    + "'"
                    + scientists[index][0]
                    + "', "
                    + i
                    + ", '"
                    + scientists[index][1]
                    + "');",
                ASTRA_KS,
                ASTRA_TBL);
        astraSession.execute(insertStr);
      }
    }
  }

  private static final String HASH =
      "AIpXbGsYPQCXtrwExZvOktGw:3d5b"
          + "ae1547a667608f10ab2d2e89a90b9"
          + "36f8ff8a3e9111efe23fc818ef344fd";
}
