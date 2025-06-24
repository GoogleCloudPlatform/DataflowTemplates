/*
 * Copyright (C) 2025 Google LLC
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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link DataStreamToSpanner} Flex template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
@Ignore("Triaging flaky test") // TODO(b/424087272)
public class DataStreamToSpannerWideRowForMax9MibTablePerDatabaseIT
    extends DataStreamToSpannerITBase {
  private static final int STRING_LENGTH = 200;
  private static final Integer NUM_EVENTS = 1;
  private static final Integer NUM_TABLES = 1;

  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String ENTRY_ADDED = "entry_added";
  private static final String LARGE_BLOB_ADDED = "large_blob";

  private static final List<String> COLUMNS =
      List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED, LARGE_BLOB_ADDED);

  private static CloudSqlResourceManager cloudSqlResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;
  private static HashSet<DataStreamToSpannerWideRowForMax9MibTablePerDatabaseIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static final List<String> TABLE_NAMES = new ArrayList<>();

  static {
    for (int i = 1; i <= NUM_TABLES; i++) {
      TABLE_NAMES.add("DataStreamToSpanner_" + i + "_" + RandomStringUtils.randomAlphanumeric(5));
    }
  }

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (DataStreamToSpannerWideRowForMax9MibTablePerDatabaseIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity("datastream-private-connect-us-central1")
                .build();
        spannerResourceManager = setUpSpannerResourceManager();
        pubsubResourceManager = setUpPubSubResourceManager();
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        cloudSqlResourceManager = CloudMySQLResourceManager.builder(testName).build();
        String sessionContent =
            generateSessionFile(
                NUM_TABLES,
                cloudSqlResourceManager.getDatabaseName(),
                spannerResourceManager.getDatabaseId(),
                TABLE_NAMES,
                generateBaseSchema());
        setupSchema();
        ADDITIONAL_JOB_PARAMS.putAll(
            new HashMap<>() {
              {
                put("network", VPC_NAME);
                put("subnetwork", SUBNET_NAME);
                put("workerRegion", VPC_REGION);
              }
            });
        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                null,
                null,
                "DataStreamToSpannerWideRowFor5000TablePerDatabaseIT",
                spannerResourceManager,
                pubsubResourceManager,
                new HashMap<>() {
                  {
                    put("inputFileFormat", "json");
                  }
                },
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                sessionContent,
                MySQLSource.builder(
                        cloudSqlResourceManager.getHost(),
                        cloudSqlResourceManager.getUsername(),
                        cloudSqlResourceManager.getPassword(),
                        cloudSqlResourceManager.getPort())
                    .setAllowedTables(
                        Map.of(cloudSqlResourceManager.getDatabaseName(), TABLE_NAMES))
                    .build());
      }
    }
  }

  @After
  public void cleanUp() throws IOException {
    for (DataStreamToSpannerWideRowForMax9MibTablePerDatabaseIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        cloudSqlResourceManager,
        datastreamResourceManager,
        spannerResourceManager,
        pubsubResourceManager,
        gcsResourceManager);
  }

  private void setupSchema() {
    TABLE_NAMES.forEach(
        tableName -> cloudSqlResourceManager.createTable(tableName, createJdbcSchema()));
    createSpannerTables();
  }

  @Test
  public void testDataStreamMySqlToSpannerFor9MBTablesPerDatabase() throws IOException {
    assertThatPipeline(jobInfo).isRunning();
    Map<String, List<Map<String, Object>>> cdcEvents = new LinkedHashMap<>();
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeJdbcData(cdcEvents),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE_NAMES.get(0))
                        .setMinRows(NUM_EVENTS)
                        .build(),
                    checkDestinationRows(cdcEvents)))
            .build();

    // Job needs to be cancelled as draining will time out
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(jobInfo, Duration.ofMinutes(20)), conditionCheck);

    // Assert
    assertThatResult(result).meetsConditions();
  }

  private String generateBaseSchema() throws IOException {
    Map<String, Object> sessionTemplate = createSessionTemplate();
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(sessionTemplate);
  }

  public static Map<String, Object> createSessionTemplate() {
    List<String> colIds = Arrays.asList("c1", "c2", "c3", "c4", "c5", "c6");
    List<Map<String, Object>> colTypeConfigs = new ArrayList<>();
    for (int j = 1; j <= colIds.size(); j++) {
      Map<String, Object> colType = new LinkedHashMap<>();
      if (j == 6) {
        colType.put("Type", "LONGBLOB");
        colType.put("Len", 0);
      } else if (j % 2 == 0) {
        colType.put("Type", "STRING");
        colType.put("Len", STRING_LENGTH);
      } else {
        colType.put("Type", "NUMERIC");
        colType.put("Len", 0);
      }
      colType.put("IsArray", false);
      colType.put("Name", "column_" + j);
      colType.put("NotNull", (j == 1));
      colType.put("Comment", "From: column_" + j + " " + colType.get("Type"));
    }
    List<Map<String, Object>> primaryKeys =
        List.of(Map.of("ColId", colIds.get(0), "Desc", false, "Order", 1));
    return createSessionTemplate(NUM_TABLES, colTypeConfigs, primaryKeys);
  }

  private JDBCResourceManager.JDBCSchema createJdbcSchema() {
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    columns.put(LARGE_BLOB_ADDED, "LONGBLOB");
    return new JDBCResourceManager.JDBCSchema(columns, ROW_ID);
  }

  private void createSpannerTables() {
    TABLE_NAMES.forEach(
        tableName ->
            spannerResourceManager.executeDdlStatement(
                "CREATE TABLE IF NOT EXISTS "
                    + tableName
                    + " ("
                    + ROW_ID
                    + " INT64 NOT NULL, "
                    + NAME
                    + " STRING(1024), "
                    + AGE
                    + " INT64, "
                    + MEMBER
                    + " STRING(1024), "
                    + ENTRY_ADDED
                    + " STRING(1024), "
                    + LARGE_BLOB_ADDED
                    + " BYTES(9437184)" // 9 MiB
                    + ") PRIMARY KEY ("
                    + ROW_ID
                    + ")"));
  }

  /**
   * Helper function for constructing a ConditionCheck whose check() method checks the rows in the
   * destination Spanner database for specific rows.
   *
   * @return A ConditionCheck containing the check operation.
   */
  private ConditionCheck checkDestinationRows(Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Check Spanner rows.";
      }

      @Override
      protected CheckResult check() {
        for (String tableName : TABLE_NAMES) {
          long totalRows = spannerResourceManager.getRowCount(tableName);
          long maxRows = cdcEvents.get(tableName).size();
          if (totalRows > maxRows) {
            return new CheckResult(
                false, String.format("Expected up to %d rows but found %d", maxRows, totalRows));
          }
        }
        return new CheckResult(true, "Spanner tables contain expected rows.");
      }
    };
  }

  /**
   * Helper function for constructing a ConditionCheck whose check() method constructs the initial
   * rows of data in the JDBC database according to the common schema for the IT's in this class.
   *
   * @return A ConditionCheck containing the JDBC write operation.
   */
  private ConditionCheck writeJdbcData(Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Send initial JDBC events.";
      }

      @Override
      protected CheckResult check() {
        boolean success = true;
        List<String> messages = new ArrayList<>();
        Random random = new Random();
        byte[] largeData = new byte[9 * 1024 * 1024];
        random.nextBytes(largeData);

        for (String tableName : TABLE_NAMES) {
          List<Map<String, Object>> rows = new ArrayList<>();

          for (int i = 0; i < NUM_EVENTS; i++) {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put(COLUMNS.get(0), i);
            values.put(COLUMNS.get(1), RandomStringUtils.randomAlphabetic(10));
            values.put(COLUMNS.get(2), random.nextInt(100));
            values.put(COLUMNS.get(3), random.nextBoolean() ? "Y" : "N");
            values.put(COLUMNS.get(4), Instant.now().toString());
            byte[] rowData = largeData.clone();
            values.put(COLUMNS.get(5), rowData);
            rows.add(values);
          }

          success &= cloudSqlResourceManager.write(tableName, rows);

          rows.forEach(
              values ->
                  values.put(
                      COLUMNS.get(5),
                      Base64.getEncoder().encodeToString((byte[]) values.get(COLUMNS.get(5)))));

          cdcEvents.put(tableName, rows);
          messages.add(String.format("%d rows to %s", rows.size(), tableName));
        }

        return new CheckResult(success, "Sent " + String.join(", ", messages) + ".");
      }
    };
  }
}
