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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.lang3.math.NumberUtils;

/** Integration test for {@link DataStreamToSpanner} Flex template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerWideRowFor5000TablePerDatabaseIT extends DataStreamToSpannerITBase {
  private static final int THREAD_POOL_SIZE = 16;
  private static final int BATCH_SIZE = 2500;
  private static final int MAX_RETRIES = 3;
  private static final long RETRY_DELAY_MS = 3000; // Delay between retries
  private static final Integer NUM_EVENTS = 1;
  private static final Integer NUM_TABLES = 2500;

  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String ENTRY_ADDED = "entry_added";
  private static final Logger log =
      LoggerFactory.getLogger(DataStreamToSpannerWideRowFor5000TablePerDatabaseIT.class);

  private static final List<String> COLUMNS = List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED);
  private static final ExecutorService EXECUTOR_SERVICE =
      Executors.newFixedThreadPool(THREAD_POOL_SIZE);
  private static CloudSqlResourceManager cloudSqlResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static HashSet<DataStreamToSpannerWideRowFor5000TablePerDatabaseIT> testInstances =
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
    synchronized (DataStreamToSpannerWideRowFor5000TablePerDatabaseIT.class) {
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
    for (DataStreamToSpannerWideRowFor5000TablePerDatabaseIT instance : testInstances) {
      instance.tearDownBase();
    }
    EXECUTOR_SERVICE.shutdown();
    ResourceManagerUtils.cleanResources(
        datastreamResourceManager,
        cloudSqlResourceManager,
        spannerResourceManager,
        pubsubResourceManager,
        gcsResourceManager);
  }

  @Test
  public void testDataStreamMySqlToSpannerFor5000TablesPerDatabase() throws IOException {
    assertThatPipeline(jobInfo).isRunning();

    Map<String, List<Map<String, Object>>> cdcEvents = new HashMap<>();
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeJdbcData(cdcEvents),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE_NAMES.get(0))
                        .setMinRows(NUM_EVENTS)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, TABLE_NAMES.get(1))
                        .setMinRows(NUM_EVENTS)
                        .build(),
                    checkDestinationRows(cdcEvents)))
            .build();

    // Job needs to be cancelled as draining will time out
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(jobInfo, Duration.ofMinutes(20)), conditionCheck);

    assertThatResult(result).meetsConditions();
  }

  private void setupSchema() {
    List<CompletableFuture<Void>> futures = new LinkedList<>();
    for (int i = 0; i < TABLE_NAMES.size(); i += BATCH_SIZE) {
      int endIndex = Math.min(i + BATCH_SIZE, TABLE_NAMES.size());
      List<String> batch = TABLE_NAMES.subList(i, endIndex);
      createSpannerTables(batch, futures);
    }
    futures.add(
        CompletableFuture.runAsync(() -> createCloudSqlTables(TABLE_NAMES), EXECUTOR_SERVICE));
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.MINUTES);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create tables", e);
    }
  }

  private void execute(String statement) throws SQLException {
    String jdbcUrl = cloudSqlResourceManager.getUri();
    Connection connection =
        DriverManager.getConnection(
            jdbcUrl, cloudSqlResourceManager.getUsername(), cloudSqlResourceManager.getPassword());
    Statement statement1 = connection.createStatement();
    statement1.execute(statement);
    statement1.close();
    connection.close();
  }

  private void createCloudSqlTables(List<String> tableNames) {
    List<CompletableFuture<Void>> futures =
        tableNames.stream()
            .map(
                tableName ->
                    CompletableFuture.runAsync(
                        () -> createCloudSqlTableWithRetries(tableName), EXECUTOR_SERVICE))
            .toList();

    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.MINUTES);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Cloud SQL tables", e);
    }
  }

  private void createCloudSqlTableWithRetries(String tableName) {
    int retries = 0;
    while (retries < MAX_RETRIES) {
      try {
        execute(getJDBCSchema(tableName));
        return;
      } catch (Exception e) {
        retries++;
        if (retries == MAX_RETRIES) {
          throw new RuntimeException("Failed to create Cloud SQL table: " + tableName, e);
        }
        try {
          Thread.sleep(RETRY_DELAY_MS);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private boolean insertIntoCloudSqlTables(Map<String, List<Map<String, Object>>> batchedInserts) {
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    batchedInserts
        .keySet()
        .forEach(
            key -> {
              futures.add(
                  CompletableFuture.runAsync(
                      () -> {
                        int retries = 0;
                        while (retries < MAX_RETRIES) {
                          try {
                            cloudSqlResourceManager.write(key, batchedInserts.get(key));
                            break;
                          } catch (Exception e) {
                            retries++;
                            if (retries == MAX_RETRIES) {
                              throw new RuntimeException(
                                  "Failed to insert data into Cloud SQL tables", e);
                            }
                            try {
                              Thread.sleep(RETRY_DELAY_MS);
                            } catch (InterruptedException ie) {
                              Thread.currentThread().interrupt();
                            }
                          }
                        }
                      },
                      EXECUTOR_SERVICE));
            });
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.MINUTES);
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  private void createSpannerTables(List<String> tableNames, List<CompletableFuture<Void>> futures) {
    List<String> ddlStatements = new ArrayList<>();
    for (String tableName : tableNames) {
      ddlStatements.add(generateSpannerDDL(tableName));
    }
    futures.add(
        CompletableFuture.runAsync(
            () -> {
              int retries = 0;
              while (retries < MAX_RETRIES) {
                try {
                  spannerResourceManager.executeDdlStatements(ddlStatements);
                  break;
                } catch (Exception e) {
                  log.error("e: ", e);
                  retries++;
                  if (retries == MAX_RETRIES) {
                    throw new RuntimeException("Failed to create Spanner table: " + tableNames, e);
                  }
                  try {
                    Thread.sleep(RETRY_DELAY_MS);
                  } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                  }
                }
              }
            },
            EXECUTOR_SERVICE));
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.MINUTES);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create tables", e);
    }
  }

  private String generateBaseSchema() throws IOException {
    Map<String, Object> sessionTemplate = createSessionTemplate();
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(sessionTemplate);
  }

  public static Map<String, Object> createSessionTemplate() {
    List<String> colIds = Arrays.asList("c1", "c2", "c3", "c4", "c5");
    List<Map<String, Object>> colTypeConfigs = new ArrayList<>();
    for (int j = 1; j <= colIds.size(); j++) {
      Map<String, Object> colType = new LinkedHashMap<>();
      colType.put("Type", (j % 2 == 0) ? "STRING" : "NUMERIC");
      colType.put("Name", colIds.get(j - 1));
      colType.put("Len", (j % 2 == 0) ? 200 : 0);
      colType.put("IsArray", false);
      colType.put("NotNull", (j == 1));
      colType.put(
          "Comment", "From: column_" + j + ((j % 2 == 0) ? " varchar(200)" : " decimal(10)"));
      colTypeConfigs.add(colType);
    }

    List<Map<String, Object>> primaryKeys =
        List.of(Map.of("ColId", colIds.get(0), "Desc", false, "Order", 1));

    return createSessionTemplate(NUM_TABLES, colTypeConfigs, primaryKeys);
  }

  private String getJDBCSchema(String tableName) {
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s ("
            + "%s NUMERIC NOT NULL, "
            + "%s VARCHAR(200), "
            + "%s NUMERIC, "
            + "%s VARCHAR(200), "
            + "%s VARCHAR(200), "
            + "PRIMARY KEY (%s))",
        tableName, ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED, ROW_ID);
  }

  private String generateSpannerDDL(String tableName) {
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s ("
            + " %s INT64 NOT NULL, "
            + " %s STRING(1024), "
            + " %s INT64, "
            + " %s STRING(1024), "
            + " %s STRING(1024)) "
            + "PRIMARY KEY (%s)",
        tableName, ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED, ROW_ID);
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
        // First, check that correct number of rows were deleted.
        List<CompletableFuture<CheckResult>> futures =
            TABLE_NAMES.stream()
                .map(
                    tableName ->
                        CompletableFuture.supplyAsync(
                            () -> {
                              long totalRows = spannerResourceManager.getRowCount(tableName);
                              long maxRows =
                                  cdcEvents.getOrDefault(tableName, Collections.emptyList()).size();
                              if (totalRows > maxRows) {
                                return new CheckResult(
                                    false,
                                    String.format(
                                        "Expected up to %d rows but found %d in table %s",
                                        maxRows, totalRows, tableName));
                              }
                              return new CheckResult(
                                  true, "Table " + tableName + " row count is valid.");
                            },
                            EXECUTOR_SERVICE))
                .toList();

        List<CheckResult> results = futures.stream().map(CompletableFuture::join).toList();

        for (CheckResult result : results) {
          if (!result.isSuccess()) {
            return result;
          }
        }

        // Next, make sure in-place mutations were applied.
        try {
          checkSpannerTables(spannerResourceManager, TABLE_NAMES, cdcEvents, COLUMNS);
          return new CheckResult(true, "Spanner tables contain expected rows.");
        } catch (AssertionError error) {
          return new CheckResult(false, "Spanner tables do not contain expected rows.");
        }
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
      private boolean success = true;
      private final List<String> messages = new ArrayList<>();

      @Override
      protected String getDescription() {
        return "Send initial JDBC events.";
      }

      private void insertIntoTables() {
        for (int i = 0; i < TABLE_NAMES.size(); i += BATCH_SIZE) {
          int endIndex = Math.min(i + BATCH_SIZE, TABLE_NAMES.size());
          List<String> batch = TABLE_NAMES.subList(i, endIndex);
          insertIntoCloudTables(batch);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }

      private void insertIntoCloudTables(List<String> tableNames) {
        Map<String, List<Map<String, Object>>> batchedInserts = new HashMap<>();
        for (String tableName : tableNames) {
          List<Map<String, Object>> rows = new ArrayList<>();
          for (int i = 0; i < NUM_EVENTS; i++) {
            Map<String, Object> values = new HashMap<>();
            values.put(COLUMNS.get(0), i);
            values.put(COLUMNS.get(1), RandomStringUtils.randomAlphabetic(10));
            values.put(COLUMNS.get(2), new Random().nextInt(100));
            values.put(COLUMNS.get(3), new Random().nextInt() % 2 == 0 ? "Y" : "N");
            values.put(COLUMNS.get(4), Instant.now().toString());
            rows.add(values);
          }
          cdcEvents.put(tableName, rows);
          batchedInserts.put(tableName, rows);
          messages.add(String.format("%d rows to %s", rows.size(), tableName));
        }

        List<String> batchStatements = new ArrayList<>();
        batchedInserts
            .keySet()
            .forEach(
                key -> {
                  for (Map<String, Object> row : batchedInserts.get(key)) {
                    List<String> columns = new ArrayList<>(row.keySet());
                    StringBuilder sql =
                        new StringBuilder("INSERT INTO ")
                            .append(key)
                            .append("(")
                            .append(String.join(",", columns))
                            .append(") VALUES (");

                    List<String> valueList = new ArrayList<>();

                    for (String colName : columns) {
                      Object value = row.get(colName);
                      if (value == null) {
                        valueList.add("NULL");
                      } else if (!NumberUtils.isCreatable(value.toString())
                          && !"true".equalsIgnoreCase(value.toString())
                          && !"false".equalsIgnoreCase(value.toString())
                          && !value.toString().startsWith("ARRAY[")) {
                        valueList.add("'" + value + "'");
                      } else {
                        valueList.add(String.valueOf(value));
                      }
                    }
                    sql.append(String.join(",", valueList)).append(")");
                    batchStatements.add(sql.toString());
                  }
                });
        success &= insertIntoCloudSqlTables(batchedInserts);
      }

      @Override
      protected CheckResult check() {
        insertIntoTables();
        return new CheckResult(success, "Sent " + String.join(", ", messages) + ".");
      }
    };
  }
}
