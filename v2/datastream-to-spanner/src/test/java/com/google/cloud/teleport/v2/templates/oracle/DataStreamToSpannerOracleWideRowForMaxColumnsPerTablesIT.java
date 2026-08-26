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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.OracleSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link DataStreamToSpanner} Flex template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerOracleWideRowForMaxColumnsPerTablesIT
    extends DataStreamToSpannerITBase {

  private static final Integer NUM_EVENTS = 1;
  private static final Integer NUM_TABLES = 1;
  private static final Integer NUM_COLUMNS = 1000;

  private static CloudOracleResourceManager cloudOracleResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;

  private static final List<String> COLUMNS = new ArrayList<>();

  private static HashSet<DataStreamToSpannerOracleWideRowForMaxColumnsPerTablesIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static final List<String> TABLE_NAMES = new ArrayList<>();

  static {
    for (int i = 1; i <= NUM_TABLES; i++) {
      // According to instructions on quoting/identifiers: Oracle often folds to uppercase but
      // Datastream captures whatever it is. Oh wait, Oracle tables are usually uppercase or need
      // quotes.
      // Let's use uppercase to avoid case folding issues if unquoted
      TABLE_NAMES.add(
          "DATASTREAMTOSPANNER_" + i + "_" + RandomStringUtils.randomAlphanumeric(5).toUpperCase());
    }
    for (int i = 1; i <= NUM_COLUMNS; i++) {
      COLUMNS.add("COL_" + i);
    }
  }

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (DataStreamToSpannerOracleWideRowForMaxColumnsPerTablesIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        datastreamResourceManager =
            DatastreamResourceManager.builder(testName, PROJECT, REGION)
                .setCredentialsProvider(credentialsProvider)
                .setPrivateConnectivity("datastream-connect-2")
                .build();
        spannerResourceManager = setUpSpannerResourceManager();
        pubsubResourceManager = setUpPubSubResourceManager();
        gcsResourceManager = setUpSpannerITGcsResourceManager();

        org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.Builder builder =
            org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager.builder(testName);
        if (System.getProperty("cloudOracleHost") != null) {
          builder.setPassword(System.getProperty("cloudOraclePassword", "TestPassword123"));
          builder.setHost(System.getProperty("cloudOracleHost"));
          builder.setPort(1521);
          builder.setUsername(System.getProperty("cloudOracleUsername", "system"));
          builder.setDatabaseName("XEPDB1");
        }
        cloudOracleResourceManager =
            (org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager)
                new SpannerOracleResourceManager(builder);

        String sessionContent = generateBaseSchema();
        sessionContent =
            sessionContent
                .replaceAll("SRC_DATABASE", cloudOracleResourceManager.getUsername().toUpperCase())
                .replaceAll("SP_DATABASE", spannerResourceManager.getDatabaseId())
                .replaceAll("\"mysql\"", "\"oracle\"");
        for (int i = 1; i <= NUM_TABLES; i++) {
          sessionContent = sessionContent.replaceAll("TABLE" + i, TABLE_NAMES.get(i - 1));
        }
        setupSchema();
        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                null,
                null,
                "DataStreamToSpannerOracleWideRowForMaxColumnsPerTablesIT",
                spannerResourceManager,
                pubsubResourceManager,
                new HashMap<>() {
                  {
                    put("inputFileFormat", "avro");
                  }
                },
                null,
                null,
                gcsResourceManager,
                datastreamResourceManager,
                sessionContent,
                OracleSource.builder(
                        cloudOracleResourceManager.getHost(),
                        cloudOracleResourceManager.getUsername(),
                        cloudOracleResourceManager.getPassword(),
                        cloudOracleResourceManager.getPort(),
                        cloudOracleResourceManager.getDatabaseName())
                    .setAllowedTables(
                        Map.of(cloudOracleResourceManager.getUsername().toUpperCase(), TABLE_NAMES))
                    .build());
      }
    }
  }

  @After
  public void cleanUp() throws IOException {
    for (DataStreamToSpannerOracleWideRowForMaxColumnsPerTablesIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        cloudOracleResourceManager,
        datastreamResourceManager,
        spannerResourceManager,
        pubsubResourceManager,
        gcsResourceManager);
  }

  private void setupSchema() {
    TABLE_NAMES.forEach(
        tableName -> {
          cloudOracleResourceManager.runSQLUpdate(getJDBCSchema(tableName));
          cloudOracleResourceManager.runSQLUpdate(
              String.format("ALTER TABLE %s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS", tableName));
        });
    createSpannerTables();
  }

  @Test
  public void testDataStreamOracleToSpannerForMaxColumnsPerTables() throws IOException {
    assertThatPipeline(jobInfo).isRunning();

    Map<String, List<Map<String, Object>>> cdcEvents = new HashMap<>();
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
                PipelineOperator.Config.builder()
                    .setJobId(jobInfo.jobId())
                    .setProject(PROJECT)
                    .setRegion(REGION)
                    .setTimeoutAfter(Duration.ofMinutes(20))
                    .setCheckAfter(Duration.ofSeconds(5))
                    .build(),
                conditionCheck);

    // Assert
    assertThatResult(result).meetsConditions();
  }

  private String generateBaseSchema() throws IOException {
    Map<String, Object> sessionTemplate = createSessionTemplate();

    Gson gson = new GsonBuilder().setPrettyPrinting().create();

    return gson.toJson(sessionTemplate);
  }

  public static Map<String, Object> createSessionTemplate() {
    List<String> colIds = new ArrayList<>();
    for (int ci = 1; ci <= NUM_COLUMNS; ci++) {
      colIds.add("c" + ci);
    }
    return createSessionTemplate(
        NUM_TABLES, createColumnDefinitions(colIds), createPrimaryKeys(colIds));
  }

  /** Creates column definitions based on column IDs. */
  private static List<Map<String, Object>> createColumnDefinitions(List<String> colIds) {
    List<Map<String, Object>> colTypeConfigs = new ArrayList<>();
    for (int j = 1; j <= colIds.size(); j++) {
      Map<String, Object> colType = new LinkedHashMap<>();
      colType.put("Type", "VARCHAR2");
      colType.put("Len", 0);
      colType.put("IsArray", false);
      colType.put("Name", COLUMNS.get(j - 1));
      colType.put("NotNull", (j == 1));
      colType.put("Comment", "From: " + COLUMNS.get(j - 1) + " VARCHAR2(5)");
      colTypeConfigs.add(colType);
    }
    return colTypeConfigs;
  }

  /** Creates a list of primary key definitions. */
  private static List<Map<String, Object>> createPrimaryKeys(List<String> colIds) {
    List<Map<String, Object>> primaryKeys = new ArrayList<>();

    for (int j = 0; j < colIds.size(); j++) {
      Map<String, Object> primaryKey = new LinkedHashMap<>();
      primaryKey.put("ColId", colIds.get(j));
      primaryKey.put("Desc", false);
      primaryKey.put("Order", j + 1);
      primaryKeys.add(primaryKey);
    }

    return primaryKeys;
  }

  private String getJDBCSchema(String tableName) {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ").append(tableName).append(" (");
    for (int i = 1; i <= NUM_COLUMNS; i++) {
      sb.append(COLUMNS.get(i - 1)).append(" VARCHAR2(5) NOT NULL");
      if (i != NUM_COLUMNS) {
        sb.append(", ");
      }
    }
    sb.append(", PRIMARY KEY (").append(COLUMNS.get(0)).append("))");
    return sb.toString();
  }

  private void createSpannerTables() {
    for (String tableName : TABLE_NAMES) {
      List<String> columns = new ArrayList<>();
      columns.add(COLUMNS.get(0) + " STRING(10) NOT NULL");

      for (int i = 2; i <= NUM_COLUMNS; i++) {
        columns.add(COLUMNS.get(i - 1) + " STRING(10)");
      }

      String ddlStatement =
          String.format(
              "CREATE TABLE %s (%s) PRIMARY KEY (%s)",
              tableName, String.join(", ", columns), COLUMNS.get(0));

      spannerResourceManager.executeDdlStatement(ddlStatement);
    }
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
      protected @NotNull String getDescription() {
        return "Check Spanner rows.";
      }

      @Override
      protected @NotNull CheckResult check() {
        // First, check that correct number of rows were deleted.
        for (String tableName : TABLE_NAMES) {
          long totalRows = spannerResourceManager.getRowCount(tableName);
          long maxRows = cdcEvents.get(tableName).size();
          if (totalRows > maxRows) {
            return new CheckResult(
                false, String.format("Expected up to %d rows but found %d", maxRows, totalRows));
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
      @Override
      protected String getDescription() {
        return "Send initial JDBC events.";
      }

      @Override
      protected CheckResult check() {
        boolean success = true;
        List<String> messages = new ArrayList<>();
        for (String tableName : TABLE_NAMES) {

          List<Map<String, Object>> rows = new ArrayList<>();
          for (int i = 0; i < NUM_EVENTS; i++) {
            Map<String, Object> values = new HashMap<>();
            for (int ci = 1; ci <= NUM_COLUMNS; ci++) {
              values.put(COLUMNS.get(ci - 1), String.valueOf(ci));
            }
            rows.add(values);
          }
          cdcEvents.put(tableName, rows);
          success &= cloudOracleResourceManager.write(tableName, rows);
          messages.add(String.format("%d rows to %s", rows.size(), tableName));
        }
        return new CheckResult(success, "Sent " + String.join(", ", messages) + ".");
      }
    };
  }
}
