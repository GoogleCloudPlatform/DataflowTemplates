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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.cloudsql.CloudPostgresResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.datastream.PostgresqlSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.conditions.JDBCRowsCheck;
import org.apache.commons.lang3.RandomStringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSQL.class)
@RunWith(JUnit4.class)
public class DataStreamToPostgresIT extends TemplateTestBase {

  @Rule public Timeout timeout = new Timeout(30, TimeUnit.MINUTES);
  private static final int NUM_EVENTS = 10;
  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToPostgresIT.class);

  // Define column names for destination (uppercase) for the uppercasing test
  private static final String ROW_ID_UPPER = "ROW_ID";
  private static final String NAME_UPPER = "NAME";
  private static final String AGE_UPPER = "AGE";
  private static final String MEMBER_UPPER = "MEMBER";
  private static final String ENTRY_ADDED_UPPER = "ENTRY_ADDED";
  private static final List<String> DESTINATION_COLUMNS =
      List.of(ROW_ID_UPPER, NAME_UPPER, AGE_UPPER, MEMBER_UPPER, ENTRY_ADDED_UPPER);

  // Define column names for source (lowercase) for all tests
  private static final List<String> SOURCE_COLUMNS =
      DESTINATION_COLUMNS.stream().map(String::toLowerCase).collect(Collectors.toList());
  private static final String SOURCE_PK = ROW_ID_UPPER.toLowerCase();

  private CloudPostgresResourceManager cloudSqlSourceResourceManager;
  private CloudPostgresResourceManager cloudSqlDestinationResourceManager;
  private DatastreamResourceManager datastreamResourceManager;
  private PubsubResourceManager pubsubResourceManager;
  private GcsResourceManager gcsResourceManager;

  @Before
  public void setUp() throws IOException {
    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity("datastream-connect-2")
            .build();

    cloudSqlSourceResourceManager =
        CloudPostgresResourceManager.builder(testName + "-source").build();
    cloudSqlDestinationResourceManager =
        CloudPostgresResourceManager.builder(testName + "-dest").build();
    gcsResourceManager =
        GcsResourceManager.builder(
                artifactBucketName, testName, credentialsProvider.getCredentials())
            .build();
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(
        cloudSqlSourceResourceManager,
        cloudSqlDestinationResourceManager,
        datastreamResourceManager,
        pubsubResourceManager,
        gcsResourceManager);
  }

  @Test
  public void testDataStreamPostgresToPostgres() throws IOException {
    String tableName = "pg_to_pg_" + RandomStringUtils.randomAlphanumeric(5).toLowerCase();
    cloudSqlSourceResourceManager.createTable(
        tableName, createJdbcSchema(SOURCE_COLUMNS, SOURCE_PK));
    cloudSqlDestinationResourceManager.createTable(
        tableName, createJdbcSchema(SOURCE_COLUMNS, SOURCE_PK));
    runTest(tableName, tableName);
  }

  @Test
  public void testDataStreamPostgresToPostgresUppercasing() throws IOException {
    String sourceTableName = "pg_to_pg_" + RandomStringUtils.randomAlphanumeric(5).toLowerCase();
    String destinationTableName = sourceTableName.toUpperCase();
    cloudSqlSourceResourceManager.createTable(
        sourceTableName, createJdbcSchema(SOURCE_COLUMNS, SOURCE_PK));
    String destinationSchema = cloudSqlDestinationResourceManager.getDatabaseName();
    cloudSqlDestinationResourceManager.runSQLUpdate(
        String.format(
            "CREATE TABLE %s.\"%s\" (\"%s\" NUMERIC NOT NULL PRIMARY KEY, \"%s\" VARCHAR(200), \"%s\" NUMERIC, \"%s\" VARCHAR(200), \"%s\" VARCHAR(200))",
            destinationSchema,
            destinationTableName,
            ROW_ID_UPPER,
            NAME_UPPER,
            AGE_UPPER,
            MEMBER_UPPER,
            ENTRY_ADDED_UPPER));
    runTest(sourceTableName, destinationTableName);
  }

  @Test
  public void testPostgresToPostgresEnumLtreeHstore() throws IOException {
    String tableName = "special_types_" + RandomStringUtils.randomAlphanumeric(5).toLowerCase();
    String enumTypeName = "asset_status_it";
    String createEnumSQL =
        String.format("CREATE TYPE %s AS ENUM ('ONLINE', 'OFFLINE', 'MAINTENANCE');", enumTypeName);
    cloudSqlSourceResourceManager.runSQLUpdate(createEnumSQL);
    cloudSqlDestinationResourceManager.runSQLUpdate(createEnumSQL);

    cloudSqlDestinationResourceManager.runSQLUpdate("CREATE EXTENSION IF NOT EXISTS hstore;");
    cloudSqlDestinationResourceManager.runSQLUpdate("CREATE EXTENSION IF NOT EXISTS ltree;");

    cloudSqlSourceResourceManager.createTable(tableName, createHybridSourceSchema(enumTypeName));
    cloudSqlDestinationResourceManager.createTable(
        tableName, createSpecialTypesJdbcSchema(enumTypeName));
    runTestWithSpecialTypes(tableName);
  }

  private void runTest(String sourceTableName, String destinationTableName) throws IOException {
    JDBCSource jdbcSource =
        PostgresqlSource.builder(
                cloudSqlSourceResourceManager.getHost(),
                cloudSqlSourceResourceManager.getUsername(),
                cloudSqlSourceResourceManager.getPassword(),
                cloudSqlSourceResourceManager.getPort(),
                cloudSqlSourceResourceManager.getDatabaseName())
            .setAllowedTables(
                Map.of(cloudSqlSourceResourceManager.getDatabaseName(), List.of(sourceTableName)))
            .build();
    String gcsPrefix = getGcsPath(testName + "/cdc/").replace("gs://" + artifactBucketName, "");
    String gcsPrefixForNotification =
        getGcsPath(testName + "/cdc/").replace("gs://" + artifactBucketName + "/", "");
    SourceConfig sourceConfig =
        datastreamResourceManager.buildJDBCSourceConfig("postgres-profile", jdbcSource);
    DestinationConfig destinationConfig =
        datastreamResourceManager.buildGCSDestinationConfig(
            "gcs-profile",
            artifactBucketName,
            gcsPrefix,
            DatastreamResourceManager.DestinationOutputFormat.JSON_FILE_FORMAT);
    Stream stream =
        datastreamResourceManager.createStream("stream-pg-to-pg", sourceConfig, destinationConfig);
    datastreamResourceManager.startStream(stream);
    com.google.pubsub.v1.TopicName topic = pubsubResourceManager.createTopic("gcs-notifications");
    com.google.pubsub.v1.SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, "dataflow-subscription");
    gcsResourceManager.createNotification(topic.toString(), gcsPrefixForNotification);
    String schemaMap =
        String.format(
            "%s:%s",
            cloudSqlSourceResourceManager.getDatabaseName(),
            cloudSqlDestinationResourceManager.getDatabaseName());
    String jobName = PipelineUtils.createJobName(testName);
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(jobName, specPath)
            .addParameter("inputFilePattern", getGcsPath(testName) + "/cdc/")
            .addParameter("gcsPubSubSubscription", subscription.toString())
            .addParameter("inputFileFormat", "json")
            .addParameter("streamName", stream.getName())
            .addParameter("databaseType", "postgres")
            .addParameter("databaseName", cloudSqlDestinationResourceManager.getDatabaseName())
            .addParameter("schemaMap", schemaMap)
            .addParameter("databaseHost", cloudSqlDestinationResourceManager.getHost())
            .addParameter(
                "databasePort", String.valueOf(cloudSqlDestinationResourceManager.getPort()))
            .addParameter("databaseUser", cloudSqlDestinationResourceManager.getUsername())
            .addParameter("databasePassword", cloudSqlDestinationResourceManager.getPassword());
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    Map<String, List<Map<String, Object>>> cdcEvents = new HashMap<>();
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writePostgresData(sourceTableName, cdcEvents),
                    buildRowCheck(destinationTableName, NUM_EVENTS),
                    changePostgresData(sourceTableName, cdcEvents),
                    checkDestinationRows(destinationTableName, sourceTableName, cdcEvents)))
            .build();
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(createConfig(info, Duration.ofMinutes(20)), conditionCheck);

    // Corrected Call
    checkRegularJdbcTable(destinationTableName, cdcEvents.get(sourceTableName));
    assertThatResult(result).meetsConditions();
  }

  private void runTestWithSpecialTypes(String tableName) throws IOException {
    JDBCSource jdbcSource =
        PostgresqlSource.builder(
                cloudSqlSourceResourceManager.getHost(),
                cloudSqlSourceResourceManager.getUsername(),
                cloudSqlSourceResourceManager.getPassword(),
                cloudSqlSourceResourceManager.getPort(),
                cloudSqlSourceResourceManager.getDatabaseName())
            .setAllowedTables(
                Map.of(cloudSqlSourceResourceManager.getDatabaseName(), List.of(tableName)))
            .build();
    String gcsPrefix = getGcsPath(testName + "/cdc/").replace("gs://" + artifactBucketName, "");
    String gcsPrefixForNotification =
        getGcsPath(testName + "/cdc/").replace("gs://" + artifactBucketName + "/", "");
    SourceConfig sourceConfig =
        datastreamResourceManager.buildJDBCSourceConfig("postgres-profile-special", jdbcSource);
    DestinationConfig destinationConfig =
        datastreamResourceManager.buildGCSDestinationConfig(
            "gcs-profile-special",
            artifactBucketName,
            gcsPrefix,
            DatastreamResourceManager.DestinationOutputFormat.JSON_FILE_FORMAT);
    Stream stream =
        datastreamResourceManager.createStream(
            "stream-pg-special", sourceConfig, destinationConfig);
    datastreamResourceManager.startStream(stream);
    com.google.pubsub.v1.TopicName topic =
        pubsubResourceManager.createTopic("gcs-notifications-special");
    com.google.pubsub.v1.SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, "dataflow-subscription-special");
    gcsResourceManager.createNotification(topic.toString(), gcsPrefixForNotification);
    String schemaMap =
        String.format(
            "%s:%s",
            cloudSqlSourceResourceManager.getDatabaseName(),
            cloudSqlDestinationResourceManager.getDatabaseName());
    String jobName = PipelineUtils.createJobName(testName + "-special");
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(jobName, specPath)
            .addParameter("inputFilePattern", getGcsPath(testName) + "/cdc/")
            .addParameter("gcsPubSubSubscription", subscription.toString())
            .addParameter("inputFileFormat", "json")
            .addParameter("streamName", stream.getName())
            .addParameter("databaseType", "postgres")
            .addParameter("databaseName", cloudSqlDestinationResourceManager.getDatabaseName())
            .addParameter("schemaMap", schemaMap)
            .addParameter("databaseHost", cloudSqlDestinationResourceManager.getHost())
            .addParameter(
                "databasePort", String.valueOf(cloudSqlDestinationResourceManager.getPort()))
            .addParameter("databaseUser", cloudSqlDestinationResourceManager.getUsername())
            .addParameter("databasePassword", cloudSqlDestinationResourceManager.getPassword());
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    Map<String, List<Map<String, Object>>> cdcEvents = new HashMap<>();
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeSpecialTypesData(tableName, cdcEvents),
                    buildRowCheck(tableName, NUM_EVENTS),
                    changeSpecialTypesData(tableName, cdcEvents),
                    checkDestinationRows(tableName, tableName, cdcEvents)))
            .build();
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(createConfig(info, Duration.ofMinutes(20)), conditionCheck);

    // Corrected Call
    checkSpecialKeys(tableName, cdcEvents.get(tableName));
    assertThatResult(result).meetsConditions();
  }

  private JDBCResourceManager.JDBCSchema createJdbcSchema(List<String> columns, String pkColumn) {
    HashMap<String, String> schema = new HashMap<>();
    schema.put(columns.get(0), "NUMERIC NOT NULL");
    schema.put(columns.get(1), "VARCHAR(200)");
    schema.put(columns.get(2), "NUMERIC");
    schema.put(columns.get(3), "VARCHAR(200)");
    schema.put(columns.get(4), "VARCHAR(200)");
    return new JDBCResourceManager.JDBCSchema(schema, pkColumn);
  }

  private JDBCResourceManager.JDBCSchema createHybridSourceSchema(String enumTypeName) {
    HashMap<String, String> schema = new HashMap<>();
    schema.put("asset_id", "INT NOT NULL");
    schema.put("status", enumTypeName);
    schema.put("attributes", "TEXT");
    schema.put("location_path", "TEXT");
    return new JDBCResourceManager.JDBCSchema(schema, "asset_id");
  }

  private JDBCResourceManager.JDBCSchema createSpecialTypesJdbcSchema(String enumTypeName) {
    HashMap<String, String> schema = new HashMap<>();
    schema.put("asset_id", "INT NOT NULL");
    schema.put("status", enumTypeName);
    schema.put("attributes", "HSTORE");
    schema.put("location_path", "LTREE");
    return new JDBCResourceManager.JDBCSchema(schema, "asset_id");
  }

  private ConditionCheck buildRowCheck(String tableName, int expectedRows) {
    if (tableName.toLowerCase().equals(tableName)) {
      return JDBCRowsCheck.builder(cloudSqlDestinationResourceManager, tableName)
          .setMinRows(expectedRows)
          .build();
    }
    return new ConditionCheck() {
      @Override
      public @NonNull String getDescription() {
        return String.format(
            "Check if destination table '\"%s\"' has at least %d rows", tableName, expectedRows);
      }

      @Override
      protected @NonNull CheckResult check() {
        String countQuery =
            String.format(
                "SELECT COUNT(*) AS row_count FROM %s.\"%s\"",
                cloudSqlDestinationResourceManager.getDatabaseName(), tableName);
        try {
          List<Map<String, Object>> result =
              cloudSqlDestinationResourceManager.runSQLQuery(countQuery);
          if (result == null || result.isEmpty() || !result.get(0).containsKey("row_count")) {
            return new CheckResult(
                false, "Query returned no results or missing 'row_count' column.");
          }
          long rowCount = Long.parseLong(result.get(0).get("row_count").toString());
          if (rowCount >= expectedRows) {
            return new CheckResult(
                true, String.format("Found %d rows in '\"%s\"'.", rowCount, tableName));
          } else {
            return new CheckResult(
                false,
                String.format(
                    "Found %d rows in '\"%s\"', expected at least %d.",
                    rowCount, tableName, expectedRows));
          }
        } catch (Exception e) {
          return new CheckResult(false, "Error checking row count: " + e.getMessage());
        }
      }
    };
  }

  private ConditionCheck writePostgresData(
      String tableName, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      public @NonNull String getDescription() {
        return "Send initial PostgreSQL events.";
      }

      @Override
      public @NonNull CheckResult check() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < NUM_EVENTS; i++) {
          Map<String, Object> values = new HashMap<>();
          values.put(SOURCE_COLUMNS.get(0), i);
          values.put(SOURCE_COLUMNS.get(1), RandomStringUtils.randomAlphabetic(10).toLowerCase());
          values.put(SOURCE_COLUMNS.get(2), new Random().nextInt(100));
          values.put(SOURCE_COLUMNS.get(3), new Random().nextInt() % 2 == 0 ? "Y" : "N");
          values.put(SOURCE_COLUMNS.get(4), Instant.now().toString());
          rows.add(values);
        }
        boolean success = cloudSqlSourceResourceManager.write(tableName, rows);
        cdcEvents.put(tableName, new ArrayList<>(rows));
        return new CheckResult(
            success, String.format("Sent %d rows to %s.", rows.size(), tableName));
      }
    };
  }

  private ConditionCheck changePostgresData(
      String tableName, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      public @NonNull String getDescription() {
        return "Send PostgreSQL changes.";
      }

      @Override
      protected @NonNull CheckResult check() {
        String qualifiedTableName =
            cloudSqlSourceResourceManager.getDatabaseName() + "." + tableName;
        List<Map<String, Object>> previousEvents = cdcEvents.get(tableName);
        List<Map<String, Object>> finalExpectedEvents = new ArrayList<>();

        for (int i = 0; i < NUM_EVENTS; i++) {
          if (i % 2 == 0) {
            Map<String, Object> values = new HashMap<>(previousEvents.get(i));
            values.put(SOURCE_COLUMNS.get(2), new Random().nextInt(100));
            values.put(
                SOURCE_COLUMNS.get(3),
                Objects.equals(values.get(SOURCE_COLUMNS.get(3)).toString(), "Y") ? "N" : "Y");

            String updateSql =
                String.format(
                    "UPDATE %s SET %s = %s, %s = '%s' WHERE %s = %d",
                    qualifiedTableName,
                    SOURCE_COLUMNS.get(2),
                    values.get(SOURCE_COLUMNS.get(2)),
                    SOURCE_COLUMNS.get(3),
                    values.get(SOURCE_COLUMNS.get(3)),
                    SOURCE_COLUMNS.get(0),
                    i);
            cloudSqlSourceResourceManager.runSQLUpdate(updateSql);
            finalExpectedEvents.add(values);
          } else {
            cloudSqlSourceResourceManager.runSQLUpdate(
                String.format(
                    "DELETE FROM %s WHERE %s = %d", qualifiedTableName, SOURCE_COLUMNS.get(0), i));
          }
        }
        cdcEvents.put(tableName, finalExpectedEvents);
        return new CheckResult(true, String.format("Sent changes to %s.", tableName));
      }
    };
  }

  private ConditionCheck writeSpecialTypesData(
      String tableName, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      public @NonNull String getDescription() {
        return "Send initial ENUM, HSTORE, and LTREE events.";
      }

      @Override
      public @NonNull CheckResult check() {
        List<Map<String, Object>> rowsForWrite = new ArrayList<>();
        String[] statuses = {"ONLINE", "OFFLINE", "MAINTENANCE"};

        for (int i = 0; i < NUM_EVENTS; i++) {
          Map<String, Object> row = new HashMap<>();
          row.put("asset_id", i);
          row.put("status", statuses[i % statuses.length]);
          row.put("location_path", String.format("USA.West.Rack%d.Host%d", i / 5, i % 5));
          row.put(
              "attributes",
              String.format("{\"ip\":\"192.168.1.%d\",\"ram_gb\":\"%d\"}", 100 + i, 32 + i));
          rowsForWrite.add(row);
        }

        boolean success = cloudSqlSourceResourceManager.write(tableName, rowsForWrite);
        cdcEvents.put(tableName, new ArrayList<>(rowsForWrite));
        return new CheckResult(
            success, String.format("Sent %d rows to %s.", rowsForWrite.size(), tableName));
      }
    };
  }

  private ConditionCheck changeSpecialTypesData(
      String tableName, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      public @NonNull String getDescription() {
        return "Send changes for ENUM, HSTORE, and LTREE.";
      }

      @Override
      protected @NonNull CheckResult check() {
        String qualifiedTableName =
            cloudSqlSourceResourceManager.getDatabaseName() + "." + tableName;
        List<Map<String, Object>> previousEvents = cdcEvents.get(tableName);
        List<Map<String, Object>> finalExpectedEvents = new ArrayList<>();

        for (int i = 0; i < NUM_EVENTS; i++) {
          // UPDATE even rows
          if (i % 2 == 0) {
            Map<String, Object> updatedEvent = new HashMap<>(previousEvents.get(i));

            // Get the current status to determine the new status
            String currentStatus = updatedEvent.get("status").toString();
            String newStatus;
            switch (currentStatus) {
              case "ONLINE":
                newStatus = "OFFLINE";
                break;
              case "OFFLINE":
                newStatus = "MAINTENANCE";
                break;
              default: // Covers MAINTENANCE and any other case
                newStatus = "ONLINE";
                break;
            }
            updatedEvent.put("status", newStatus);

            String updateSql =
                String.format(
                    "UPDATE %s SET status = '%s' WHERE asset_id = %d",
                    qualifiedTableName, newStatus, i);
            cloudSqlSourceResourceManager.runSQLUpdate(updateSql);
            finalExpectedEvents.add(updatedEvent);
          } else {
            // DELETE odd rows
            cloudSqlSourceResourceManager.runSQLUpdate(
                String.format("DELETE FROM %s WHERE asset_id = %d", qualifiedTableName, i));
          }
        }
        cdcEvents.put(tableName, finalExpectedEvents);
        return new CheckResult(true, String.format("Sent changes to %s.", tableName));
      }
    };
  }

  private ConditionCheck checkDestinationRows(
      String destinationTableName,
      String sourceTableName,
      Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      public @NonNull String getDescription() {
        return "Check that destination table has expected row count and content after changes.";
      }

      @Override
      protected @NonNull CheckResult check() {
        List<Map<String, Object>> expectedRecords = cdcEvents.get(sourceTableName);
        long expectedRowCount = expectedRecords.size();
        String destinationSchema = cloudSqlDestinationResourceManager.getDatabaseName();

        try {
          List<Map<String, Object>> actualRecords;
          String pkColumnForExpected;
          String pkColumnForActual;
          String pkColumnForQuery;
          String qualifiedTableName;

          if (destinationTableName.toLowerCase().equals(destinationTableName)) {
            qualifiedTableName = destinationSchema + "." + destinationTableName;
          } else {
            qualifiedTableName = destinationSchema + ".\"" + destinationTableName + "\"";
          }

          if (destinationTableName.startsWith("special_types_")) {
            pkColumnForExpected = "asset_id";
            pkColumnForActual = "asset_id";
            pkColumnForQuery = "asset_id";
          } else {
            pkColumnForExpected = SOURCE_PK; // "row_id"

            if (!destinationTableName.toLowerCase().equals(destinationTableName)) {
              // This is the uppercase test
              pkColumnForActual = ROW_ID_UPPER; // "ROW_ID"
              pkColumnForQuery = "\"" + ROW_ID_UPPER + "\"";
            } else {
              // This is the standard lowercase test
              pkColumnForActual = SOURCE_PK; // "row_id"
              pkColumnForQuery = SOURCE_PK;
            }
          }

          actualRecords =
              cloudSqlDestinationResourceManager.runSQLQuery(
                  String.format("SELECT %s FROM %s", pkColumnForQuery, qualifiedTableName));

          if (actualRecords.size() != expectedRowCount) {
            return new CheckResult(
                false,
                String.format(
                    "Polling: Found %d rows in '%s', but expected %d.",
                    actualRecords.size(), destinationTableName, expectedRowCount));
          }

          LOG.info(
              "Row count for '{}' matches expected {}. Performing final data validation.",
              destinationTableName,
              expectedRowCount);

          Set<BigDecimal> actualIds =
              actualRecords.stream()
                  .map(row -> new BigDecimal(row.get(pkColumnForActual).toString()))
                  .collect(Collectors.toSet());
          Set<BigDecimal> expectedIds =
              expectedRecords.stream()
                  .map(row -> new BigDecimal(row.get(pkColumnForExpected).toString()))
                  .collect(Collectors.toSet());
          assertThat(actualIds).isEqualTo(expectedIds);

          return new CheckResult(true, "Destination table row count and content are correct.");

        } catch (Exception e) {
          return new CheckResult(
              false, "An unexpected error occurred while checking rows: " + e.getMessage());
        }
      }
    };
  }

  private void checkRegularJdbcTable(
      String destinationTableName, List<Map<String, Object>> expectedRecords) {
    String destinationSchema = cloudSqlDestinationResourceManager.getDatabaseName();
    if (!destinationTableName.toLowerCase().equals(destinationTableName)) {
      LOG.info("Executing case-sensitive query for table: \"{}\"", destinationTableName);
      String selectQuery =
          String.format("SELECT * FROM %s.\"%s\"", destinationSchema, destinationTableName);
      assertThatRecords(cloudSqlDestinationResourceManager.runSQLQuery(selectQuery))
          .hasRecordsUnorderedCaseInsensitiveColumns(expectedRecords);
    } else {
      LOG.info("Executing standard readTable for table: {}", destinationTableName);
      // readTable in the resource manager does not currently support schema qualification,
      // so we use a qualified query instead for consistency.
      String selectQuery =
          String.format("SELECT * FROM %s.%s", destinationSchema, destinationTableName);
      assertThatRecords(cloudSqlDestinationResourceManager.runSQLQuery(selectQuery))
          .hasRecordsUnorderedCaseInsensitiveColumns(expectedRecords);
    }
  }

  private void checkSpecialKeys(String tableName, List<Map<String, Object>> expectedRecords) {
    String destinationSchema = cloudSqlDestinationResourceManager.getDatabaseName();
    LOG.info("Performing final validation of primary keys in '{}'", tableName);
    List<Map<String, Object>> actualRecords =
        cloudSqlDestinationResourceManager.runSQLQuery(
            String.format("SELECT asset_id FROM %s.%s", destinationSchema, tableName));

    Set<BigDecimal> actualIds =
        actualRecords.stream()
            .map(row -> new BigDecimal(row.get("asset_id").toString()))
            .collect(Collectors.toSet());
    Set<BigDecimal> expectedIds =
        expectedRecords.stream()
            .map(row -> new BigDecimal(row.get("asset_id").toString()))
            .collect(Collectors.toSet());
    assertThat(actualIds).isEqualTo(expectedIds);
  }
}
