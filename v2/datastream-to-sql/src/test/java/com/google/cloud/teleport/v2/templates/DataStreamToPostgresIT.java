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
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
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

  private String replicationSlot;

  private CloudPostgresResourceManager cloudSqlSourceResourceManager;
  private CloudPostgresResourceManager cloudSqlDestinationResourceManager;
  private DatastreamResourceManager datastreamResourceManager;
  private PubsubResourceManager pubsubResourceManager;
  private GcsResourceManager gcsResourceManager;

  @Before
  public void setUp() throws IOException {
    String privateConnectivityId = System.getProperty("privateConnectivityId");
    Objects.requireNonNull(
        privateConnectivityId, "The -DprivateConnectivityId parameter must be set.");

    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity(privateConnectivityId)
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
    final int maxAttempts = 5;
    final long retryDelaySeconds = 10;
    boolean slotCleanupSuccess = false;

    // Only attempt cleanup if a replication slot was created for this test
    if (this.replicationSlot != null && !this.replicationSlot.isBlank()) {
      for (int attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
          LOG.info(
              "Attempt {}/{} to clean up replication slot '{}'...",
              attempt,
              maxAttempts,
              this.replicationSlot);

          // 1. Try to terminate any backend process using the slot.
          // This is wrapped in its own try-catch because it's okay if it fails
          // (e.g., if no process is using the slot).
          try {
            cloudSqlSourceResourceManager.runSQLUpdate(
                String.format(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE slot_name = '%s'",
                    this.replicationSlot));
            LOG.info("Successfully terminated backend for slot '{}'.", this.replicationSlot);
          } catch (Exception e) {
            LOG.warn(
                "Could not terminate backend for slot '{}' on attempt {}, which can be expected: {}",
                this.replicationSlot,
                attempt,
                e.getMessage());
          }

          // 2. Try to drop the replication slot. This is the critical step.
          cloudSqlSourceResourceManager.runSQLUpdate(
              String.format("SELECT pg_drop_replication_slot('%s');", this.replicationSlot));

          LOG.info("Successfully dropped replication slot '{}'.", this.replicationSlot);
          slotCleanupSuccess = true;
          break; // If we get here, cleanup succeeded, so we exit the loop.

        } catch (Exception e) {
          LOG.warn("Attempt {} to clean up replication slot failed: {}.", attempt, e.getMessage());
          if (attempt < maxAttempts) {
            LOG.info("Retrying in {} seconds...", retryDelaySeconds);
            try {
              TimeUnit.SECONDS.sleep(retryDelaySeconds);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              LOG.error("Cleanup retry loop was interrupted.", ie);
              break;
            }
          }
        }
      }

      if (!slotCleanupSuccess) {
        LOG.error(
            "FAILED to clean up replication slot '{}' after {} attempts. Subsequent cleanup may fail.",
            this.replicationSlot,
            maxAttempts);
      }
    }

    // Finally, clean up all other resources.
    ResourceManagerUtils.cleanResources(
        cloudSqlSourceResourceManager,
        cloudSqlDestinationResourceManager,
        datastreamResourceManager,
        pubsubResourceManager,
        gcsResourceManager);
  }

  @Test
  public void testDataStreamPostgresToPostgres() throws IOException {
    // STANDARD TEST: Source and Target schemas have identical (lowercase) casing.
    String tableName = "pg_to_pg_" + RandomStringUtils.randomAlphanumeric(5).toLowerCase();
    cloudSqlSourceResourceManager.createTable(
        tableName, createJdbcSchema(SOURCE_COLUMNS, SOURCE_PK));
    cloudSqlDestinationResourceManager.createTable(
        tableName, createJdbcSchema(SOURCE_COLUMNS, SOURCE_PK));

    // Run the common test logic
    runTest(tableName, tableName);
  }

  @Test
  public void testDataStreamPostgresToPostgresUppercasing() throws IOException {
    // UPPERCASING TEST: Source is lowercase, Target is uppercase and quoted.
    String sourceTableName = "pg_to_pg_" + RandomStringUtils.randomAlphanumeric(5).toLowerCase();
    String destinationTableName = sourceTableName.toUpperCase();

    // Create a lowercase source table
    cloudSqlSourceResourceManager.createTable(
        sourceTableName, createJdbcSchema(SOURCE_COLUMNS, SOURCE_PK));

    // Create an uppercase, case-sensitive target table
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

    // Run the common test logic
    runTest(sourceTableName, destinationTableName);
  }

  private void runTest(String sourceTableName, String destinationTableName) throws IOException {
    // 3. Create required Replication Slot and Publication for Datastream
    this.replicationSlot = "ds_it_slot_" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();
    String publication = "ds_it_pub_" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();
    String user = cloudSqlSourceResourceManager.getUsername();
    String schema = cloudSqlSourceResourceManager.getDatabaseName();

    cloudSqlSourceResourceManager.runSQLUpdate(
        String.format("ALTER USER %s WITH REPLICATION;", user));
    cloudSqlSourceResourceManager.runSQLUpdate(
        String.format(
            "DO $$BEGIN PERFORM pg_create_logical_replication_slot('%s', 'pgoutput'); END$$;",
            this.replicationSlot));
    cloudSqlSourceResourceManager.runSQLUpdate(
        String.format(
            "CREATE PUBLICATION %s FOR TABLE %s.%s;", publication, schema, sourceTableName));
    cloudSqlSourceResourceManager.runSQLUpdate(
        String.format("GRANT USAGE ON SCHEMA %s TO %s;", schema, user));
    cloudSqlSourceResourceManager.runSQLUpdate(
        String.format("GRANT SELECT ON ALL TABLES IN SCHEMA %s TO %s;", schema, user));
    cloudSqlSourceResourceManager.runSQLUpdate(
        String.format(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT SELECT ON TABLES TO %s;", schema, user));

    String datastreamSourceHost = System.getProperty("datastreamSourceHost");
    Objects.requireNonNull(datastreamSourceHost, "-DdatastreamSourceHost is required.");
    String network = System.getProperty("network");
    Objects.requireNonNull(network, "-Dnetwork is required.");
    String region = System.getProperty("region");
    Objects.requireNonNull(region, "-Dregion is required.");
    String subnetName = System.getProperty("subnetName");
    Objects.requireNonNull(subnetName, "-DsubnetName is required.");
    String subnetwork = String.format("regions/%s/subnetworks/%s", region, subnetName);

    JDBCSource jdbcSource =
        PostgresqlSource.builder(
                datastreamSourceHost,
                cloudSqlSourceResourceManager.getUsername(),
                cloudSqlSourceResourceManager.getPassword(),
                cloudSqlSourceResourceManager.getPort(),
                cloudSqlSourceResourceManager.getDatabaseName(),
                this.replicationSlot,
                publication)
            .setAllowedTables(
                Map.of(cloudSqlSourceResourceManager.getDatabaseName(), List.of(sourceTableName)))
            .build();

    String gcsPrefix = getGcsPath(testName + "/cdc/").replace("gs://" + artifactBucketName, "");
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
    gcsResourceManager.createNotification(topic.toString(), "");

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
            .addParameter("databaseHost", datastreamSourceHost)
            .addParameter("network", network)
            .addParameter("subnetwork", subnetwork)
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

    checkJdbcTable(destinationTableName, sourceTableName, cdcEvents);
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
        cdcEvents.put(tableName, new ArrayList<>(rows)); // Store a copy for assertion
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
        String schema = cloudSqlSourceResourceManager.getDatabaseName();
        String qualifiedTableName = schema + "." + tableName;
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
        long expectedRowCount = cdcEvents.get(sourceTableName).size();
        long actualRowCount = -1;

        try {
          // Use logic similar to the buildRowCheck method to get the current row count,
          // which correctly handles both lowercase and case-sensitive (quoted) table names.
          if (destinationTableName.toLowerCase().equals(destinationTableName)) {
            actualRowCount = cloudSqlDestinationResourceManager.getRowCount(destinationTableName);
          } else {
            String countQuery =
                String.format(
                    "SELECT COUNT(*) AS row_count FROM %s.\"%s\"",
                    cloudSqlDestinationResourceManager.getDatabaseName(), destinationTableName);
            List<Map<String, Object>> result =
                cloudSqlDestinationResourceManager.runSQLQuery(countQuery);
            if (result != null && !result.isEmpty() && result.get(0).containsKey("row_count")) {
              actualRowCount = Long.parseLong(result.get(0).get("row_count").toString());
            } else {
              return new CheckResult(false, "Query to count rows failed or returned empty.");
            }
          }

          // 1. First, poll until the row count is correct. This is a resilient check.
          if (actualRowCount != expectedRowCount) {
            return new CheckResult(
                false,
                String.format(
                    "Polling: Found %d rows in '%s', but expected %d.",
                    actualRowCount, destinationTableName, expectedRowCount));
          }

          // 2. If row count matches, perform the final, strict data validation.
          LOG.info(
              "Row count for '{}' matches expected {}. Performing final data validation.",
              destinationTableName,
              expectedRowCount);
          checkJdbcTable(destinationTableName, sourceTableName, cdcEvents);

          // If the above assertion passes, the condition is met.
          return new CheckResult(true, "Destination table row count and content are correct.");

        } catch (AssertionError e) {
          // This can happen in a rare race condition where the count is correct but the
          // content is not *yet*. Returning false allows the test to poll again.
          return new CheckResult(
              false, "Row count is correct, but data does not match yet: " + e.getMessage());
        } catch (Exception e) {
          // Catch any other exceptions during the check.
          return new CheckResult(
              false, "An unexpected error occurred while checking rows: " + e.getMessage());
        }
      }
    };
  }

  private void checkJdbcTable(
      String destinationTableName,
      String sourceTableName,
      Map<String, List<Map<String, Object>>> cdcEvents) {

    List<Map<String, Object>> expectedRecords = cdcEvents.get(sourceTableName);

    // **THE FIX:** This condition correctly identifies if the destination table name
    // has uppercase characters and therefore requires quoting.
    if (!destinationTableName.toLowerCase().equals(destinationTableName)) {

      // This path is now correctly taken by the uppercasing test.
      LOG.info("Executing case-sensitive query for table: \"{}\"", destinationTableName);
      String selectQuery =
          String.format(
              "SELECT * FROM %s.\"%s\"",
              cloudSqlDestinationResourceManager.getDatabaseName(), destinationTableName);
      assertThatRecords(cloudSqlDestinationResourceManager.runSQLQuery(selectQuery))
          .hasRecordsUnorderedCaseInsensitiveColumns(expectedRecords);
    } else {

      // This path is taken by the standard test with a lowercase table name.
      LOG.info("Executing standard readTable for table: {}", destinationTableName);
      assertThatRecords(cloudSqlDestinationResourceManager.readTable(destinationTableName))
          .hasRecordsUnorderedCaseInsensitiveColumns(expectedRecords);
    }
  }
}
