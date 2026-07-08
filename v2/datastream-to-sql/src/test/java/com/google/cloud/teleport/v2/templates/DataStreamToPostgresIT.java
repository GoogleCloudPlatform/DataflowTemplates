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
  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String ENTRY_ADDED = "entry_added";
  private static final List<String> COLUMNS = List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED);
  private String replicationSlot;

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
            .setPrivateConnectivity("datastream-connect-2") // Use the parameter
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
    try {
      cloudSqlSourceResourceManager.runSQLUpdate(
          String.format(
              "DO $$ "
                  + "DECLARE slot_pid INTEGER; "
                  + "BEGIN "
                  + "    SELECT active_pid INTO slot_pid FROM pg_replication_slots WHERE slot_name = '%s'; "
                  + "    IF slot_pid IS NOT NULL THEN "
                  + "        PERFORM pg_terminate_backend(slot_pid); "
                  + "    END IF; "
                  + "    PERFORM pg_drop_replication_slot('%s'); "
                  + "EXCEPTION WHEN undefined_object THEN "
                  + "    RAISE NOTICE 'Replication slot %s not found, skipping.'; "
                  + "END $$ LANGUAGE plpgsql;",
              this.replicationSlot, this.replicationSlot, this.replicationSlot));
      LOG.info("Successfully dropped replication slot '{}'.", this.replicationSlot);
    } catch (Exception e) {
      LOG.warn("Error during replication slot cleanup: {}", e.getMessage());
    }

    ResourceManagerUtils.cleanResources(
        datastreamResourceManager,
        pubsubResourceManager,
        gcsResourceManager,
        cloudSqlSourceResourceManager,
        cloudSqlDestinationResourceManager);
  }

  @Test
  public void testDataStreamPostgresToPostgres() throws IOException {
    String tableName = "pg_to_pg_" + RandomStringUtils.randomAlphanumeric(5).toLowerCase();
    cloudSqlSourceResourceManager.createTable(tableName, createJdbcSchema(COLUMNS, ROW_ID));
    cloudSqlDestinationResourceManager.createTable(tableName, createJdbcSchema(COLUMNS, ROW_ID));
    runTest(tableName);
  }

  private void runTest(String sourceTableName) throws IOException {
    this.replicationSlot = "ds_it_slot_" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();
    String publication = "ds_it_pub_" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();
    String user = cloudSqlSourceResourceManager.getUsername();
    String schema = cloudSqlSourceResourceManager.getDatabaseName();
    cloudSqlSourceResourceManager.runSQLUpdate(
        String.format("ALTER USER %s WITH REPLICATION;", user));

    // Try to create the replication slot, with retry logic if slots are full
    createReplicationSlotWithRetry(this.replicationSlot);
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

    JDBCSource jdbcSource =
        PostgresqlSource.builder(
                cloudSqlSourceResourceManager.getHost(),
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
                    JDBCRowsCheck.builder(cloudSqlDestinationResourceManager, sourceTableName)
                        .setMinRows(NUM_EVENTS)
                        .build(),
                    changePostgresData(sourceTableName, cdcEvents),
                    checkDestinationRows(sourceTableName, cdcEvents)))
            .build();
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(createConfig(info, Duration.ofMinutes(20)), conditionCheck);

    checkJdbcTable(sourceTableName, cdcEvents);
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
          values.put(COLUMNS.get(0), i);
          values.put(COLUMNS.get(1), RandomStringUtils.randomAlphabetic(10).toLowerCase());
          values.put(COLUMNS.get(2), new Random().nextInt(100));
          values.put(COLUMNS.get(3), new Random().nextInt() % 2 == 0 ? "Y" : "N");
          values.put(COLUMNS.get(4), Instant.now().toString());
          rows.add(values);
        }
        boolean success = cloudSqlSourceResourceManager.write(tableName, rows);
        cdcEvents.put(tableName, rows);
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
        // 1. Get the schema name from the source resource manager
        String schema = cloudSqlSourceResourceManager.getDatabaseName();
        String qualifiedTableName = schema + "." + tableName;

        List<Map<String, Object>> newCdcEvents = new ArrayList<>();
        for (int i = 0; i < NUM_EVENTS; i++) {
          if (i % 2 == 0) {
            Map<String, Object> values = cdcEvents.get(tableName).get(i);
            values.put(COLUMNS.get(2), new Random().nextInt(100));
            values.put(
                COLUMNS.get(3),
                (Objects.equals(values.get(COLUMNS.get(3)).toString(), "Y") ? "N" : "Y"));

            // 2. Use the fully-qualified table name for the UPDATE
            String updateSql =
                "UPDATE "
                    + qualifiedTableName // <-- FIX
                    + " SET "
                    + COLUMNS.get(2)
                    + " = "
                    + values.get(COLUMNS.get(2))
                    + ", "
                    + COLUMNS.get(3)
                    + " = '"
                    + values.get(COLUMNS.get(3))
                    + "'"
                    + " WHERE "
                    + COLUMNS.get(0)
                    + " = "
                    + i;
            cloudSqlSourceResourceManager.runSQLUpdate(updateSql);
            newCdcEvents.add(values);
          } else {
            // 3. Use the fully-qualified table name for the DELETE
            cloudSqlSourceResourceManager.runSQLUpdate(
                "DELETE FROM "
                    + qualifiedTableName
                    + " WHERE "
                    + COLUMNS.get(0)
                    + "="
                    + i); // <-- FIX
          }
        }
        cdcEvents.put(tableName, newCdcEvents);
        return new CheckResult(
            true, String.format("Sent %d changes to %s.", newCdcEvents.size(), tableName));
      }
    };
  }

  private ConditionCheck checkDestinationRows(
      String tableName, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected @NonNull String getDescription() {
        return "Check JDBC rows.";
      }

      @Override
      protected @NonNull CheckResult check() {
        long totalRows = cloudSqlDestinationResourceManager.getRowCount(tableName);
        long maxRows = cdcEvents.get(tableName).size();
        if (totalRows > maxRows) {
          return new CheckResult(
              false, String.format("Expected up to %d rows but found %d", maxRows, totalRows));
        }
        try {
          checkJdbcTable(tableName, cdcEvents);
          return new CheckResult(true, "JDBC table contains expected rows.");
        } catch (AssertionError error) {
          return new CheckResult(false, "JDBC table does not contain expected rows.");
        }
      }
    };
  }

  private void checkJdbcTable(String tableName, Map<String, List<Map<String, Object>>> cdcEvents) {
    List<Map<String, Object>> expectedRows = cdcEvents.get(tableName);
    List<Map<String, Object>> actualRows = cloudSqlDestinationResourceManager.readTable(tableName);
    assertThatRecords(actualRows).hasRecordsUnordered(expectedRows);
  }

  /**
   * Cleans up any existing replication slots that might be left over from previous test runs. This
   * helps prevent the "all replication slots are in use" error.
   */
  private void cleanupExistingReplicationSlots() {
    try {
      // Get list of existing replication slots that match our test pattern
      String query =
          "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'ds_it_slot_%'";
      List<Map<String, Object>> existingSlots = cloudSqlSourceResourceManager.runSQLQuery(query);

      for (Map<String, Object> slot : existingSlots) {
        String slotName = (String) slot.get("slot_name");
        try {
          cloudSqlSourceResourceManager.runSQLUpdate(
              String.format(
                  "DO $$ "
                      + "DECLARE slot_pid INTEGER; "
                      + "BEGIN "
                      + "    SELECT active_pid INTO slot_pid FROM pg_replication_slots WHERE slot_name = '%s'; "
                      + "    IF slot_pid IS NOT NULL THEN "
                      + "        PERFORM pg_terminate_backend(slot_pid); "
                      + "    END IF; "
                      + "    PERFORM pg_drop_replication_slot('%s'); "
                      + "EXCEPTION WHEN undefined_object THEN "
                      + "    RAISE NOTICE 'Replication slot %s not found, skipping.'; "
                      + "END $$ LANGUAGE plpgsql;",
                  slotName, slotName, slotName));
          LOG.info("Cleaned up existing replication slot: {}", slotName);
        } catch (Exception e) {
          LOG.warn("Failed to cleanup replication slot {}: {}", slotName, e.getMessage());
        }
      }
    } catch (Exception e) {
      LOG.warn("Error during replication slot cleanup: {}", e.getMessage());
    }
  }

  /**
   * Creates a replication slot with retry logic to handle cases where all slots are in use. If slot
   * creation fails due to slot limit, it will attempt to clean up old slots and retry.
   */
  private void createReplicationSlotWithRetry(String slotName) {
    int maxRetries = 3;
    int retryCount = 0;

    while (retryCount < maxRetries) {
      try {
        cloudSqlSourceResourceManager.runSQLUpdate(
            String.format(
                "DO $$BEGIN PERFORM pg_create_logical_replication_slot('%s', 'pgoutput'); END$$;",
                slotName));
        LOG.info("Successfully created replication slot: {}", slotName);
        return;
      } catch (Exception e) {
        if (e.getMessage().contains("all replication slots are in use")) {
          LOG.warn(
              "All replication slots are in use, attempting cleanup and retry {} of {}",
              retryCount + 1,
              maxRetries);

          // Clean up any inactive or old replication slots
          cleanupInactiveReplicationSlots();

          retryCount++;
          if (retryCount < maxRetries) {
            try {
              Thread.sleep(2000); // Wait 2 seconds before retry
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(
                  "Interrupted while waiting to retry replication slot creation", ie);
            }
          }
        } else {
          throw new RuntimeException("Failed to create replication slot: " + e.getMessage(), e);
        }
      }
    }

    throw new RuntimeException(
        "Failed to create replication slot after " + maxRetries + " attempts");
  }

  /** Cleans up inactive replication slots to free up space for new ones. */
  private void cleanupInactiveReplicationSlots() {
    try {
      // Drop inactive replication slots (those without an active connection)
      String query = "SELECT slot_name FROM pg_replication_slots WHERE active = false";
      List<Map<String, Object>> inactiveSlots = cloudSqlSourceResourceManager.runSQLQuery(query);

      for (Map<String, Object> slot : inactiveSlots) {
        String slotName = (String) slot.get("slot_name");
        try {
          cloudSqlSourceResourceManager.runSQLUpdate(
              String.format("SELECT pg_drop_replication_slot('%s')", slotName));
          LOG.info("Dropped inactive replication slot: {}", slotName);
        } catch (Exception e) {
          LOG.warn("Failed to drop inactive replication slot {}: {}", slotName, e.getMessage());
        }
      }
    } catch (Exception e) {
      LOG.warn("Error during inactive replication slot cleanup: {}", e.getMessage());
    }
  }
}
