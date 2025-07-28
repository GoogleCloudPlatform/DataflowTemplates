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
    String privateConnectivityId = System.getProperty("privateConnectivityId");
    Objects.requireNonNull(
        privateConnectivityId, "The -DprivateConnectivityId parameter must be set.");

    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity(privateConnectivityId) // Use the parameter
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
    // Use a try-catch block to make cleanup resilient
    try {
      // Drop the replication slot BEFORE cleaning up the database resource
      if (this.replicationSlot != null && !this.replicationSlot.isBlank()) {

        // 1. Forcefully terminate any backend connection that is using the slot.
        //    This query finds the active process ID from the pg_replication_slots table
        //    and terminates it.
        cloudSqlSourceResourceManager.runSQLUpdate(
            String.format(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '%s' AND active = 't'",
                this.replicationSlot));

        // 2. Now that the connection is severed, drop the replication slot.
        cloudSqlSourceResourceManager.runSQLUpdate(
            String.format("SELECT pg_drop_replication_slot('%s');", this.replicationSlot));
      }
    } catch (Exception e) {
      LOG.warn("Error during replication slot cleanup: {}", e.getMessage());
    }

    // Clean up the rest of the resources
    ResourceManagerUtils.cleanResources(
        cloudSqlSourceResourceManager,
        cloudSqlDestinationResourceManager,
        datastreamResourceManager,
        pubsubResourceManager,
        gcsResourceManager);
  }

  @Test
  public void testDataStreamPostgresToPostgres() throws IOException {
    // 1. Create source and destination tables
    String tableName = "pg_to_pg_" + RandomStringUtils.randomAlphanumeric(5).toLowerCase();
    cloudSqlSourceResourceManager.createTable(tableName, createJdbcSchema());
    cloudSqlDestinationResourceManager.createTable(tableName, createJdbcSchema());

    // 3. Create required Replication Slot and Publication for Datastream
    this.replicationSlot = "ds_it_slot_" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();
    String publication = "ds_it_pub_" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();
    // Succeeds because PERFORM discards the result
    String user = cloudSqlSourceResourceManager.getUsername();
    String schema = cloudSqlSourceResourceManager.getDatabaseName();

    // Step 3a: Ensure the user has REPLICATION privilege.
    // This is idempotent; it's safe to run even if the user already has the role.
    cloudSqlSourceResourceManager.runSQLUpdate(
        String.format("ALTER USER %s WITH REPLICATION;", user));

    // Step 3b: Create the logical replication slot.
    // The DO/PERFORM block is used to discard the success message.
    cloudSqlSourceResourceManager.runSQLUpdate(
        String.format(
            "DO $$BEGIN PERFORM pg_create_logical_replication_slot('%s', 'pgoutput'); END$$;",
            this.replicationSlot));

    // Step 3c: Create the publication.
    // We scope it to the specific table used in the test.
    cloudSqlSourceResourceManager.runSQLUpdate(
        String.format("CREATE PUBLICATION %s FOR TABLE %s.%s;", publication, schema, tableName));

    // Step 3d: Grant privileges for the Datastream user to read schema and tables.
    // This is CRITICAL for the initial backfill/snapshot to succeed.
    cloudSqlSourceResourceManager.runSQLUpdate(
        String.format("GRANT USAGE ON SCHEMA %s TO %s;", schema, user));
    cloudSqlSourceResourceManager.runSQLUpdate(
        String.format("GRANT SELECT ON ALL TABLES IN SCHEMA %s TO %s;", schema, user));

    // Step 3e: Grant future SELECT privileges. This handles any tables
    // that might be created later or transiently.
    cloudSqlSourceResourceManager.runSQLUpdate(
        String.format(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT SELECT ON TABLES TO %s;", schema, user));

    String datastreamSourceHost = System.getProperty("datastreamSourceHost");
    Objects.requireNonNull(
        datastreamSourceHost,
        "The -DdatastreamSourceHost parameter must be set with the Cloud SQL Private IP.");

    // 4. Create a PostgreSQL source for Datastream
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
                Map.of(cloudSqlSourceResourceManager.getDatabaseName(), List.of(tableName)))
            .build();

    // 5. Create Datastream resources
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

    // Create a Pub/Sub topic for GCS notifications
    com.google.pubsub.v1.TopicName topic = pubsubResourceManager.createTopic("gcs-notifications");

    // Create a subscription for Dataflow to listen to
    com.google.pubsub.v1.SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, "dataflow-subscription");

    // Create a GCS notification on the artifact bucket to publish to the new topic
    gcsResourceManager.createNotification(topic.toString(), "");

    // Schema Map
    String schemaMap =
        String.format(
            "%s:%s",
            cloudSqlSourceResourceManager.getDatabaseName(),
            cloudSqlDestinationResourceManager.getDatabaseName());

    // 6. Construct and launch the Dataflow template
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
            .addParameter("network", "datastream-vpc")
            .addParameter("subnetwork", "regions/us-central1/subnetworks/sql-subnet")
            .addParameter(
                "databasePort", String.valueOf(cloudSqlDestinationResourceManager.getPort()))
            .addParameter("databaseUser", cloudSqlDestinationResourceManager.getUsername())
            .addParameter("databasePassword", cloudSqlDestinationResourceManager.getPassword());

    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // 7. Prepare and run the data validation checks
    Map<String, List<Map<String, Object>>> cdcEvents = new HashMap<>();
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writePostgresData(tableName, cdcEvents),
                    JDBCRowsCheck.builder(cloudSqlDestinationResourceManager, tableName)
                        .setMinRows(NUM_EVENTS)
                        .build(),
                    changePostgresData(tableName, cdcEvents),
                    checkDestinationRows(tableName, cdcEvents)))
            .build();

    // 8. Wait for conditions to be met and cancel the job
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(createConfig(info, Duration.ofMinutes(30)), conditionCheck);

    // 9. Assert the final results
    checkJdbcTable(tableName, cdcEvents);
    assertThatResult(result).meetsConditions();
  }

  private JDBCResourceManager.JDBCSchema createJdbcSchema() {
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    return new JDBCResourceManager.JDBCSchema(columns, ROW_ID);
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
    assertThatRecords(cloudSqlDestinationResourceManager.readTable(tableName))
        .hasRecordsUnorderedCaseInsensitiveColumns(cdcEvents.get(tableName));
  }
}
