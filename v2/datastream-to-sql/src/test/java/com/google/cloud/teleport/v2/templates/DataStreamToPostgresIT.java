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

  // Define column names for destination (uppercase)
  private static final String ROW_ID = "ROW_ID";
  private static final String NAME = "NAME";
  private static final String AGE = "AGE";
  private static final String MEMBER = "MEMBER";
  private static final String ENTRY_ADDED = "ENTRY_ADDED";
  private static final List<String> DESTINATION_COLUMNS =
      List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED);

  // Define column names for source (lowercase)
  private static final List<String> SOURCE_COLUMNS =
      DESTINATION_COLUMNS.stream().map(String::toLowerCase).collect(Collectors.toList());
  private static final String SOURCE_PK = ROW_ID.toLowerCase();

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
      if (this.replicationSlot != null && !this.replicationSlot.isBlank()) {

        // First, try to terminate any active connection.
        // It's okay if this fails, so we wrap it in its own try-catch.
        try {
          cloudSqlSourceResourceManager.runSQLUpdate(
              String.format(
                  "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE slot_name = '%s'",
                  this.replicationSlot));
        } catch (Exception e) {
          LOG.info(
              "No active connection to terminate for slot '{}', which is expected.",
              this.replicationSlot);
        }

        // Now, unconditionally drop the replication slot.
        // This will run even if the command above failed.
        cloudSqlSourceResourceManager.runSQLUpdate(
            String.format("SELECT pg_drop_replication_slot('%s');", this.replicationSlot));
      }
    } catch (Exception e) {
      LOG.warn("Error dropping replication slot: {}", e.getMessage());
    }

    /*
    // Clean up the rest of the resources
    ResourceManagerUtils.cleanResources(
        cloudSqlSourceResourceManager,
        cloudSqlDestinationResourceManager,
        datastreamResourceManager,
        pubsubResourceManager,
        gcsResourceManager);
    */
  }

  @Test
  public void testDataStreamPostgresToPostgres() throws IOException {
    // 1. Create source (lowercase) and destination (uppercase) table names
    String randomSuffix = RandomStringUtils.randomAlphanumeric(5).toLowerCase();
    String sourceTableName = "pg_to_pg_" + randomSuffix;
    String destinationTableName = sourceTableName.toUpperCase();

    // 2. Create source and destination tables with different case schemas
    cloudSqlSourceResourceManager.createTable(
        sourceTableName, createJdbcSchema(SOURCE_COLUMNS, SOURCE_PK));

    String destinationSchema = cloudSqlDestinationResourceManager.getDatabaseName();
    cloudSqlDestinationResourceManager.runSQLUpdate(
        String.format(
            "CREATE TABLE %s.\"%s\" (\"%s\" NUMERIC NOT NULL PRIMARY KEY, \"%s\" VARCHAR(200), \"%s\" NUMERIC, \"%s\" VARCHAR(200), \"%s\" VARCHAR(200))",
            destinationSchema, destinationTableName, ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED));

    // Query to check if the table exists
    String checkTableQuery =
        String.format(
            "SELECT COUNT(*) FROM pg_tables WHERE schemaname = '%s' AND tablename = '%s';",
            destinationSchema,
            destinationTableName); // PostgreSQL table names are often lowercased by default

    List<Map<String, Object>> resultForTargetTableTest =
        cloudSqlDestinationResourceManager.runSQLQuery(checkTableQuery);

    // If using runSQLQuery, the subsequent check would change slightly:
    if (resultForTargetTableTest != null && !resultForTargetTableTest.isEmpty()) {
      long count =
          Long.parseLong(
              resultForTargetTableTest
                  .get(0)
                  .get("count")
                  .toString()); // Assuming "count" is the column name
      if (count > 0) {
        System.out.println(
            "✅ Target table "
                + destinationSchema
                + ".\""
                + destinationTableName
                + "\" was successfully created.");
      } else {
        System.err.println(
            "❌ Target table "
                + destinationSchema
                + ".\""
                + destinationTableName
                + "\" was NOT found.");
      }
    } else {
      System.err.println(
          "🚨 Failed to execute table existence check query or received an empty result.");
    }

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

    // Set values
    String datastreamSourceHost = System.getProperty("datastreamSourceHost");
    Objects.requireNonNull(
        datastreamSourceHost,
        "The -DdatastreamSourceHost parameter must be set with the Cloud SQL Private IP.");
    String network = System.getProperty("network");
    Objects.requireNonNull(network, "The -Dnetwork parameter must be set.");

    String region = System.getProperty("region");
    Objects.requireNonNull(region, "The -Dregion parameter must be set.");

    String subnetName = System.getProperty("subnetName");
    Objects.requireNonNull(subnetName, "The -DsubnetName parameter must be set.");
    String subnetwork = String.format("regions/%s/subnetworks/%s", region, subnetName);

    // 4. Create a PostgreSQL source for Datastream pointing to the lowercase table
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

    com.google.pubsub.v1.TopicName topic = pubsubResourceManager.createTopic("gcs-notifications");
    com.google.pubsub.v1.SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, "dataflow-subscription");
    gcsResourceManager.createNotification(topic.toString(), "");

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
                    writePostgresData(sourceTableName, SOURCE_COLUMNS, cdcEvents),
                    // MODIFICATION START: Replaced JDBCRowsCheck with a custom ConditionCheck
                    new ConditionCheck() {
                      @Override
                      public @NonNull String getDescription() {
                        // FIX: Use quotes for destinationTableName in the description for
                        // consistency
                        return String.format(
                            "Check if destination table '\"%s\"' has at least %d rows",
                            destinationTableName, NUM_EVENTS);
                      }

                      @Override
                      protected @NonNull CheckResult check() {
                        // Manually build the query with quoted identifiers to preserve case
                        // This part was already correct for checking a case-sensitive table
                        String countQuery =
                            String.format(
                                "SELECT COUNT(*) AS row_count FROM %s.\"%s\"",
                                cloudSqlDestinationResourceManager.getDatabaseName(),
                                destinationTableName);

                        try {
                          List<Map<String, Object>> result =
                              cloudSqlDestinationResourceManager.runSQLQuery(countQuery);

                          // Ensure result is not null/empty and contains expected key
                          if (result == null
                              || result.isEmpty()
                              || !result.get(0).containsKey("row_count")) {
                            return new CheckResult(
                                false, "Query returned no results or missing 'row_count' column.");
                          }

                          long rowCount = Long.parseLong(result.get(0).get("row_count").toString());

                          if (rowCount >= NUM_EVENTS) {
                            return new CheckResult(
                                true,
                                String.format(
                                    "Found %d rows in '\"%s\"'. Met expectation.", // FIX: Use
                                    // quotes here
                                    // too
                                    rowCount, destinationTableName));
                          } else {
                            return new CheckResult(
                                false,
                                String.format(
                                    "Found %d rows in '\"%s\"', but expected at least %d.", // FIX:
                                    // Use
                                    // quotes here too
                                    rowCount, destinationTableName, NUM_EVENTS));
                          }
                        } catch (Exception e) {
                          // More detailed error logging for debugging
                          return new CheckResult(
                              false,
                              "Error checking row count for '\""
                                  + destinationTableName
                                  + "\": "
                                  + e.getMessage()
                                  + ". Stack: "
                                  + e.toString());
                        }
                      }
                    },
                    // MODIFICATION END
                    changePostgresData(sourceTableName, SOURCE_COLUMNS, cdcEvents),
                    checkDestinationRows(destinationTableName, sourceTableName, cdcEvents)))
            .build();

    // 8. Wait for conditions to be met and cancel the job
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(createConfig(info, Duration.ofMinutes(10)), conditionCheck);

    // 9. Assert the final results
    checkJdbcTable(destinationTableName, sourceTableName, cdcEvents);
    assertThatResult(result).meetsConditions();
  }

  private JDBCResourceManager.JDBCSchema createJdbcSchema(List<String> columns, String pkColumn) {
    HashMap<String, String> schema = new HashMap<>();
    schema.put(columns.get(0), "NUMERIC NOT NULL"); // ROW_ID
    schema.put(columns.get(1), "VARCHAR(200)"); // NAME
    schema.put(columns.get(2), "NUMERIC"); // AGE
    schema.put(columns.get(3), "VARCHAR(200)"); // MEMBER
    schema.put(columns.get(4), "VARCHAR(200)"); // ENTRY_ADDED
    return new JDBCResourceManager.JDBCSchema(schema, pkColumn);
  }

  private ConditionCheck writePostgresData(
      String tableName, List<String> columns, Map<String, List<Map<String, Object>>> cdcEvents) {
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
          values.put(columns.get(0), i);
          values.put(columns.get(1), RandomStringUtils.randomAlphabetic(10).toLowerCase());
          values.put(columns.get(2), new Random().nextInt(100));
          values.put(columns.get(3), new Random().nextInt() % 2 == 0 ? "Y" : "N");
          values.put(columns.get(4), Instant.now().toString());
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
      String tableName, List<String> columns, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      public @NonNull String getDescription() {
        return "Send PostgreSQL changes.";
      }

      @Override
      protected @NonNull CheckResult check() {
        String schema = cloudSqlSourceResourceManager.getDatabaseName();
        String qualifiedTableName = schema + "." + tableName;

        List<Map<String, Object>> newCdcEvents = new ArrayList<>();
        for (int i = 0; i < NUM_EVENTS; i++) {
          if (i % 2 == 0) {
            Map<String, Object> values = cdcEvents.get(tableName).get(i);
            values.put(columns.get(2), new Random().nextInt(100));
            values.put(
                columns.get(3),
                (Objects.equals(values.get(columns.get(3)).toString(), "Y") ? "N" : "Y"));

            String updateSql =
                "UPDATE "
                    + qualifiedTableName
                    + " SET "
                    + columns.get(2)
                    + " = "
                    + values.get(columns.get(2))
                    + ", "
                    + columns.get(3)
                    + " = '"
                    + values.get(columns.get(3))
                    + "'"
                    + " WHERE "
                    + columns.get(0)
                    + " = "
                    + i;
            cloudSqlSourceResourceManager.runSQLUpdate(updateSql);
            newCdcEvents.add(values);
          } else {
            cloudSqlSourceResourceManager.runSQLUpdate(
                "DELETE FROM " + qualifiedTableName + " WHERE " + columns.get(0) + "=" + i);
          }
        }
        cdcEvents.put(tableName, newCdcEvents);
        return new CheckResult(
            true, String.format("Sent %d changes to %s.", newCdcEvents.size(), tableName));
      }
    };
  }

  private ConditionCheck checkDestinationRows(
      String destinationTableName,
      String sourceTableName,
      Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected @NonNull String getDescription() {
        return "Check JDBC rows.";
      }

      @Override
      protected @NonNull CheckResult check() {
        // Build a correctly quoted SQL query to get the row count
        String countQuery =
            String.format(
                "SELECT COUNT(*) AS row_count FROM %s.\"%s\"",
                cloudSqlDestinationResourceManager.getDatabaseName(), destinationTableName);

        LOG.info("Executing the following query: " + countQuery);

        try {
          // Use runSQLQuery to execute the raw SQL
          List<Map<String, Object>> result =
              cloudSqlDestinationResourceManager.runSQLQuery(countQuery);
          long totalRows = Long.parseLong(result.get(0).get("row_count").toString());

          long maxRows = cdcEvents.get(sourceTableName).size();
          if (totalRows > maxRows) {
            return new CheckResult(
                false, String.format("Expected up to %d rows but found %d", maxRows, totalRows));
          }

          checkJdbcTable(destinationTableName, sourceTableName, cdcEvents);
          return new CheckResult(true, "JDBC table contains expected rows.");
        } catch (Exception error) {
          return new CheckResult(
              false, "JDBC table does not contain expected rows yet. Error: " + error.getMessage());
        }
      }
    };
  }

  private void checkJdbcTable(
      String destinationTableName,
      String sourceTableName,
      Map<String, List<Map<String, Object>>> cdcEvents) {

    // Build a correctly quoted SQL query to read all table data
    String selectQuery =
        String.format(
            "SELECT * FROM %s.\"%s\"",
            cloudSqlDestinationResourceManager.getDatabaseName(), destinationTableName);

    // Use runSQLQuery instead of readTable to execute the correct query
    assertThatRecords(cloudSqlDestinationResourceManager.runSQLQuery(selectQuery))
        .hasRecordsUnorderedCaseInsensitiveColumns(cdcEvents.get(sourceTableName));
  }
}
