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
import com.google.common.base.CaseFormat;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import org.apache.commons.lang3.StringUtils;
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
public class DataStreamToPostgresMultiTableIT extends TemplateTestBase {

  @Rule public Timeout timeout = new Timeout(60, TimeUnit.MINUTES);
  private static final int NUM_EVENTS_PER_TABLE = 5;
  private static final int NUM_TABLES_TO_CREATE = 5;
  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToPostgresMultiTableIT.class);

  // Define column sets for different test scenarios
  private static final List<String> SOURCE_CAMEL_COLUMNS =
      List.of("idPk", "userName", "itemCount", "itemDesc", "createdAt");
  private static final String SOURCE_CAMEL_PK = SOURCE_CAMEL_COLUMNS.get(0);

  private static final List<String> SOURCE_SNAKE_COLUMNS =
      List.of("id_pk", "user_name", "item_count", "item_desc", "created_at");
  private static final String SOURCE_SNAKE_PK = SOURCE_SNAKE_COLUMNS.get(0);

  private static final List<String> SOURCE_LOWER_COLUMNS =
      List.of("idpk", "username", "itemcount", "itemdesc", "createdat");
  private static final String SOURCE_LOWER_PK = SOURCE_LOWER_COLUMNS.get(0);

  private static final List<String> SOURCE_UPPER_COLUMNS =
      SOURCE_LOWER_COLUMNS.stream().map(String::toUpperCase).collect(Collectors.toList());
  private static final String SOURCE_UPPER_PK = SOURCE_UPPER_COLUMNS.get(0);

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
    ResourceManagerUtils.cleanResources(
        datastreamResourceManager, pubsubResourceManager, gcsResourceManager);

    try {
      if (this.replicationSlot != null && !this.replicationSlot.isBlank()) {
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
      }
    } catch (Exception e) {
      LOG.warn("Error during replication slot cleanup: {}", e.getMessage());
    }

    ResourceManagerUtils.cleanResources(
        cloudSqlSourceResourceManager, cloudSqlDestinationResourceManager);
  }

  /**
   * Tests the CAMEL casing option, verifying both preservation of existing camelCase names and
   * conversion of snake_case names to camelCase.
   */
  @Test
  public void testDataStreamCamelCasing() throws IOException {
    List<String> sourceTableNames = new ArrayList<>();
    Map<String, String> destinationTableNames = new HashMap<>();
    Map<String, List<String>> sourceTableColumns = new HashMap<>();
    Map<String, String> sourceTablePks = new HashMap<>();
    List<String> mappingRules = new ArrayList<>();

    String sourceSchema = cloudSqlSourceResourceManager.getDatabaseName();
    String destinationSchema = cloudSqlDestinationResourceManager.getDatabaseName();
    mappingRules.add(String.format("%s:%s", sourceSchema, destinationSchema));

    for (int i = 0; i < NUM_TABLES_TO_CREATE; i++) {
      if (i < 3) { // Scenario 1: PRESERVATION (camelCase -> camelCase)
        String sourceTable =
            "camel" + StringUtils.capitalize(RandomStringUtils.randomAlphabetic(5).toLowerCase());
        sourceTableNames.add(sourceTable);
        destinationTableNames.put(sourceTable, sourceTable);
        sourceTableColumns.put(sourceTable, SOURCE_CAMEL_COLUMNS);
        sourceTablePks.put(sourceTable, SOURCE_CAMEL_PK);
        createSourceCamelTable(sourceTable);
        createDestinationCamelTable(sourceTable);
      } else { // Scenario 2: CONVERSION (snake_case -> camelCase)
        String sourceTable =
            "snake_" + RandomStringUtils.randomAlphabetic(5).toLowerCase() + "_" + i;
        String destinationTable =
            CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, sourceTable);
        sourceTableNames.add(sourceTable);
        destinationTableNames.put(sourceTable, destinationTable);
        sourceTableColumns.put(sourceTable, SOURCE_SNAKE_COLUMNS);
        sourceTablePks.put(sourceTable, SOURCE_SNAKE_PK);
        cloudSqlSourceResourceManager.createTable(
            sourceTable, createJdbcSchema(SOURCE_SNAKE_COLUMNS, SOURCE_SNAKE_PK));
        createDestinationCamelTable(destinationTable);
      }
    }

    String schemaMap = String.join("|", mappingRules);
    runMultiTableTest(
        sourceTableNames,
        destinationTableNames,
        sourceTableColumns,
        sourceTablePks,
        schemaMap,
        "CAMEL",
        "CAMEL");
  }

  /**
   * Tests the SNAKE casing option, verifying both preservation of existing snake_case names and
   * conversion of camelCase names to snake_case.
   */
  @Test
  public void testDataStreamSnakeCasing() throws IOException {
    List<String> sourceTableNames = new ArrayList<>();
    Map<String, String> destinationTableNames = new HashMap<>();
    Map<String, List<String>> sourceTableColumns = new HashMap<>();
    Map<String, String> sourceTablePks = new HashMap<>();
    List<String> mappingRules = new ArrayList<>();

    String sourceSchema = cloudSqlSourceResourceManager.getDatabaseName();
    String destinationSchema = cloudSqlDestinationResourceManager.getDatabaseName();
    mappingRules.add(String.format("%s:%s", sourceSchema, destinationSchema));

    for (int i = 0; i < NUM_TABLES_TO_CREATE; i++) {
      if (i < 3) { // Scenario 1: PRESERVATION (snake_case -> snake_case)
        String sourceTable =
            "snake_" + RandomStringUtils.randomAlphabetic(5).toLowerCase() + "_" + i;
        sourceTableNames.add(sourceTable);
        destinationTableNames.put(sourceTable, sourceTable);
        sourceTableColumns.put(sourceTable, SOURCE_SNAKE_COLUMNS);
        sourceTablePks.put(sourceTable, SOURCE_SNAKE_PK);
        cloudSqlSourceResourceManager.createTable(
            sourceTable, createJdbcSchema(SOURCE_SNAKE_COLUMNS, SOURCE_SNAKE_PK));
        createDestinationSnakeTable(sourceTable);
      } else { // Scenario 2: CONVERSION (camelCase -> snake_case)
        String sourceTable =
            "camel" + StringUtils.capitalize(RandomStringUtils.randomAlphabetic(5).toLowerCase());
        String destinationTable =
            CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, sourceTable);
        sourceTableNames.add(sourceTable);
        destinationTableNames.put(sourceTable, destinationTable);
        sourceTableColumns.put(sourceTable, SOURCE_CAMEL_COLUMNS);
        sourceTablePks.put(sourceTable, SOURCE_CAMEL_PK);
        createSourceCamelTable(sourceTable);
        createDestinationSnakeTable(destinationTable);
      }
    }

    String schemaMap = String.join("|", mappingRules);
    runMultiTableTest(
        sourceTableNames,
        destinationTableNames,
        sourceTableColumns,
        sourceTablePks,
        schemaMap,
        "SNAKE",
        "SNAKE");
  }

  /**
   * Tests the UPPERCASE casing option, verifying both preservation of existing UPPERCASE names and
   * conversion of lowercase names to UPPERCASE.
   */
  @Test
  public void testDataStreamUppercaseCasing() throws IOException {
    List<String> sourceTableNames = new ArrayList<>();
    Map<String, String> destinationTableNames = new HashMap<>();
    Map<String, List<String>> sourceTableColumns = new HashMap<>();
    Map<String, String> sourceTablePks = new HashMap<>();
    List<String> mappingRules = new ArrayList<>();

    String sourceSchema = cloudSqlSourceResourceManager.getDatabaseName();
    String destinationSchema = cloudSqlDestinationResourceManager.getDatabaseName();
    mappingRules.add(String.format("%s:%s", sourceSchema, destinationSchema));

    for (int i = 0; i < NUM_TABLES_TO_CREATE; i++) {
      if (i < 3) { // Scenario 1: PRESERVATION (UPPERCASE -> UPPERCASE)
        String sourceTable = "UPPER" + RandomStringUtils.randomAlphabetic(5).toUpperCase();
        sourceTableNames.add(sourceTable);
        destinationTableNames.put(sourceTable, sourceTable);
        sourceTableColumns.put(sourceTable, SOURCE_UPPER_COLUMNS);
        sourceTablePks.put(sourceTable, SOURCE_UPPER_PK);
        createSourceUpperTable(sourceTable);
        createDestinationUpperTable(sourceTable);
      } else { // Scenario 2: CONVERSION (lowercase -> UPPERCASE)
        String sourceTable = "lower" + RandomStringUtils.randomAlphabetic(5).toLowerCase();
        String destinationTable = sourceTable.toUpperCase();
        sourceTableNames.add(sourceTable);
        destinationTableNames.put(sourceTable, destinationTable);
        sourceTableColumns.put(sourceTable, SOURCE_LOWER_COLUMNS);
        sourceTablePks.put(sourceTable, SOURCE_LOWER_PK);
        cloudSqlSourceResourceManager.createTable(
            sourceTable, createJdbcSchema(SOURCE_LOWER_COLUMNS, SOURCE_LOWER_PK));
        createDestinationUpperTable(destinationTable);
      }
    }

    String schemaMap = String.join("|", mappingRules);
    runMultiTableTest(
        sourceTableNames,
        destinationTableNames,
        sourceTableColumns,
        sourceTablePks,
        schemaMap,
        "UPPERCASE",
        "UPPERCASE");
  }

  private void runMultiTableTest(
      List<String> sourceTableNames,
      Map<String, String> destinationTableNames,
      Map<String, List<String>> sourceTableColumns,
      Map<String, String> sourceTablePks,
      String schemaMap,
      String defaultCasing,
      String columnCasing)
      throws IOException {

    this.replicationSlot = "ds_it_slot_" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();
    String publication = "ds_it_pub_" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();
    String user = cloudSqlSourceResourceManager.getUsername();
    String sourceDbSchema = cloudSqlSourceResourceManager.getDatabaseName();

    cloudSqlSourceResourceManager.runSQLUpdate(
        String.format("ALTER USER %s WITH REPLICATION;", user));
    cloudSqlSourceResourceManager.runSQLUpdate(
        String.format(
            "DO $$BEGIN PERFORM pg_create_logical_replication_slot('%s', 'pgoutput'); END$$;",
            this.replicationSlot));
    cloudSqlSourceResourceManager.runSQLUpdate(
        String.format("CREATE PUBLICATION %s FOR ALL TABLES;", publication));

    String datastreamSourceHost = System.getProperty("datastreamSourceHost");
    String network = System.getProperty("network");
    String subnetwork =
        String.format("regions/%s/subnetworks/%s", REGION, System.getProperty("subnetName"));

    JDBCSource jdbcSource =
        PostgresqlSource.builder(
                datastreamSourceHost,
                cloudSqlSourceResourceManager.getUsername(),
                cloudSqlSourceResourceManager.getPassword(),
                cloudSqlSourceResourceManager.getPort(),
                sourceDbSchema,
                this.replicationSlot,
                publication)
            .setAllowedTables(Map.of(sourceDbSchema, sourceTableNames))
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

    String jobName = PipelineUtils.createJobName(testName);
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(jobName, specPath)
            .addParameter("inputFilePattern", getGcsPath(testName) + "/cdc/")
            .addParameter("gcsPubSubSubscription", subscription.toString())
            .addParameter("inputFileFormat", "json")
            .addParameter("streamName", stream.getName())
            .addParameter("databaseType", "postgres")
            .addParameter("databaseName", cloudSqlDestinationResourceManager.getDatabaseName())
            .addParameter("defaultCasing", defaultCasing)
            .addParameter("columnCasing", columnCasing)
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

    List<ConditionCheck> allChecks = new ArrayList<>();
    for (String sourceTable : sourceTableNames) {
      List<String> columns = sourceTableColumns.get(sourceTable);
      String pk = sourceTablePks.get(sourceTable);
      String destTable = destinationTableNames.get(sourceTable);

      String destPk;
      if ("SNAKE".equalsIgnoreCase(columnCasing)) {
        destPk = pk.contains("_") ? pk : CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, pk);
      } else if ("UPPERCASE".equalsIgnoreCase(columnCasing)) {
        destPk = pk.toUpperCase();
      } else { // Assume CAMEL
        destPk = pk.contains("_") ? CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, pk) : pk;
      }

      allChecks.add(writePostgresData(sourceTable, columns, cdcEvents));
      allChecks.add(buildRowCheck(destTable, NUM_EVENTS_PER_TABLE));
      allChecks.add(changePostgresData(sourceTable, columns, pk, cdcEvents));
      allChecks.add(checkDestinationRows(destTable, sourceTable, destPk, cdcEvents, columnCasing));
    }

    ChainedConditionCheck conditionCheck = ChainedConditionCheck.builder(allChecks).build();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(createConfig(info, Duration.ofMinutes(25)), conditionCheck);

    for (String sourceTable : sourceTableNames) {
      List<Map<String, Object>> expectedRecords;
      if ("SNAKE".equalsIgnoreCase(columnCasing)) {
        expectedRecords = convertRecordsToSnakeCase(cdcEvents.get(sourceTable));
      } else if ("UPPERCASE".equalsIgnoreCase(columnCasing)) {
        expectedRecords = convertRecordsToUpperCase(cdcEvents.get(sourceTable));
      } else { // Assume CAMEL
        expectedRecords = convertRecordsToCamelCase(cdcEvents.get(sourceTable));
      }
      checkRegularJdbcTable(destinationTableNames.get(sourceTable), expectedRecords);
    }
    assertThatResult(result).meetsConditions();
  }

  private void createSourceCamelTable(String tableName) {
    String columnDefs =
        SOURCE_CAMEL_COLUMNS.stream()
            .map(col -> String.format("\"%s\" VARCHAR(200)", col))
            .collect(Collectors.joining(", "));
    String createSql =
        String.format(
            "CREATE TABLE %s.\"%s\" (%s, PRIMARY KEY (\"%s\"))",
            cloudSqlSourceResourceManager.getDatabaseName(),
            tableName,
            columnDefs,
            SOURCE_CAMEL_PK);
    cloudSqlSourceResourceManager.runSQLUpdate(createSql);
  }

  private void createSourceUpperTable(String tableName) {
    String columnDefs =
        SOURCE_UPPER_COLUMNS.stream()
            .map(col -> String.format("\"%s\" VARCHAR(200)", col))
            .collect(Collectors.joining(", "));
    String createSql =
        String.format(
            "CREATE TABLE %s.\"%s\" (%s, PRIMARY KEY (\"%s\"))",
            cloudSqlSourceResourceManager.getDatabaseName(),
            tableName,
            columnDefs,
            SOURCE_UPPER_PK);
    cloudSqlSourceResourceManager.runSQLUpdate(createSql);
  }

  private void createDestinationCamelTable(String tableName) {
    List<String> destCamelCols =
        SOURCE_SNAKE_COLUMNS.stream()
            .map(col -> CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, col))
            .collect(Collectors.toList());
    String columnDefs =
        destCamelCols.stream()
            .map(col -> String.format("\"%s\" VARCHAR(200)", col))
            .collect(Collectors.joining(", "));
    String createSql =
        String.format(
            "CREATE TABLE %s.\"%s\" (%s, PRIMARY KEY (\"%s\"))",
            cloudSqlDestinationResourceManager.getDatabaseName(),
            tableName,
            columnDefs,
            destCamelCols.get(0));
    cloudSqlDestinationResourceManager.runSQLUpdate(createSql);
  }

  private void createDestinationSnakeTable(String tableName) {
    List<String> destSnakeCols =
        SOURCE_CAMEL_COLUMNS.stream()
            .map(col -> CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, col))
            .collect(Collectors.toList());
    cloudSqlDestinationResourceManager.createTable(
        tableName, createJdbcSchema(destSnakeCols, destSnakeCols.get(0)));
  }

  private void createDestinationUpperTable(String tableName) {
    List<String> destUpperCols =
        SOURCE_LOWER_COLUMNS.stream().map(String::toUpperCase).collect(Collectors.toList());
    String columnDefs =
        destUpperCols.stream()
            .map(col -> String.format("\"%s\" VARCHAR(200)", col))
            .collect(Collectors.joining(", "));
    String createSql =
        String.format(
            "CREATE TABLE %s.\"%s\" (%s, PRIMARY KEY (\"%s\"))",
            cloudSqlDestinationResourceManager.getDatabaseName(),
            tableName,
            columnDefs,
            destUpperCols.get(0));
    cloudSqlDestinationResourceManager.runSQLUpdate(createSql);
  }

  private JDBCResourceManager.JDBCSchema createJdbcSchema(List<String> columns, String pkColumn) {
    HashMap<String, String> schema = new HashMap<>();
    for (String col : columns) {
      schema.put(col, "VARCHAR(200)");
    }
    return new JDBCResourceManager.JDBCSchema(schema, pkColumn);
  }

  private ConditionCheck buildRowCheck(String tableName, int expectedRows) {
    if (tableName.equals(tableName.toLowerCase())) {
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
      String tableName, List<String> columns, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      public @NonNull String getDescription() {
        return "Send initial PostgreSQL events for " + tableName;
      }

      @Override
      public @NonNull CheckResult check() {
        try {
          List<Map<String, Object>> rows = new ArrayList<>();
          for (int i = 0; i < NUM_EVENTS_PER_TABLE; i++) {
            Map<String, Object> values = new HashMap<>();
            values.put(columns.get(0), String.valueOf(i));
            values.put(columns.get(1), RandomStringUtils.randomAlphabetic(10).toLowerCase());
            values.put(columns.get(2), String.valueOf(new Random().nextInt(100)));
            values.put(columns.get(3), new Random().nextInt() % 2 == 0 ? "Y" : "N");
            values.put(columns.get(4), Instant.now().toString());
            rows.add(values);
          }

          LOG.info("Attempting to write {} rows to source table '{}'", rows.size(), tableName);
          String qualifiedTableName;
          String columnNamesSql;

          if (tableName.equals(tableName.toLowerCase())) {
            qualifiedTableName = cloudSqlSourceResourceManager.getDatabaseName() + "." + tableName;
            columnNamesSql = String.join(", ", columns);
          } else {
            qualifiedTableName =
                cloudSqlSourceResourceManager.getDatabaseName() + ".\"" + tableName + "\"";
            columnNamesSql =
                columns.stream().map(c -> "\"" + c + "\"").collect(Collectors.joining(", "));
          }

          for (Map<String, Object> row : rows) {
            List<String> formattedValues = new ArrayList<>();
            for (String col : columns) {
              String escapedValue = row.get(col).toString().replace("'", "''");
              formattedValues.add("'" + escapedValue + "'");
            }
            String valuesSql = String.join(", ", formattedValues);
            String insertSql =
                String.format(
                    "INSERT INTO %s (%s) VALUES (%s)",
                    qualifiedTableName, columnNamesSql, valuesSql);
            cloudSqlSourceResourceManager.runSQLUpdate(insertSql);
          }

          cloudSqlSourceResourceManager.runSQLUpdate("COMMIT;");
          LOG.info("Transaction for initial insert on '{}' committed.", tableName);

          cdcEvents.put(tableName, new ArrayList<>(rows));
          return new CheckResult(
              true, String.format("Sent %d rows to %s.", rows.size(), tableName));

        } catch (Exception e) {
          LOG.error("EXCEPTION CAUGHT while writing to source table '{}': ", tableName, e);
          return new CheckResult(false, "Failed to write to source table: " + e.getMessage());
        }
      }
    };
  }

  private ConditionCheck changePostgresData(
      String tableName,
      List<String> columns,
      String pkColumn,
      Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      public @NonNull String getDescription() {
        return "Send PostgreSQL changes for " + tableName;
      }

      @Override
      protected @NonNull CheckResult check() {
        String qualifiedTableName;
        String pkColumnForQuery;
        String col2ForQuery;
        String col3ForQuery;

        if (tableName.equals(tableName.toLowerCase())) {
          qualifiedTableName = cloudSqlSourceResourceManager.getDatabaseName() + "." + tableName;
          pkColumnForQuery = pkColumn;
          col2ForQuery = columns.get(2);
          col3ForQuery = columns.get(3);
        } else {
          qualifiedTableName =
              cloudSqlSourceResourceManager.getDatabaseName() + ".\"" + tableName + "\"";
          pkColumnForQuery = "\"" + pkColumn + "\"";
          col2ForQuery = "\"" + columns.get(2) + "\"";
          col3ForQuery = "\"" + columns.get(3) + "\"";
        }

        List<Map<String, Object>> previousEvents = cdcEvents.get(tableName);
        List<Map<String, Object>> finalExpectedEvents = new ArrayList<>();

        for (int i = 0; i < NUM_EVENTS_PER_TABLE; i++) {
          if (i % 2 == 0) {
            Map<String, Object> values = new HashMap<>(previousEvents.get(i));
            values.put(columns.get(2), String.valueOf(new Random().nextInt(100)));
            values.put(
                columns.get(3),
                Objects.equals(values.get(columns.get(3)).toString(), "Y") ? "N" : "Y");

            String updateSql =
                String.format(
                    "UPDATE %s SET %s = '%s', %s = '%s' WHERE %s = '%s'",
                    qualifiedTableName,
                    col2ForQuery,
                    values.get(columns.get(2)),
                    col3ForQuery,
                    values.get(columns.get(3)),
                    pkColumnForQuery,
                    values.get(pkColumn));
            cloudSqlSourceResourceManager.runSQLUpdate(updateSql);
            finalExpectedEvents.add(values);
          } else {
            cloudSqlSourceResourceManager.runSQLUpdate(
                String.format(
                    "DELETE FROM %s WHERE %s = '%s'",
                    qualifiedTableName, pkColumnForQuery, previousEvents.get(i).get(pkColumn)));
          }
        }

        cloudSqlSourceResourceManager.runSQLUpdate("COMMIT;");
        LOG.info("Transaction for data changes on '{}' committed.", tableName);

        cdcEvents.put(tableName, finalExpectedEvents);
        return new CheckResult(true, String.format("Sent changes to %s.", tableName));
      }
    };
  }

  private ConditionCheck checkDestinationRows(
      String destinationTableName,
      String sourceTableName,
      String destinationPkName,
      Map<String, List<Map<String, Object>>> cdcEvents,
      String columnCasing) {
    return new ConditionCheck() {
      @Override
      public @NonNull String getDescription() {
        return "Check that destination table has expected row count and content after changes.";
      }

      @Override
      protected @NonNull CheckResult check() {
        List<Map<String, Object>> expectedRecords;
        if ("SNAKE".equalsIgnoreCase(columnCasing)) {
          expectedRecords = convertRecordsToSnakeCase(cdcEvents.get(sourceTableName));
        } else if ("UPPERCASE".equalsIgnoreCase(columnCasing)) {
          expectedRecords = convertRecordsToUpperCase(cdcEvents.get(sourceTableName));
        } else { // Assume CAMEL
          expectedRecords = convertRecordsToCamelCase(cdcEvents.get(sourceTableName));
        }
        long expectedRowCount = expectedRecords.size();
        String destinationSchema = cloudSqlDestinationResourceManager.getDatabaseName();

        try {
          List<Map<String, Object>> actualRecords;
          String qualifiedTableName;
          String pkColumnForQuery = destinationPkName;
          String quotedPkColumnForQuery;

          if (pkColumnForQuery.equals(pkColumnForQuery.toLowerCase())) {
            quotedPkColumnForQuery = pkColumnForQuery;
          } else {
            quotedPkColumnForQuery = "\"" + pkColumnForQuery + "\"";
          }

          if (destinationTableName.equals(destinationTableName.toLowerCase())) {
            qualifiedTableName = destinationSchema + "." + destinationTableName;
          } else {
            qualifiedTableName = destinationSchema + ".\"" + destinationTableName + "\"";
          }

          actualRecords =
              cloudSqlDestinationResourceManager.runSQLQuery(
                  String.format("SELECT %s FROM %s", quotedPkColumnForQuery, qualifiedTableName));

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
                  .map(row -> new BigDecimal(row.get(pkColumnForQuery).toString()))
                  .collect(Collectors.toSet());
          Set<BigDecimal> expectedIds =
              expectedRecords.stream()
                  .map(row -> new BigDecimal(row.get(pkColumnForQuery).toString()))
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

  private List<Map<String, Object>> convertRecordsToCamelCase(List<Map<String, Object>> records) {
    if (records == null) {
      return new ArrayList<>();
    }
    List<Map<String, Object>> camelCaseRecords = new ArrayList<>();
    for (Map<String, Object> record : records) {
      Map<String, Object> camelRecord = new LinkedHashMap<>();
      for (Map.Entry<String, Object> entry : record.entrySet()) {
        String camelKey =
            entry.getKey().contains("_")
                ? CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, entry.getKey())
                : entry.getKey();
        camelRecord.put(camelKey, entry.getValue());
      }
      camelCaseRecords.add(camelRecord);
    }
    return camelCaseRecords;
  }

  private List<Map<String, Object>> convertRecordsToSnakeCase(List<Map<String, Object>> records) {
    if (records == null) {
      return new ArrayList<>();
    }
    List<Map<String, Object>> snakeCaseRecords = new ArrayList<>();
    for (Map<String, Object> record : records) {
      Map<String, Object> snakeRecord = new LinkedHashMap<>();
      for (Map.Entry<String, Object> entry : record.entrySet()) {
        String snakeKey =
            entry.getKey().contains("_")
                ? entry.getKey()
                // Convert camel to snake, and also handle plain lowercase to snake
                : CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entry.getKey());
        snakeRecord.put(snakeKey, entry.getValue());
      }
      snakeCaseRecords.add(snakeRecord);
    }
    return snakeCaseRecords;
  }

  private List<Map<String, Object>> convertRecordsToUpperCase(List<Map<String, Object>> records) {
    if (records == null) {
      return new ArrayList<>();
    }
    List<Map<String, Object>> upperCaseRecords = new ArrayList<>();
    for (Map<String, Object> record : records) {
      Map<String, Object> upperRecord = new LinkedHashMap<>();
      for (Map.Entry<String, Object> entry : record.entrySet()) {
        upperRecord.put(entry.getKey().toUpperCase(), entry.getValue());
      }
      upperCaseRecords.add(upperRecord);
    }
    return upperCaseRecords;
  }

  private void checkRegularJdbcTable(
      String destinationTableName, List<Map<String, Object>> expectedRecords) {

    String destinationSchema = cloudSqlDestinationResourceManager.getDatabaseName();
    String qualifiedTableName;

    if (destinationTableName.equals(destinationTableName.toLowerCase())) {
      qualifiedTableName = destinationSchema + "." + destinationTableName;
    } else {
      qualifiedTableName = destinationSchema + ".\"" + destinationTableName + "\"";
    }

    LOG.info("Executing final validation query for table: {}", qualifiedTableName);
    String selectQuery = String.format("SELECT * FROM %s", qualifiedTableName);
    assertThatRecords(cloudSqlDestinationResourceManager.runSQLQuery(selectQuery))
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedRecords);
  }
}
