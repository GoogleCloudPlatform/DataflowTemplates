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
package com.google.cloud.teleport.v2.templates;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.apache.beam.it.gcp.datastream.OracleSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerTemplateITBase;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Integration test for {@link DataStreamToSpanner} Flex template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(Parameterized.class)
public class DataStreamToSpannerIT extends SpannerTemplateITBase {

  enum JDBCType {
    MYSQL,
    ORACLE
  }

  private static final Integer NUM_EVENTS = 10;

  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String ENTRY_ADDED = "entry_added";

  private String gcsPrefix;
  private String dlqGcsPrefix;

  private SubscriptionName subscription;
  private SubscriptionName dlqSubscription;

  private static final List<String> COLUMNS = List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED);

  private CloudSqlResourceManager cloudSqlResourceManager;
  private DatastreamResourceManager datastreamResourceManager;
  private SpannerResourceManager spannerResourceManager;
  private PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws IOException {
    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity("datastream-private-connect-us-central1")
            .build();

    gcsPrefix = getGcsPath(testName + "/cdc/").replace("gs://" + artifactBucketName, "");
    dlqGcsPrefix = getGcsPath(testName + "/dlq/").replace("gs://" + artifactBucketName, "");
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        cloudSqlResourceManager,
        datastreamResourceManager,
        spannerResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testDataStreamMySqlToSpanner() throws IOException {
    // Run a simple IT
    simpleMySqlToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        Dialect.GOOGLE_STANDARD_SQL,
        Function.identity());
  }

  @Test
  public void testDataStreamOracleToSpanner() throws IOException {
    // Run a simple IT
    simpleOracleToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        Dialect.GOOGLE_STANDARD_SQL,
        Function.identity());
  }

  @Test
  public void testDataStreamMySqlToPostgresSpanner() throws IOException {
    // Run a simple IT
    simpleMySqlToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        Dialect.POSTGRESQL,
        Function.identity());
  }

  @Test
  public void testDataStreamMySqlToSpannerStreamingEngine() throws IOException {
    // Run a simple IT
    simpleMySqlToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        Dialect.GOOGLE_STANDARD_SQL,
        config -> config.addEnvironment("enableStreamingEngine", true));
  }

  @Test
  public void testDataStreamMySqlToSpannerJson() throws IOException {
    // Run a simple IT
    simpleMySqlToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.JSON_FILE_FORMAT,
        Dialect.GOOGLE_STANDARD_SQL,
        Function.identity());
  }

  @Test
  public void testDataStreamOracleToSpannerJson() throws IOException {
    // Run a simple IT
    simpleOracleToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.JSON_FILE_FORMAT,
        Dialect.GOOGLE_STANDARD_SQL,
        Function.identity());
  }

  private void simpleMySqlToSpannerTest(
      DatastreamResourceManager.DestinationOutputFormat fileFormat,
      Dialect spannerDialect,
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {

    simpleJdbcToSpannerTest(
        JDBCType.MYSQL,
        fileFormat,
        spannerDialect,
        config ->
            paramsAdder.apply(
                config.addParameter("sessionFilePath", getGcsPath("input/mysql-session.json"))));
  }

  private void simpleOracleToSpannerTest(
      DatastreamResourceManager.DestinationOutputFormat fileFormat,
      Dialect spannerDialect,
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {

    simpleJdbcToSpannerTest(
        JDBCType.ORACLE, fileFormat, spannerDialect, config -> paramsAdder.apply(config));
  }

  private void simpleJdbcToSpannerTest(
      JDBCType jdbcType,
      DatastreamResourceManager.DestinationOutputFormat fileFormat,
      Dialect spannerDialect,
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {

    // Create JDBC Resource manager
    cloudSqlResourceManager =
        jdbcType.equals(JDBCType.MYSQL)
            ? CloudMySQLResourceManager.builder(testName).build()
            : CloudOracleResourceManager.builder(testName).build();

    // Create Spanner Resource Manager
    SpannerResourceManager.Builder spannerResourceManagerBuilder =
        SpannerResourceManager.builder(testName, PROJECT, REGION, spannerDialect)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .setCredentials(credentials);
    spannerResourceManager = spannerResourceManagerBuilder.build();

    // Generate table names
    List<String> tableNames =
        List.of(
            "DatastreamToSpanner_1_" + RandomStringUtils.randomAlphanumeric(5),
            "DatastreamToSpanner_2_" + RandomStringUtils.randomAlphanumeric(5));

    // Generate session file
    if (jdbcType.equals(JDBCType.MYSQL)) {
      gcsClient.createArtifact(
          "input/mysql-session.json",
          generateSessionFile(
              cloudSqlResourceManager.getDatabaseName(),
              spannerResourceManager.getDatabaseId(),
              tableNames));
    }

    // Create JDBC tables
    tableNames.forEach(
        tableName ->
            cloudSqlResourceManager.createTable(
                tableName, createJdbcSchema(jdbcType.equals(JDBCType.ORACLE))));

    JDBCSource jdbcSource;
    if (jdbcType.equals(JDBCType.MYSQL)) {
      jdbcSource =
          MySQLSource.builder(
                  cloudSqlResourceManager.getHost(),
                  cloudSqlResourceManager.getUsername(),
                  cloudSqlResourceManager.getPassword(),
                  cloudSqlResourceManager.getPort())
              .setAllowedTables(Map.of(cloudSqlResourceManager.getDatabaseName(), tableNames))
              .build();
    } else {
      jdbcSource =
          OracleSource.builder(
                  cloudSqlResourceManager.getHost(),
                  cloudSqlResourceManager.getUsername(),
                  cloudSqlResourceManager.getPassword(),
                  cloudSqlResourceManager.getPort(),
                  cloudSqlResourceManager.getDatabaseName())
              .setAllowedTables(
                  Map.of(
                      cloudSqlResourceManager.getUsername().toUpperCase(),
                      List.of(tableNames.get(0).toUpperCase(), tableNames.get(1).toUpperCase())))
              .build();
    }

    // Create Spanner tables
    createSpannerTables(tableNames, spannerDialect);

    // Create Datastream JDBC Source Connection profile and config
    SourceConfig sourceConfig =
        datastreamResourceManager.buildJDBCSourceConfig("jdbc-profile", jdbcSource);

    // Create Datastream GCS Destination Connection profile and config
    DestinationConfig destinationConfig =
        datastreamResourceManager.buildGCSDestinationConfig(
            "gcs-profile", artifactBucketName, gcsPrefix, fileFormat);

    // Create and start Datastream stream
    Stream stream =
        datastreamResourceManager.createStream("stream1", sourceConfig, destinationConfig);
    datastreamResourceManager.startStream(stream);

    // Construct template
    createPubSubNotifications();
    String jobName = PipelineUtils.createJobName(testName);
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(jobName, specPath)
                .addParameter("gcsPubSubSubscription", subscription.toString())
                .addParameter("dlqGcsPubSubSubscription", dlqSubscription.toString())
                .addParameter("streamName", stream.getName())
                .addParameter("instanceId", spannerResourceManager.getInstanceId())
                .addParameter("databaseId", spannerResourceManager.getDatabaseId())
                .addParameter("projectId", PROJECT)
                .addParameter("deadLetterQueueDirectory", getGcsPath(testName) + "/dlq/")
                .addParameter("spannerHost", spannerResourceManager.getSpannerHost())
                .addParameter(
                    "inputFileFormat",
                    fileFormat.equals(
                            DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT)
                        ? "avro"
                        : "json"));

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // Construct a ChainedConditionCheck with 4 stages.
    // 1. Send initial wave of events to JDBC
    // 2. Wait on Spanner to merge events from staging to destination
    // 3. Send wave of mutations to JDBC
    // 4. Wait on Spanner to merge second wave of events
    Map<String, List<Map<String, Object>>> cdcEvents = new HashMap<>();
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeJdbcData(tableNames, cdcEvents, jdbcType.equals(JDBCType.ORACLE)),
                    SpannerRowsCheck.builder(spannerResourceManager, tableNames.get(0))
                        .setMinRows(NUM_EVENTS)
                        .build(),
                    SpannerRowsCheck.builder(spannerResourceManager, tableNames.get(1))
                        .setMinRows(NUM_EVENTS)
                        .build(),
                    changeJdbcData(tableNames, cdcEvents, jdbcType.equals(JDBCType.ORACLE)),
                    checkDestinationRows(tableNames, cdcEvents)))
            .build();

    // Job needs to be cancelled as draining will time out
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(createConfig(info, Duration.ofMinutes(20)), conditionCheck);

    // Assert
    checkSpannerTables(tableNames, cdcEvents);
    assertThatResult(result).meetsConditions();
  }

  private String generateSessionFile(String srcDb, String spannerDb, List<String> tableNames)
      throws IOException {
    String sessionFile =
        Files.readString(
            Paths.get(Resources.getResource("DataStreamToSpannerIT/mysql-session.json").getPath()));
    return sessionFile
        .replaceAll("SRC_DATABASE", srcDb)
        .replaceAll("SP_DATABASE", spannerDb)
        .replaceAll("TABLE1", tableNames.get(0))
        .replaceAll("TABLE2", tableNames.get(1));
  }

  private JDBCResourceManager.JDBCSchema createJdbcSchema(boolean isOracle) {
    // Arrange MySQL-compatible schema
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, (isOracle ? "NUMBER" : "NUMERIC") + " NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, isOracle ? "NUMBER" : "NUMERIC");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    return new JDBCResourceManager.JDBCSchema(columns, ROW_ID);
  }

  private void createPubSubNotifications() throws IOException {
    // Instantiate pubsub resource manager for notifications
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();

    // Create pubsub notifications
    TopicName topic = pubsubResourceManager.createTopic("it");
    TopicName dlqTopic = pubsubResourceManager.createTopic("dlq");
    subscription = pubsubResourceManager.createSubscription(topic, "it-sub");
    dlqSubscription = pubsubResourceManager.createSubscription(dlqTopic, "dlq-sub");
    gcsClient.createNotification(topic.toString(), gcsPrefix.substring(1));
    gcsClient.createNotification(dlqTopic.toString(), dlqGcsPrefix.substring(1));
  }

  private void createSpannerTables(List<String> tableNames, Dialect spannerDialect) {
    boolean usingPg = Dialect.POSTGRESQL.equals(spannerDialect);
    // Create Spanner dataset
    tableNames.forEach(
        tableName ->
            spannerResourceManager.executeDdlStatement(
                "CREATE TABLE "
                    + tableName
                    + " ("
                    + ROW_ID
                    + (usingPg ? " bigint " : " INT64 ")
                    + "NOT NULL, "
                    + NAME
                    + (usingPg ? " character varying(50), " : " STRING(1024), ")
                    + AGE
                    + (usingPg ? " bigint, " : " INT64, ")
                    + MEMBER
                    + (usingPg ? " character varying(50), " : " STRING(1024), ")
                    + ENTRY_ADDED
                    + (usingPg ? " character varying(50)" : " STRING(1024)")
                    + (usingPg ? ", " : ") ")
                    + "PRIMARY KEY ("
                    + ROW_ID
                    + ")"
                    + (usingPg ? ")" : "")));
  }

  /**
   * Helper function for constructing a ConditionCheck whose check() method checks the rows in the
   * destination Spanner database for specific rows.
   *
   * @return A ConditionCheck containing the check operation.
   */
  private ConditionCheck checkDestinationRows(
      List<String> tableNames, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Check Spanner rows.";
      }

      @Override
      protected CheckResult check() {
        // First, check that correct number of rows were deleted.
        for (String tableName : tableNames) {
          long totalRows = spannerResourceManager.getRowCount(tableName);
          long maxRows = cdcEvents.get(tableName).size();
          if (totalRows > maxRows) {
            return new CheckResult(
                false, String.format("Expected up to %d rows but found %d", maxRows, totalRows));
          }
        }

        // Next, make sure in-place mutations were applied.
        try {
          checkSpannerTables(tableNames, cdcEvents);
          return new CheckResult(true, "Spanner tables contain expected rows.");
        } catch (AssertionError error) {
          return new CheckResult(false, "Spanner tables do not contain expected rows.");
        }
      }
    };
  }

  /** Helper function for checking the rows of the destination Spanner tables. */
  private void checkSpannerTables(
      List<String> tableNames, Map<String, List<Map<String, Object>>> cdcEvents) {
    tableNames.forEach(
        tableName ->
            SpannerAsserts.assertThatStructs(
                    spannerResourceManager.readTableRecords(tableName, COLUMNS))
                .hasRecordsUnorderedCaseInsensitiveColumns(cdcEvents.get(tableName)));
  }

  /**
   * Helper function for constructing a ConditionCheck whose check() method constructs the initial
   * rows of data in the JDBC database according to the common schema for the IT's in this class.
   *
   * @return A ConditionCheck containing the JDBC write operation.
   */
  private ConditionCheck writeJdbcData(
      List<String> tableNames, Map<String, List<Map<String, Object>>> cdcEvents, boolean isOracle) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Send initial JDBC events.";
      }

      @Override
      protected CheckResult check() {
        boolean success = true;
        List<String> messages = new ArrayList<>();
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
          success &= cloudSqlResourceManager.write(tableName, rows);
          messages.add(String.format("%d rows to %s", rows.size(), tableName));
        }

        // Force log file archive - needed so Datastream can see changes which are read from
        // archived log files.
        if (isOracle) {
          cloudSqlResourceManager.runSQLUpdate("ALTER SYSTEM SWITCH LOGFILE");
        }
        return new CheckResult(success, "Sent " + String.join(", ", messages) + ".");
      }
    };
  }

  /**
   * Helper function for constructing a ConditionCheck whose check() method changes rows of data in
   * the JDBC database according to the common schema for the IT's in this class. Half the rows are
   * mutated and half are removed completely.
   *
   * @return A ConditionCheck containing the JDBC mutate operation.
   */
  private ConditionCheck changeJdbcData(
      List<String> tableNames, Map<String, List<Map<String, Object>>> cdcEvents, boolean isOracle) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Send JDBC changes.";
      }

      @Override
      protected CheckResult check() {
        List<String> messages = new ArrayList<>();
        for (String tableName : tableNames) {

          List<Map<String, Object>> newCdcEvents = new ArrayList<>();
          for (int i = 0; i < NUM_EVENTS; i++) {
            if (i % 2 == 0) {
              Map<String, Object> values = cdcEvents.get(tableName).get(i);
              values.put(COLUMNS.get(1), values.get(COLUMNS.get(1)).toString().toUpperCase());
              values.put(COLUMNS.get(2), new Random().nextInt(100));
              values.put(
                  COLUMNS.get(3),
                  (Objects.equals(values.get(COLUMNS.get(3)).toString(), "Y") ? "N" : "Y"));

              String updateSql =
                  "UPDATE "
                      + tableName
                      + " SET "
                      + COLUMNS.get(1)
                      + " = '"
                      + values.get(COLUMNS.get(1))
                      + "',"
                      + COLUMNS.get(2)
                      + " = "
                      + values.get(COLUMNS.get(2))
                      + ","
                      + COLUMNS.get(3)
                      + " = '"
                      + values.get(COLUMNS.get(3))
                      + "'"
                      + " WHERE "
                      + COLUMNS.get(0)
                      + " = "
                      + i;
              cloudSqlResourceManager.runSQLUpdate(updateSql);
              newCdcEvents.add(values);
            } else {
              cloudSqlResourceManager.runSQLUpdate(
                  "DELETE FROM " + tableName + " WHERE " + COLUMNS.get(0) + "=" + i);
            }
          }
          cdcEvents.put(tableName, newCdcEvents);
          messages.add(String.format("%d changes to %s", newCdcEvents.size(), tableName));
        }

        // Force log file archive - needed so Datastream can see changes which are read from
        // archived log files.
        if (isOracle) {
          cloudSqlResourceManager.runSQLUpdate("ALTER SYSTEM SWITCH LOGFILE");
        }
        return new CheckResult(true, "Sent " + String.join(", ", messages) + ".");
      }
    };
  }
}
