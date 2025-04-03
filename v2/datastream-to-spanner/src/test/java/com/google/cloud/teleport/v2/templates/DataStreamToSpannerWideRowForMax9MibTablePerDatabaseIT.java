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

import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerTemplateITBase;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
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
public class DataStreamToSpannerWideRowForMax9MibTablePerDatabaseIT extends SpannerTemplateITBase {
  private static final int STRING_LENGTH = 200;
  private static final Integer NUM_EVENTS = 1;
  private static final Integer NUM_TABLES = 1;

  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String ENTRY_ADDED = "entry_added";
  private static final String LARGE_BLOB_ADDED = "large_blob";

  private String gcsPrefix;
  private String dlqGcsPrefix;

  private SubscriptionName subscription;
  private SubscriptionName dlqSubscription;

  private static final List<String> COLUMNS =
      List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED, LARGE_BLOB_ADDED);

  private CloudSqlResourceManager cloudSqlResourceManager;
  private DatastreamResourceManager datastreamResourceManager;
  private SpannerResourceManager spannerResourceManager;
  private PubsubResourceManager pubsubResourceManager;

  private GcsResourceManager gcsResourceManager;

  @Before
  public void setUp() throws IOException {
    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .build();

    gcsResourceManager = setUpSpannerITGcsResourceManager();
    gcsPrefix =
        getGcsPath(testName + "/cdc/", gcsResourceManager)
            .replace("gs://" + gcsResourceManager.getBucket(), "");
    dlqGcsPrefix =
        getGcsPath(testName + "/dlq/", gcsResourceManager)
            .replace("gs://" + gcsResourceManager.getBucket(), "");
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        cloudSqlResourceManager,
        datastreamResourceManager,
        spannerResourceManager,
        pubsubResourceManager,
        gcsResourceManager);
  }

  @Test
  public void testDataStreamMySqlToSpannerFor5000TablesPerDatabase() throws IOException {
    simpleMaxMySqlTablesPerDatabaseToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        Dialect.GOOGLE_STANDARD_SQL,
        Function.identity());
  }

  @Test
  public void testDataStreamMySqlToSpannerStreamingEngine() throws IOException {
    simpleMaxMySqlTablesPerDatabaseToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        Dialect.GOOGLE_STANDARD_SQL,
        config -> config.addEnvironment("enableStreamingEngine", true));
  }

  @Test
  public void testDataStreamMySqlToSpannerJson() throws IOException {
    simpleMaxMySqlTablesPerDatabaseToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.JSON_FILE_FORMAT,
        Dialect.GOOGLE_STANDARD_SQL,
        Function.identity());
  }

  private void simpleMaxMySqlTablesPerDatabaseToSpannerTest(
      DatastreamResourceManager.DestinationOutputFormat fileFormat,
      Dialect spannerDialect,
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {

    simpleJdbcToSpannerTest(
        fileFormat,
        spannerDialect,
        config ->
            paramsAdder.apply(
                config.addParameter(
                    "sessionFilePath",
                    getGcsPath("input/mysql-session.json", gcsResourceManager))));
  }

  private void simpleJdbcToSpannerTest(
      DatastreamResourceManager.DestinationOutputFormat fileFormat,
      Dialect spannerDialect,
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {

    // Create JDBC Resource manager
    cloudSqlResourceManager = CloudMySQLResourceManager.builder(testName).build();

    // Create Spanner Resource Manager
    SpannerResourceManager.Builder spannerResourceManagerBuilder =
        SpannerResourceManager.builder(testName, PROJECT, REGION, spannerDialect)
            .maybeUseStaticInstance();
    spannerResourceManager = spannerResourceManagerBuilder.build();

    List<String> tableNames = new ArrayList<>();
    for (int i = 1; i <= NUM_TABLES; i++) {
      tableNames.add("DataStreamToSpanner_" + i + "_" + RandomStringUtils.randomAlphanumeric(5));
    }

    gcsResourceManager.createArtifact(
        "input/mysql-session.json",
        generateSessionFile(
            cloudSqlResourceManager.getDatabaseName(),
            spannerResourceManager.getDatabaseId(),
            tableNames));

    // Create JDBC tables
    tableNames.forEach(
        tableName -> cloudSqlResourceManager.createTable(tableName, createJdbcSchema()));

    JDBCSource jdbcSource =
        MySQLSource.builder(
                cloudSqlResourceManager.getHost(),
                cloudSqlResourceManager.getUsername(),
                cloudSqlResourceManager.getPassword(),
                cloudSqlResourceManager.getPort())
            .setAllowedTables(Map.of(cloudSqlResourceManager.getDatabaseName(), tableNames))
            .build();

    // Create Spanner tables
    createSpannerTables(tableNames);

    // Create Datastream JDBC Source Connection profile and config
    SourceConfig sourceConfig =
        datastreamResourceManager.buildJDBCSourceConfig("jdbc-profile", jdbcSource);

    // Create Datastream GCS Destination Connection profile and config
    DestinationConfig destinationConfig =
        datastreamResourceManager.buildGCSDestinationConfig(
            "gcs-profile", gcsResourceManager.getBucket(), gcsPrefix, fileFormat);

    // Create and start Datastream stream
    Stream stream =
        datastreamResourceManager.createStream("stream1", sourceConfig, destinationConfig);
    datastreamResourceManager.startStream(stream);

    // Construct template
    createPubSubNotifications();
    String jobName = PipelineUtils.createJobName(testName);
    LaunchConfig.Builder options =
        paramsAdder.apply(
            LaunchConfig.builder(jobName, specPath)
                .addParameter("gcsPubSubSubscription", subscription.toString())
                .addParameter("dlqGcsPubSubSubscription", dlqSubscription.toString())
                .addParameter("streamName", stream.getName())
                .addParameter("instanceId", spannerResourceManager.getInstanceId())
                .addParameter("databaseId", spannerResourceManager.getDatabaseId())
                .addParameter("projectId", PROJECT)
                .addParameter(
                    "deadLetterQueueDirectory", getGcsPath(testName, gcsResourceManager) + "/dlq/")
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
    Map<String, List<Map<String, Object>>> cdcEvents = new LinkedHashMap<>();
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeJdbcData(tableNames, cdcEvents),
                    SpannerRowsCheck.builder(spannerResourceManager, tableNames.get(0))
                        .setMinRows(NUM_EVENTS)
                        .build(),
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

    String sessionFile = generateBaseSchema();
    String sessionFileContent =
        sessionFile.replaceAll("SRC_DATABASE", srcDb).replaceAll("SP_DATABASE", spannerDb);
    for (int i = 1; i <= NUM_TABLES; i++) {
      sessionFileContent = sessionFileContent.replaceAll("TABLE" + i, tableNames.get(i - 1));
    }
    return sessionFileContent;
  }

  private String generateBaseSchema() throws IOException {
    Map<String, Object> sessionTemplate = createSessionTemplate();

    Gson gson = new GsonBuilder().setPrettyPrinting().create();

    return gson.toJson(sessionTemplate);
  }

  public static Map<String, Object> createSessionTemplate() {
    Map<String, Object> sessionTemplate = new LinkedHashMap<>();
    sessionTemplate.put("SessionName", "NewSession");
    sessionTemplate.put("EditorName", "");
    sessionTemplate.put("DatabaseType", "mysql");
    sessionTemplate.put("DatabaseName", "SP_DATABASE");
    sessionTemplate.put("Dialect", "google_standard_sql");
    sessionTemplate.put("Notes", null);
    sessionTemplate.put("Tags", null);
    sessionTemplate.put("SpSchema", new LinkedHashMap<>());
    sessionTemplate.put("SyntheticPKeys", new LinkedHashMap<>());
    sessionTemplate.put("SrcSchema", new LinkedHashMap<>());
    sessionTemplate.put("SchemaIssues", new LinkedHashMap<>());
    sessionTemplate.put("Location", new LinkedHashMap<>());
    sessionTemplate.put("TimezoneOffset", "+00:00");
    sessionTemplate.put("SpDialect", "google_standard_sql");
    sessionTemplate.put("UniquePKey", new LinkedHashMap<>());
    sessionTemplate.put("Rules", new ArrayList<>());
    sessionTemplate.put("IsSharded", false);
    sessionTemplate.put("SpRegion", "");
    sessionTemplate.put("ResourceValidation", false);
    sessionTemplate.put("UI", false);

    for (int i = 1; i <= NUM_TABLES; i++) {
      String tableName = "TABLE" + i;
      List<String> colIds = Arrays.asList("c1", "c2", "c3", "c4", "c5", "c6");

      Map<String, Object> colDefs = new LinkedHashMap<>();
      for (int j = 1; j <= colIds.size(); j++) {
        Map<String, Object> colType = new LinkedHashMap<>();
        if (j == 6) {
          colType.put("Name", "LONGBLOB");
          colType.put("Len", 0);
        } else if (j % 2 == 0) {
          colType.put("Name", "STRING");
          colType.put("Len", STRING_LENGTH);
        } else {
          colType.put("Name", "NUMERIC");
          colType.put("Len", 0);
        }
        colType.put("IsArray", false);

        Map<String, Object> column = new LinkedHashMap<>();
        column.put("Name", "column_" + j);
        column.put("T", colType);
        column.put("NotNull", (j == 1));
        column.put("Comment", "From: column_" + j + " " + colType.get("Name"));
        column.put("Id", colIds.get(j - 1));

        colDefs.put(colIds.get(j - 1), column);
      }

      List<Map<String, Object>> primaryKeys = new ArrayList<>();
      Map<String, Object> primaryKey = new LinkedHashMap<>();
      primaryKey.put("ColId", "c1");
      primaryKey.put("Desc", false);
      primaryKey.put("Order", 1);
      primaryKeys.add(primaryKey);

      Map<String, Object> spSchemaEntry = new LinkedHashMap<>();
      spSchemaEntry.put("Name", tableName);
      spSchemaEntry.put("ColIds", colIds);
      spSchemaEntry.put("ShardIdColumn", "");
      spSchemaEntry.put("ColDefs", colDefs);
      spSchemaEntry.put("PrimaryKeys", primaryKeys);
      spSchemaEntry.put("ForeignKeys", null);
      spSchemaEntry.put("Indexes", null);
      spSchemaEntry.put("ParentId", "");
      spSchemaEntry.put("Comment", "Spanner schema for source table " + tableName);
      spSchemaEntry.put("Id", "t" + i);

      Map<String, Object> spSchemaMap = (Map<String, Object>) sessionTemplate.get("SpSchema");
      spSchemaMap.put("t" + i, spSchemaEntry);

      Map<String, Object> srcSchemaEntry = new LinkedHashMap<>(spSchemaEntry);
      srcSchemaEntry.put("Schema", "SRC_DATABASE");

      Map<String, Object> srcSchemaMap = (Map<String, Object>) sessionTemplate.get("SrcSchema");
      srcSchemaMap.put("t" + i, srcSchemaEntry);

      Map<String, Object> schemaIssuesEntry = new LinkedHashMap<>();
      schemaIssuesEntry.put("ColumnLevelIssues", new LinkedHashMap<>());
      schemaIssuesEntry.put("TableLevelIssues", null);

      Map<String, Object> schemaIssuesMap =
          (Map<String, Object>) sessionTemplate.get("SchemaIssues");
      schemaIssuesMap.put("t" + i, schemaIssuesEntry);
    }

    return sessionTemplate;
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

  private void createPubSubNotifications() throws IOException {
    // Instantiate pubsub resource manager for notifications
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();

    // Create pubsub notifications
    TopicName topic = pubsubResourceManager.createTopic("it");
    TopicName dlqTopic = pubsubResourceManager.createTopic("dlq");
    subscription = pubsubResourceManager.createSubscription(topic, "it-sub");
    dlqSubscription = pubsubResourceManager.createSubscription(dlqTopic, "dlq-sub");
    gcsResourceManager.createNotification(topic.toString(), gcsPrefix.substring(1));
    gcsResourceManager.createNotification(dlqTopic.toString(), dlqGcsPrefix.substring(1));
  }

  private void createSpannerTables(List<String> tableNames) {
    tableNames.forEach(
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
      List<String> tableNames, Map<String, List<Map<String, Object>>> cdcEvents) {
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

        for (String tableName : tableNames) {
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
