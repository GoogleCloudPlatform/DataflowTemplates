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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;
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
public class DataStreamToSpannerWideRowFor1600MBColumnsPerTablesIT extends SpannerTemplateITBase {
  private static final String CHARACTERS =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  private static final Integer NUM_EVENTS = 1;
  private static final Integer NUM_TABLES = 1;
  private static final Integer NUM_COLUMNS = 801;
  private static final int STRING_LENGTH = 2_621_440;

  private static final Random RANDOM_GENERATOR = new Random();

  private String gcsPrefix;
  private String dlqGcsPrefix;

  private SubscriptionName subscription;
  private SubscriptionName dlqSubscription;

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
  public void testDataStreamMySqlToSpannerFor1600MBColumnsPerTables() throws IOException {
    simpleMaxMySqlColumnsPerTablesToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        Dialect.GOOGLE_STANDARD_SQL,
        Function.identity());
  }

  @Test
  public void testDataStreamMySqlToSpannerStreamingEngine() throws IOException {
    simpleMaxMySqlColumnsPerTablesToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        Dialect.GOOGLE_STANDARD_SQL,
        config -> config.addEnvironment("enableStreamingEngine", true));
  }

  @Test
  public void testDataStreamMySqlToSpannerJson() throws IOException {
    simpleMaxMySqlColumnsPerTablesToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.JSON_FILE_FORMAT,
        Dialect.GOOGLE_STANDARD_SQL,
        Function.identity());
  }

  private void simpleMaxMySqlColumnsPerTablesToSpannerTest(
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

  public static String generateRandomString() {
    return RANDOM_GENERATOR
        .ints(STRING_LENGTH, 0, CHARACTERS.length())
        .mapToObj(CHARACTERS::charAt)
        .map(Object::toString)
        .collect(Collectors.joining());
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

    // Generate 5000 table names
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
    tableNames.forEach(tableName -> cloudSqlResourceManager.runSQLUpdate(getJDBCSchema(tableName)));

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
    Map<String, List<Map<String, Object>>> cdcEvents = new HashMap<>();
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
    sessionTemplate.put("TimezoneOffset", "+00:00");
    sessionTemplate.put("SpDialect", "google_standard_sql");
    sessionTemplate.put("IsSharded", false);
    sessionTemplate.put("SpRegion", "");
    sessionTemplate.put("ResourceValidation", false);
    sessionTemplate.put("UI", false);

    // Initialize schema maps
    sessionTemplate.put("SpSchema", new LinkedHashMap<>());
    sessionTemplate.put("SyntheticPKeys", new LinkedHashMap<>());
    sessionTemplate.put("SrcSchema", new LinkedHashMap<>());
    sessionTemplate.put("SchemaIssues", new LinkedHashMap<>());
    sessionTemplate.put("Location", new LinkedHashMap<>());
    sessionTemplate.put("UniquePKey", new LinkedHashMap<>());
    sessionTemplate.put("Rules", new ArrayList<>());

    for (int i = 1; i <= NUM_TABLES; i++) {
      String tableName = "TABLE" + i;

      // Generate column IDs
      List<String> colIds = new ArrayList<>();
      for (int ci = 1; ci <= NUM_COLUMNS; ci++) {
        colIds.add("c" + ci);
      }

      // Create column definitions
      Map<String, Object> colDefs = createColumnDefinitions(colIds);

      // Create primary keys
      List<Map<String, Object>> primaryKeys = createPrimaryKeys(colIds);

      // Create schema entries for Spanner & Source
      Map<String, Object> spSchemaEntry =
          createSchemaEntry(
              tableName,
              colIds,
              colDefs,
              primaryKeys,
              "t" + i,
              "Spanner schema for source table " + tableName);
      ((Map<String, Object>) sessionTemplate.get("SpSchema")).put("t" + i, spSchemaEntry);

      Map<String, Object> srcSchemaEntry = new LinkedHashMap<>(spSchemaEntry);
      srcSchemaEntry.put("Schema", "SRC_DATABASE");
      ((Map<String, Object>) sessionTemplate.get("SrcSchema")).put("t" + i, srcSchemaEntry);

      // Initialize Schema Issues
      Map<String, Object> schemaIssuesEntry = new LinkedHashMap<>();
      schemaIssuesEntry.put("ColumnLevelIssues", new LinkedHashMap<>());
      schemaIssuesEntry.put("TableLevelIssues", null);
      ((Map<String, Object>) sessionTemplate.get("SchemaIssues")).put("t" + i, schemaIssuesEntry);
    }

    return sessionTemplate;
  }

  /** Creates column definitions based on column IDs. */
  private static Map<String, Object> createColumnDefinitions(List<String> colIds) {
    Map<String, Object> colDefs = new LinkedHashMap<>();

    for (int j = 1; j <= colIds.size(); j++) {
      Map<String, Object> colType = new LinkedHashMap<>();
      colType.put("Name", (j == 1) ? "NUMERIC" : "MEDIUMTEXT");
      colType.put("Len", 0);
      colType.put("IsArray", false);

      Map<String, Object> column = new LinkedHashMap<>();
      column.put("Name", "col_" + j);
      column.put("T", colType);
      column.put("NotNull", (j == 1)); // First column is NOT NULL
      column.put("Comment", "From: col_" + j + ((j == 1) ? " decimal(10)" : " MEDIUMTEXT"));
      column.put("Id", colIds.get(j - 1));

      colDefs.put(colIds.get(j - 1), column);
    }

    return colDefs;
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

  /** Creates a schema entry for a table. */
  private static Map<String, Object> createSchemaEntry(
      String tableName,
      List<String> colIds,
      Map<String, Object> colDefs,
      List<Map<String, Object>> primaryKeys,
      String tableId,
      String comment) {
    Map<String, Object> schemaEntry = new LinkedHashMap<>();
    schemaEntry.put("Name", tableName);
    schemaEntry.put("ColIds", colIds);
    schemaEntry.put("ShardIdColumn", "");
    schemaEntry.put("ColDefs", colDefs);
    schemaEntry.put("PrimaryKeys", primaryKeys);
    schemaEntry.put("ForeignKeys", null);
    schemaEntry.put("Indexes", null);
    schemaEntry.put("ParentId", "");
    schemaEntry.put("Comment", comment);
    schemaEntry.put("Id", tableId);

    return schemaEntry;
  }

  private String getJDBCSchema(String tableName) {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
    for (int i = 1; i <= NUM_COLUMNS; i++) {
      if (i == 1) {
        sb.append("col_").append(i).append(" NUMERIC NOT NULL");
      } else {
        sb.append("col_").append(i).append(" MEDIUMTEXT NOT NULL");
      }
      if (i != NUM_COLUMNS) {
        sb.append(", ");
      }
    }
    sb.append(", PRIMARY KEY (").append("col_1").append("))");
    return sb.toString();
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
    for (String tableName : tableNames) {
      List<String> columns = new ArrayList<>();
      columns.add("col_1 INT64 NOT NULL");

      for (int i = 2; i <= NUM_COLUMNS; i++) {
        columns.add("col_" + i + " STRING(MAX)");
      }

      String ddlStatement =
          String.format(
              "CREATE TABLE %s (%s) PRIMARY KEY (col_1)", tableName, String.join(", ", columns));

      spannerResourceManager.executeDdlStatement(ddlStatement);
    }
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
        tableName -> {
          List<String> columns = new ArrayList<>();
          for (int i = 1; i <= NUM_COLUMNS; i++) {
            columns.add("col_" + i);
          }
          SpannerAsserts.assertThatStructs(
                  spannerResourceManager.readTableRecords(tableName, columns))
              .hasRecordsUnorderedCaseInsensitiveColumns(cdcEvents.get(tableName));
        });
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
        for (String tableName : tableNames) {

          List<Map<String, Object>> rows = new ArrayList<>();
          for (int i = 0; i < NUM_EVENTS; i++) {
            Map<String, Object> values = new HashMap<>();
            for (int ci = 1; ci <= NUM_COLUMNS; ci++) {
              if (ci == 1) {
                values.put("col_" + ci, ci);
              } else {
                values.put("col_" + ci, generateRandomString());
              }
            }
            rows.add(values);
          }
          cdcEvents.put(tableName, rows);
          success &= cloudSqlResourceManager.write(tableName, rows);
          messages.add(String.format("%d rows to %s", rows.size(), tableName));
        }
        return new CheckResult(success, "Sent " + String.join(", ", messages) + ".");
      }
    };
  }
}
