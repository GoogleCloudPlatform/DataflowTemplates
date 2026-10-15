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

import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.resourcemanager.SpannerOracleResourceManager;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
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
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.OracleSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Oracle port of {@code DataStreamToSpannerIT}: end-to-end live CDC (insert/update/delete) from an
 * Oracle source through a real Datastream stream into Spanner, for Avro and JSON Datastream output
 * and for both GoogleSQL and PostgreSQL-dialect Spanner databases.
 *
 * <p>Each test provisions an isolated Oracle common user (schema) on the shared static Oracle
 * instance via {@link SharedOracleLiveITInstance}, its own Spanner database, Datastream stream,
 * Pub/Sub notifications and Dataflow job.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class OracleDataStreamToSpannerIT extends DataStreamToSpannerITBase {

  private static final Logger LOG = LoggerFactory.getLogger(OracleDataStreamToSpannerIT.class);

  private static final String ORACLE_DDL_RESOURCE =
      "oracle/OracleDataStreamToSpannerIT/oracle-schema.sql";
  private static final String SPANNER_GSQL_DDL_RESOURCE =
      "oracle/OracleDataStreamToSpannerIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";
  private static final String SPANNER_PG_DDL_RESOURCE =
      "oracle/OracleDataStreamToSpannerIT/oracle-POSTGRESQL-spanner-schema.sql";

  private static final Integer NUM_EVENTS = 10;

  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String ENTRY_ADDED = "entry_added";

  private static final List<String> COLUMNS = List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED);

  /** Mixed-case table names; must match the (quoted) identifiers in the schema resources. */
  private static final List<String> TABLE_NAMES =
      List.of("DatastreamToSpanner_1", "DatastreamToSpanner_2");

  private String gcsPrefix;
  private String dlqGcsPrefix;

  private SubscriptionName subscription;
  private SubscriptionName dlqSubscription;

  private SpannerOracleResourceManager oracleResourceManager;
  private String oracleUser;
  private DatastreamResourceManager datastreamResourceManager;
  private SpannerResourceManager spannerResourceManager;
  private PubsubResourceManager pubsubResourceManager;
  private GcsResourceManager gcsResourceManager;

  private Dialect spannerDialect;

  @Before
  public void setUp() throws IOException {
    // Dataflow jobs are cancelled explicitly in cleanUp() before the other resources are deleted.
    skipBaseCleanup = true;

    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity("datastream-connect-2")
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
  public void cleanUp() throws IOException {
    // Stop consumers (Dataflow job, Datastream stream) before dropping the source Oracle user.
    tearDownBase();
    ResourceManagerUtils.cleanResources(
        datastreamResourceManager,
        spannerResourceManager,
        pubsubResourceManager,
        gcsResourceManager);
    SharedOracleLiveITInstance.dropUser(oracleUser);
  }

  @Test
  public void testDataStreamOracleToSpanner() throws Exception {
    simpleOracleToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        Dialect.GOOGLE_STANDARD_SQL,
        Function.identity());
  }

  @Test
  public void testDataStreamOracleToPostgresSpanner() throws Exception {
    simpleOracleToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        Dialect.POSTGRESQL,
        Function.identity());
  }

  @Test
  public void testDataStreamOracleToSpannerStreamingEngine() throws Exception {
    simpleOracleToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        Dialect.GOOGLE_STANDARD_SQL,
        config -> config.addEnvironment("enableStreamingEngine", true));
  }

  @Test
  public void testDataStreamOracleToSpannerJson() throws Exception {
    simpleOracleToSpannerTest(
        DatastreamResourceManager.DestinationOutputFormat.JSON_FILE_FORMAT,
        Dialect.GOOGLE_STANDARD_SQL,
        Function.identity());
  }

  private void simpleOracleToSpannerTest(
      DatastreamResourceManager.DestinationOutputFormat fileFormat,
      Dialect spannerDialect,
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws Exception {
    this.spannerDialect = spannerDialect;

    // Create Oracle resources: shared PDB admin + isolated per-test schema (common user).
    oracleResourceManager = SharedOracleLiveITInstance.getInstance();
    oracleUser = SharedOracleLiveITInstance.setupOracleIsolatedUser();

    // Create Spanner Resource Manager
    spannerResourceManager =
        Dialect.POSTGRESQL.equals(spannerDialect)
            ? setUpPGDialectSpannerResourceManager()
            : setUpSpannerResourceManager();

    // Create Oracle tables
    executeOracleSqlFileScript(oracleResourceManager, ORACLE_DDL_RESOURCE, oracleUser);
    SharedOracleLiveITInstance.flushRedoLogs();

    OracleSource oracleSource =
        OracleSource.builder(
                oracleResourceManager.getHost(),
                oracleUser,
                SharedOracleLiveITInstance.ORACLE_PASSWORD,
                oracleResourceManager.getPort(),
                oracleResourceManager.getDatabaseName())
            .setAllowedTables(Map.of(oracleUser.toUpperCase(), TABLE_NAMES))
            .build();

    // Create Spanner tables
    createSpannerDDL(
        spannerResourceManager,
        Dialect.POSTGRESQL.equals(spannerDialect)
            ? SPANNER_PG_DDL_RESOURCE
            : SPANNER_GSQL_DDL_RESOURCE);

    // Create and start Datastream stream (Oracle source -> GCS destination)
    Stream stream =
        createDataStream(
            datastreamResourceManager, gcsResourceManager, gcsPrefix, oracleSource, fileFormat);

    // Construct template
    createPubSubNotifications();
    String jobName = PipelineUtils.createJobName(testName);
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder
            .apply(
                PipelineLauncher.LaunchConfig.builder(jobName, specPath)
                    .addParameter("gcsPubSubSubscription", subscription.toString())
                    .addParameter("dlqGcsPubSubSubscription", dlqSubscription.toString())
                    .addParameter("streamName", stream.getName())
                    .addParameter("instanceId", spannerResourceManager.getInstanceId())
                    .addParameter("databaseId", spannerResourceManager.getDatabaseId())
                    .addParameter("projectId", PROJECT)
                    .addParameter(
                        "deadLetterQueueDirectory",
                        getGcsPath(testName, gcsResourceManager) + "/dlq/")
                    .addParameter("spannerHost", spannerResourceManager.getSpannerHost())
                    // Streaming right fitting requires horizontal autoscaling to be enabled.
                    .addParameter("autoscalingAlgorithm", "THROUGHPUT_BASED")
                    .addParameter(
                        "inputFileFormat",
                        fileFormat.equals(
                                DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT)
                            ? "avro"
                            : "json"))
            .addEnvironment("additionalPipelineOptions", List.of("resourceHints=cpu_count=4"))
            .addEnvironment(
                "additionalExperiments", List.of("use_runner_v2", "enable_streaming_rightfitting"));

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // Construct a ChainedConditionCheck with 4 stages.
    // 1. Send initial wave of events to Oracle
    // 2. Wait on Spanner to merge events from staging to destination
    // 3. Send wave of mutations to Oracle
    // 4. Wait on Spanner to merge second wave of events
    Map<String, List<Map<String, Object>>> cdcEvents = new HashMap<>();
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeOracleData(cdcEvents),
                    SpannerRowsCheck.builder(
                            spannerResourceManager, spannerSqlTableName(TABLE_NAMES.get(0)))
                        .setMinRows(NUM_EVENTS)
                        .build(),
                    SpannerRowsCheck.builder(
                            spannerResourceManager, spannerSqlTableName(TABLE_NAMES.get(1)))
                        .setMinRows(NUM_EVENTS)
                        .build(),
                    changeOracleData(cdcEvents),
                    checkDestinationRows(cdcEvents)))
            .build();

    // Job needs to be cancelled as draining will time out
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(createConfig(info, Duration.ofMinutes(20)), conditionCheck);

    // Assert
    checkSpannerTables(cdcEvents);
    assertThatResult(result).meetsConditions();
  }

  private void createPubSubNotifications() throws IOException {
    // Instantiate pubsub resource manager for notifications
    pubsubResourceManager = setUpPubSubResourceManager();

    // Create pubsub notifications
    TopicName topic = pubsubResourceManager.createTopic("it");
    TopicName dlqTopic = pubsubResourceManager.createTopic("dlq");
    subscription = pubsubResourceManager.createSubscription(topic, "it-sub");
    dlqSubscription = pubsubResourceManager.createSubscription(dlqTopic, "dlq-sub");
    gcsResourceManager.createNotification(topic.toString(), gcsPrefix.substring(1));
    gcsResourceManager.createNotification(dlqTopic.toString(), dlqGcsPrefix.substring(1));
  }

  /**
   * Returns the table name to use inside Spanner SQL queries. The schema preserves the mixed-case
   * table names of the Oracle source, so they must be double-quoted for PostgreSQL-dialect Spanner,
   * which folds unquoted identifiers to lower case. GoogleSQL identifiers are case-insensitive.
   */
  private String spannerSqlTableName(String tableName) {
    return Dialect.POSTGRESQL.equals(spannerDialect) ? "\"" + tableName + "\"" : tableName;
  }

  /**
   * Returns the expected Spanner representation of an Oracle {@code NUMBER} {@code age} value.
   *
   * <p>Per the Oracle data type mapping matrix, a non-key {@code NUMBER} maps to {@code NUMERIC} in
   * GoogleSQL but to {@code double precision} in PostgreSQL-dialect Spanner, whose string form has
   * a trailing {@code .0}. This only adapts the expected formatting of the same value; the value
   * written to Oracle is the same in both cases.
   */
  private Object ageValue(int age) {
    return Dialect.POSTGRESQL.equals(spannerDialect) ? (Object) (double) age : (Object) age;
  }

  /** Quotes an Oracle identifier to preserve its exact case. */
  private static String q(String identifier) {
    return "\"" + identifier + "\"";
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
      protected String getDescription() {
        return "Check Spanner rows.";
      }

      @Override
      protected CheckResult check() {
        // First, check that correct number of rows were deleted.
        for (String tableName : TABLE_NAMES) {
          long totalRows = spannerResourceManager.getRowCount(spannerSqlTableName(tableName));
          long maxRows = cdcEvents.get(tableName).size();
          if (totalRows > maxRows) {
            return new CheckResult(
                false, String.format("Expected up to %d rows but found %d", maxRows, totalRows));
          }
        }

        // Next, make sure in-place mutations were applied.
        try {
          checkSpannerTables(cdcEvents);
          return new CheckResult(true, "Spanner tables contain expected rows.");
        } catch (AssertionError error) {
          return new CheckResult(false, "Spanner tables do not contain expected rows.");
        }
      }
    };
  }

  /** Helper function for checking the rows of the destination Spanner tables. */
  private void checkSpannerTables(Map<String, List<Map<String, Object>>> cdcEvents) {
    TABLE_NAMES.forEach(
        tableName ->
            SpannerAsserts.assertThatStructs(
                    spannerResourceManager.readTableRecords(tableName, COLUMNS))
                .hasRecordsUnorderedCaseInsensitiveColumns(cdcEvents.get(tableName)));
  }

  /**
   * Helper function for constructing a ConditionCheck whose check() method constructs the initial
   * rows of data in the Oracle database according to the common schema for the IT's in this class.
   *
   * @return A ConditionCheck containing the Oracle write operation.
   */
  private ConditionCheck writeOracleData(Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Send initial Oracle events.";
      }

      @Override
      protected CheckResult check() {
        List<String> messages = new ArrayList<>();
        try {
          for (String tableName : TABLE_NAMES) {
            List<Map<String, Object>> rows = new ArrayList<>();
            List<String> inserts = new ArrayList<>();
            for (int i = 0; i < NUM_EVENTS; i++) {
              Map<String, Object> values = new HashMap<>();
              values.put(ROW_ID, i);
              values.put(NAME, RandomStringUtils.randomAlphabetic(10));
              values.put(AGE, ageValue(new Random().nextInt(100)));
              values.put(MEMBER, new Random().nextInt() % 2 == 0 ? "Y" : "N");
              values.put(ENTRY_ADDED, Instant.now().toString());
              rows.add(values);
              inserts.add(
                  String.format(
                      "INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (%s, '%s', %s, '%s', '%s')",
                      q(tableName),
                      q(ROW_ID),
                      q(NAME),
                      q(AGE),
                      q(MEMBER),
                      q(ENTRY_ADDED),
                      values.get(ROW_ID),
                      values.get(NAME),
                      values.get(AGE),
                      values.get(MEMBER),
                      values.get(ENTRY_ADDED)));
            }
            executeOracleSql(oracleResourceManager, String.join(";", inserts), oracleUser);
            cdcEvents.put(tableName, rows);
            messages.add(String.format("%d rows to %s", rows.size(), tableName));
          }
        } catch (Exception e) {
          LOG.error("Failed to write initial rows to Oracle", e);
          return new CheckResult(false, "Failed to write to Oracle: " + e.getMessage());
        }

        // Force log file archive - needed so Datastream can see changes which are read from
        // archived log files.
        SharedOracleLiveITInstance.flushRedoLogs();
        return new CheckResult(true, "Sent " + String.join(", ", messages) + ".");
      }
    };
  }

  /**
   * Helper function for constructing a ConditionCheck whose check() method changes rows of data in
   * the Oracle database according to the common schema for the IT's in this class. Half the rows
   * are mutated and half are removed completely.
   *
   * @return A ConditionCheck containing the Oracle mutate operation.
   */
  private ConditionCheck changeOracleData(Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Send Oracle changes.";
      }

      @Override
      protected CheckResult check() {
        List<String> messages = new ArrayList<>();
        try {
          for (String tableName : TABLE_NAMES) {
            List<Map<String, Object>> newCdcEvents = new ArrayList<>();
            List<String> statements = new ArrayList<>();
            for (int i = 0; i < NUM_EVENTS; i++) {
              if (i % 2 == 0) {
                Map<String, Object> values = cdcEvents.get(tableName).get(i);
                values.put(NAME, values.get(NAME).toString().toUpperCase());
                values.put(AGE, ageValue(new Random().nextInt(100)));
                values.put(
                    MEMBER, (Objects.equals(values.get(MEMBER).toString(), "Y") ? "N" : "Y"));
                statements.add(
                    String.format(
                        "UPDATE %s SET %s = '%s', %s = %s, %s = '%s' WHERE %s = %d",
                        q(tableName),
                        q(NAME),
                        values.get(NAME),
                        q(AGE),
                        values.get(AGE),
                        q(MEMBER),
                        values.get(MEMBER),
                        q(ROW_ID),
                        i));
                newCdcEvents.add(values);
              } else {
                statements.add(
                    String.format("DELETE FROM %s WHERE %s = %d", q(tableName), q(ROW_ID), i));
              }
            }
            executeOracleSql(oracleResourceManager, String.join(";", statements), oracleUser);
            cdcEvents.put(tableName, newCdcEvents);
            messages.add(String.format("%d changes to %s", newCdcEvents.size(), tableName));
          }
        } catch (Exception e) {
          LOG.error("Failed to apply changes to Oracle", e);
          return new CheckResult(false, "Failed to change Oracle data: " + e.getMessage());
        }

        // Force log file archive - needed so Datastream can see changes which are read from
        // archived log files.
        SharedOracleLiveITInstance.flushRedoLogs();
        return new CheckResult(true, "Sent " + String.join(", ", messages) + ".");
      }
    };
  }
}
