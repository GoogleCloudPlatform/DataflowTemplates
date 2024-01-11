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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.jdbc.CustomMySQLResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

/** Integration test for {@link DataStreamToSpanner} Flex template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerIT extends TemplateTestBase {

  // Test
  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToSpannerIT.class);

  private static final Integer NUM_EVENTS = 10;

  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String ENTRY_ADDED = "entry_added";

  private static final List<String> COLUMNS = List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED);

  private CustomMySQLResourceManager jdbcResourceManager;
  private DatastreamResourceManager datastreamResourceManager;
  private SpannerResourceManager spannerResourceManager;

  @Before
  public void setUp() throws IOException {
    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity("datastream-private-connect-us-central1")
            .build();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        jdbcResourceManager, datastreamResourceManager, spannerResourceManager);
  }

  @Test
  public void testDataStreamMySqlToSpanner() throws IOException {
    // Create MySQL Resource manager
    jdbcResourceManager = CustomMySQLResourceManager.builder(testName).build();

    // Arrange MySQL-compatible schema
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);

    // Create JDBC table
    String tableName = "MySqlToSpanner";
    jdbcResourceManager.createTable(tableName, schema);

    MySQLSource mySQLSource =
        MySQLSource.builder(
                jdbcResourceManager.getHost(),
                jdbcResourceManager.getUsername(),
                jdbcResourceManager.getPassword(),
                jdbcResourceManager.getPort())
            .build();

    spannerResourceManager =
        SpannerResourceManager.builder(tableName, PROJECT, REGION)
            .setCredentials(credentials)
            .build();

    // Create Spanner dataset
    spannerResourceManager.executeDdlStatement(
        "CREATE TABLE "
            + tableName
            + " ("
            + ROW_ID
            + " INT64 NOT NULL,"
            + NAME
            + " STRING(1024),"
            + AGE
            + " INT64,"
            + MEMBER
            + " STRING(1024),"
            + ENTRY_ADDED
            + " STRING(1024)"
            + ") PRIMARY KEY ("
            + ROW_ID
            + ")");

    // Run a simple IT
    simpleJdbcToSpannerTest(
        testName,
        tableName,
        mySQLSource,
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        b -> b.addParameter("inputFileFormat", "avro"));
  }

  @Test
  public void testDataStreamMySqlToSpannerJson() throws IOException {
    // Create MySQL Resource manager
    jdbcResourceManager = CustomMySQLResourceManager.builder(testName).build();

    // Arrange MySQL-compatible schema
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);

    // Create JDBC table
    String tableName = "MySqlToSpannerJson";
    jdbcResourceManager.createTable(tableName, schema);

    MySQLSource mySQLSource =
        MySQLSource.builder(
                jdbcResourceManager.getHost(),
                jdbcResourceManager.getUsername(),
                jdbcResourceManager.getPassword(),
                jdbcResourceManager.getPort())
            .build();

    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION)
            .setCredentials(credentials)
            .build();

    // Create Spanner dataset
    spannerResourceManager.executeDdlStatement(
        "CREATE TABLE "
            + tableName
            + " ("
            + ROW_ID
            + " INT64 NOT NULL,"
            + NAME
            + " STRING(1024),"
            + AGE
            + " INT64,"
            + MEMBER
            + " STRING(1024),"
            + ENTRY_ADDED
            + " STRING(1024)"
            + ") PRIMARY KEY ("
            + ROW_ID
            + ")");

    // Run a simple IT
    simpleJdbcToSpannerTest(
        testName,
        tableName,
        mySQLSource,
        DatastreamResourceManager.DestinationOutputFormat.JSON_FILE_FORMAT,
        b -> b.addParameter("inputFileFormat", "json"));
  }

  private void simpleJdbcToSpannerTest(
      String testName,
      String tableName,
      JDBCSource jdbcSource,
      DatastreamResourceManager.DestinationOutputFormat fileFormat,
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {

    // Create Datastream JDBC Source Connection profile and config
    SourceConfig sourceConfig =
        datastreamResourceManager.buildJDBCSourceConfig("mysql-profile", jdbcSource);

    // Create Datastream GCS Destination Connection profile and config
    String gcsPrefix = getGcsPath(testName).replace("gs://" + artifactBucketName, "") + "/cdc/";
    DestinationConfig destinationConfig =
        datastreamResourceManager.buildGCSDestinationConfig(
            "gcs-profile", artifactBucketName, gcsPrefix, fileFormat);

    // Create and start Datastream stream
    Stream stream =
        datastreamResourceManager.createStream("stream1", sourceConfig, destinationConfig);
    datastreamResourceManager.startStream(stream);

    // Construct template
    String jobName = PipelineUtils.createJobName(testName);
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(jobName, specPath)
                .addParameter("inputFilePattern", getGcsPath(testName) + "/cdc/")
                .addParameter("streamName", stream.getName())
                .addParameter("instanceId", spannerResourceManager.getInstanceId())
                .addParameter("databaseId", spannerResourceManager.getDatabaseId())
                .addParameter("projectId", PROJECT)
                .addParameter("deadLetterQueueDirectory", getGcsPath(testName) + "/dlq/"));

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // Increase timeout to reduce flakiness caused by multi-stage ConditionCheck
    PipelineOperator.Config config =
        PipelineOperator.Config.builder()
            .setJobId(info.jobId())
            .setProject(PROJECT)
            .setRegion(REGION)
            .setTimeoutAfter(Duration.ofMinutes(20))
            .build();

    AtomicReference<List<Map<String, Object>>> cdcEvents = new AtomicReference<>(new ArrayList<>());

    // Construct a ChainedConditionCheck with 4 stages.
    // 1. Send initial wave of events to JDBC
    // 2. Wait on Spanner to merge events from staging to destination
    // 3. Send wave of mutations to JDBC
    // 4. Wait on Spanner to merge second wave of events
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeJdbcData(tableName, cdcEvents),
                    SpannerRowsCheck.builder(spannerResourceManager, tableName)
                        .setMinRows(NUM_EVENTS)
                        .build(),
                    changeJdbcData(tableName, cdcEvents),
                    SpannerRowsCheck.builder(spannerResourceManager, tableName)
                        .setMinRows(1)
                        .setMaxRows(NUM_EVENTS / 2)
                        .build()))
            .build();

    // Job needs to be cancelled as draining will time out
    PipelineOperator.Result result =
        pipelineOperator().waitForConditionAndCancel(config, conditionCheck);

    // Assert
    assertThatResult(result).meetsConditions();

    SpannerAsserts.assertThatStructs(spannerResourceManager.readTableRecords(tableName, COLUMNS))
        .hasRecordsUnorderedCaseInsensitiveColumns(cdcEvents.get());
  }

  /**
   * Helper function for constructing a ConditionCheck whose check() method constructs the initial
   * rows of data in the JDBC database according to the common schema for the IT's in this class.
   *
   * @return A ConditionCheck containing the JDBC write operation.
   */
  private ConditionCheck writeJdbcData(
      String tableName, AtomicReference<List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Send initial JDBC events.";
      }

      @Override
      protected CheckResult check() {
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
        cdcEvents.set(rows);

        return new CheckResult(
            jdbcResourceManager.write(tableName, rows),
            String.format("Sent %d rows to %s.", rows.size(), tableName));
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
      String tableName, AtomicReference<List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Send initial JDBC events.";
      }

      @Override
      protected CheckResult check() {
        List<Map<String, Object>> newCdcEvents = new ArrayList<>();
        for (int i = 0; i < NUM_EVENTS; i++) {
          if (i % 2 == 0) {
            Map<String, Object> values = cdcEvents.get().get(i);
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
            jdbcResourceManager.runSQLUpdate(updateSql);
            newCdcEvents.add(values);
          } else {
            jdbcResourceManager.runSQLUpdate(
                "DELETE FROM " + tableName + " WHERE " + COLUMNS.get(0) + "=" + i);
          }
        }
        cdcEvents.set(newCdcEvents);

        return new CheckResult(
            true, String.format("Sent %d rows to %s.", newCdcEvents.size(), tableName));
      }
    };
  }
}
