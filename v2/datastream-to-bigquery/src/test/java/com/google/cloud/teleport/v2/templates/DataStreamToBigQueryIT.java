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

import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineOperator;
import com.google.cloud.teleport.it.common.utils.PipelineUtils;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.conditions.ChainedConditionCheck;
import com.google.cloud.teleport.it.conditions.ConditionCheck;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import com.google.cloud.teleport.it.gcp.bigquery.matchers.BigQueryAsserts;
import com.google.cloud.teleport.it.gcp.datastream.DatastreamResourceManager;
import com.google.cloud.teleport.it.gcp.datastream.JDBCSource;
import com.google.cloud.teleport.it.gcp.datastream.MySQLSource;
import com.google.cloud.teleport.it.jdbc.AbstractJDBCResourceManager;
import com.google.cloud.teleport.it.jdbc.JDBCResourceManager;
import com.google.cloud.teleport.it.jdbc.MySQLResourceManager;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

/** Integration test for {@link DataStreamToBigQuery} Flex template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToBigQuery.class)
@RunWith(JUnit4.class)
public class DataStreamToBigQueryIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToBigQueryIT.class);

  private static final Integer NUM_EVENTS = 10;

  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String ENTRY_ADDED = "entry_added";

  private static final List<String> COLUMNS = List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED);

  private AbstractJDBCResourceManager<?> jdbcResourceManager;
  private DatastreamResourceManager datastreamResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setUp() throws IOException {
    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity("datastream-private-connect-us-central1")
            .build();
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT).setCredentials(credentials).build();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        jdbcResourceManager, datastreamResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testDataStreamMySqlToBQ() throws IOException {
    // Create MySQL Resource manager
    jdbcResourceManager = MySQLResourceManager.builder(testName).build();

    // Arrange MySQL-compatible schema
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);

    MySQLSource mySQLSource =
        MySQLSource.builder(
                jdbcResourceManager.getHost(),
                jdbcResourceManager.getUsername(),
                jdbcResourceManager.getPassword(),
                jdbcResourceManager.getPort())
            .build();

    // Run a simple IT
    simpleJdbcToBigQueryTest(
        testName,
        schema,
        jdbcResourceManager,
        mySQLSource,
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        b -> b.addParameter("gcsPubSubSubscription", "").addParameter("inputFileFormat", "avro"));
  }

  @Test
  public void testDataStreamMySqlToBQJson() throws IOException {
    // Create MySQL Resource manager
    jdbcResourceManager = MySQLResourceManager.builder(testName).build();

    // Arrange MySQL-compatible schema
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);

    MySQLSource mySQLSource =
        MySQLSource.builder(
                jdbcResourceManager.getHost(),
                jdbcResourceManager.getUsername(),
                jdbcResourceManager.getPassword(),
                jdbcResourceManager.getPort())
            .build();

    // Run a simple IT
    simpleJdbcToBigQueryTest(
        testName,
        schema,
        jdbcResourceManager,
        mySQLSource,
        DatastreamResourceManager.DestinationOutputFormat.JSON_FILE_FORMAT,
        b -> b.addParameter("gcsPubSubSubscription", "").addParameter("inputFileFormat", "json"));
  }

  private void simpleJdbcToBigQueryTest(
      String tableName,
      JDBCResourceManager.JDBCSchema schema,
      AbstractJDBCResourceManager<?> jdbcResourceManager,
      JDBCSource jdbcSource,
      DatastreamResourceManager.DestinationOutputFormat fileFormat,
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {

    // Create JDBC table
    jdbcResourceManager.createTable(tableName, schema);

    // Create BigQuery dataset
    bigQueryResourceManager.createDataset(REGION);

    // Create Datastream JDBC Source Connection profile and config
    SourceConfig sourceConfig =
        datastreamResourceManager.buildJDBCSourceConfig("mysql-profile", jdbcSource);

    // Create Datastream GCS Destination Connection profile and config
    String gcsPrefix = getGcsPath(tableName).replace("gs://" + artifactBucketName, "") + "/cdc/";
    DestinationConfig destinationConfig =
        datastreamResourceManager.buildGCSDestinationConfig(
            "gcs-profile", artifactBucketName, gcsPrefix, fileFormat);

    // Create and start Datastream stream
    Stream stream =
        datastreamResourceManager.createStream("stream1", sourceConfig, destinationConfig);
    datastreamResourceManager.startStream(stream);

    // Construct template
    String jobName = PipelineUtils.createJobName(tableName);
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(jobName, specPath)
                .addParameter("inputFilePattern", getGcsPath(tableName) + "/cdc/")
                .addParameter(
                    "outputStagingDatasetTemplate", bigQueryResourceManager.getDatasetId())
                .addParameter("outputDatasetTemplate", bigQueryResourceManager.getDatasetId())
                .addParameter("streamName", stream.getName())
                .addParameter("deadLetterQueueDirectory", getGcsPath(tableName) + "/dlq/")
                .addParameter("mergeFrequencyMinutes", "1")
                .addParameter("dlqRetryMinutes", "1")
                .addParameter("defaultWorkerLogLevel", "DEBUG"));

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
    TableId bqTableId = TableId.of(PROJECT, bigQueryResourceManager.getDatasetId(), tableName);

    // Construct a ChainedConditionCheck with 4 stages.
    // 1. Send initial wave of events to JDBC
    // 2. Wait on BQ to merge events from staging to destination
    // 3. Send wave of mutations to JDBC
    // 4. Wait on BQ to merge second wave of events
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeJdbcData(tableName, cdcEvents),
                    BigQueryRowsCheck.builder(bigQueryResourceManager, bqTableId)
                        .setMinRows(NUM_EVENTS)
                        .build(),
                    changeJdbcData(tableName, cdcEvents),
                    BigQueryRowsCheck.builder(bigQueryResourceManager, bqTableId)
                        .setMinRows(1)
                        .setMaxRows(NUM_EVENTS / 2)
                        .build()))
            .build();

    // Job needs to be cancelled as draining will time out
    PipelineOperator.Result result =
        pipelineOperator().waitForConditionAndCancel(config, conditionCheck);

    // Assert
    assertThatResult(result).meetsConditions();

    String bqColumns = String.join(",", COLUMNS);
    BigQueryAsserts.assertThatBigQueryRecords(
            bigQueryResourceManager.runQuery(
                "SELECT TO_JSON_STRING(t) FROM (SELECT "
                    + bqColumns
                    + " FROM `"
                    + toTableSpecStandard(bqTableId)
                    + "`) as t"))
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
    class WriteJDBCDataConditionCheck extends ConditionCheck {
      @Override
      protected String getDescription() {
        return "Send initial JDBC events.";
      }

      @Override
      protected CheckResult check() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < DataStreamToBigQueryIT.NUM_EVENTS; i++) {
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
    }
    ;

    return new WriteJDBCDataConditionCheck();
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
    class ChangeJDBCDataConditionCheck extends ConditionCheck {
      @Override
      protected String getDescription() {
        return "Send initial JDBC events.";
      }

      @Override
      protected CheckResult check() {
        List<Map<String, Object>> newCdcEvents = new ArrayList<>();
        for (int i = 0; i < DataStreamToBigQueryIT.NUM_EVENTS; i++) {
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
    }
    ;

    return new ChangeJDBCDataConditionCheck();
  }
}
