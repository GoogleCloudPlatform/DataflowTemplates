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
package com.google.cloud.dataflow.cdc.applier;

import static com.google.cloud.teleport.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.PipelineOperator;
import com.google.cloud.teleport.it.common.utils.IORedirectUtil;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.JDBCBaseIT;
import com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import com.google.cloud.teleport.it.gcp.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.it.jdbc.JDBCResourceManager;
import com.google.cloud.teleport.it.jdbc.MySQLResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

/** Integration test for {@link CdcToBigQueryChangeApplierPipeline} flex template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(CdcToBigQueryChangeApplierPipeline.class)
@RunWith(JUnit4.class)
public class CdcToBigQueryChangeApplierPipelineIT extends JDBCBaseIT {

  private static final Logger LOG =
      LoggerFactory.getLogger(CdcToBigQueryChangeApplierPipelineIT.class);
  private MySQLResourceManager mySQLResourceManager;
  private PubsubResourceManager pubsubResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;
  private Process exec;

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final int NUM_RECORDS = 10;
  private static final String PEOPLE = "people";
  private static final String PEOPLE_CHANGELOG = "people_changelog";
  private static final String PETS = "pets";
  private static final String PETS_CHANGELOG = "pets_changelog";
  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String AGE = "age";
  private static final String OWNER = "owner";

  private static final Pattern JDBC_PORT_PATTERN =
      Pattern.compile("^jdbc:mysql://(.*?):([0-9]+).*$");

  @Before
  public void setUp() throws IOException {
    mySQLResourceManager = MySQLResourceManager.builder(testName).build();
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT).setCredentials(credentials).build();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        mySQLResourceManager, pubsubResourceManager, bigQueryResourceManager);
    exec.destroy();
  }

  @Test
  public void testDebeziumCdcToBigQueryMultiTopic() throws Exception {
    testDebeziumCdcToBigQuery(false);
  }

  @Test
  public void testDebeziumCdcToBigQuerySingleTopic() throws Exception {
    testDebeziumCdcToBigQuery(true);
  }

  private void testDebeziumCdcToBigQuery(boolean singleTopicMode) throws Exception {
    // Arrange
    HashMap<String, String> peopleColumns = new HashMap<>();
    peopleColumns.put(ROW_ID, "NUMERIC NOT NULL");
    peopleColumns.put(NAME, "VARCHAR(200)");
    peopleColumns.put(AGE, "NUMERIC");
    JDBCResourceManager.JDBCSchema peopleSchema =
        new JDBCResourceManager.JDBCSchema(peopleColumns, ROW_ID);
    mySQLResourceManager.createTable(PEOPLE, peopleSchema);

    List<Map<String, Object>> peopleData = getPeopleJdbcData();
    mySQLResourceManager.write(PEOPLE, peopleData);

    HashMap<String, String> petsColumns = new HashMap<>();
    petsColumns.put(NAME, "VARCHAR(200) NOT NULL");
    petsColumns.put(AGE, "NUMERIC");
    petsColumns.put(OWNER, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema petsSchema =
        new JDBCResourceManager.JDBCSchema(petsColumns, NAME);
    mySQLResourceManager.createTable(PETS, petsSchema);

    List<Map<String, Object>> petsData = getPetsJdbcData();
    mySQLResourceManager.write(PETS, petsData);

    // Debezium authenticates with MySQL using the sha2_password authentication (pre MySQL 8.0)
    // algorithm, or else it won't connect
    mySQLResourceManager.runSQLUpdate(
        "ALTER USER 'root'@'%' IDENTIFIED WITH mysql_native_password BY '"
            + mySQLResourceManager.getPassword()
            + "';");

    String subscriptionsString;
    String formattedTimestamp =
        DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")
            .withZone(ZoneId.of("UTC"))
            .format(Instant.now());
    if (singleTopicMode) {
      String singleTopicName =
          String.format("%s_%s", "debezium_single_topic_test", formattedTimestamp);
      TopicName singleTopic = pubsubResourceManager.createTopicWithoutPrefix(singleTopicName);
      SubscriptionName singleSubscription =
          pubsubResourceManager.createSubscription(singleTopic, "single_topic_subscription");

      subscriptionsString = singleSubscription.getSubscription();
    } else {
      String peopleTopicName =
          String.format("%s.%s.%s", testName, mySQLResourceManager.getDatabaseName(), PEOPLE);
      TopicName peopleTopic = pubsubResourceManager.createTopic(peopleTopicName);
      SubscriptionName peopleSubscription =
          pubsubResourceManager.createSubscription(peopleTopic, "people_subscription");

      String petsTopicName =
          String.format("%s.%s.%s", testName, mySQLResourceManager.getDatabaseName(), PETS);
      TopicName petsTopic = pubsubResourceManager.createTopic(petsTopicName);
      SubscriptionName petsSubscription =
          pubsubResourceManager.createSubscription(petsTopic, "pets_subscription");

      subscriptionsString =
          peopleSubscription.getSubscription() + "," + petsSubscription.getSubscription();
    }

    bigQueryResourceManager.createDataset(REGION);

    Path path = tmpFolder.newFile("property_file_" + singleTopicMode + ".properties").toPath();
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("databaseName=" + testName + System.lineSeparator());
    stringBuilder.append(
        "databaseUsername=" + mySQLResourceManager.getUsername() + System.lineSeparator());
    stringBuilder.append(
        "databasePassword=" + mySQLResourceManager.getPassword() + System.lineSeparator());
    stringBuilder.append(
        "databaseAddress=" + mySQLResourceManager.getHost() + System.lineSeparator());
    stringBuilder.append(
        "databasePort=" + parseJdbcPort(mySQLResourceManager.getUri()) + System.lineSeparator());
    stringBuilder.append("gcpProject=" + PROJECT + System.lineSeparator());
    stringBuilder.append(
        "gcpPubsubTopicPrefix="
            + (singleTopicMode
                ? String.format("%s_%s", "debezium_single_topic_test", formattedTimestamp)
                : pubsubResourceManager.getTestId() + "-")
            + System.lineSeparator());
    stringBuilder.append("singleTopicMode=" + singleTopicMode + System.lineSeparator());
    stringBuilder.append("inMemoryOffsetStorage=true" + System.lineSeparator());
    stringBuilder.append("debezium.offset.flush.interval.ms=300" + System.lineSeparator());
    String whitelistedTablesValue =
        String.format(
            "%s.%s.%s,%s.%s.%s",
            testName,
            mySQLResourceManager.getDatabaseName(),
            PEOPLE,
            testName,
            mySQLResourceManager.getDatabaseName(),
            PETS);
    stringBuilder.append("whitelistedTables=" + whitelistedTablesValue + System.lineSeparator());
    Files.write(path, stringBuilder.toString().getBytes());

    // Act

    // Start up Debezium connector
    String mavenCmd = "mvn exec:java -pl cdc-embedded-connector -Dexec.args=\"" + path + "\"";
    try {
      exec = Runtime.getRuntime().exec(mavenCmd, null, new File("../"));
      IORedirectUtil.redirectLinesLog(exec.getInputStream(), LOG);
      IORedirectUtil.redirectLinesLog(exec.getErrorStream(), LOG);

      // If the timeout expires, it means that the connector is still running. This is good, as it
      // has not hit any errors.
      if (exec.waitFor(60, TimeUnit.SECONDS)) {
        throw new RuntimeException("Error running Debezium connector, check Maven logs.");
      }

      LOG.info("Debezium connector successfully started!");

    } catch (Exception e) {
      throw new IllegalArgumentException("Error running Debezium connector", e);
    }

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("inputSubscriptions", subscriptionsString)
            .addParameter("changeLogDataset", bigQueryResourceManager.getDatasetId())
            .addParameter("replicaDataset", bigQueryResourceManager.getDatasetId())
            .addParameter("useSingleTopic", Boolean.toString(singleTopicMode))
            .addParameter("updateFrequencySecs", "100");

    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // Add more data to verify additional changes are being captured
    List<Map<String, Object>> additionalPetsData = getPetsJdbcData();
    mySQLResourceManager.write(PETS, additionalPetsData);
    petsData.addAll(additionalPetsData);

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(info),
                BigQueryRowsCheck.builder(
                        bigQueryResourceManager,
                        TableId.of(bigQueryResourceManager.getDatasetId(), PEOPLE))
                    .setMinRows(NUM_RECORDS)
                    .build(),
                BigQueryRowsCheck.builder(
                        bigQueryResourceManager,
                        TableId.of(bigQueryResourceManager.getDatasetId(), PETS))
                    .setMinRows(NUM_RECORDS * 2)
                    .build(),
                BigQueryRowsCheck.builder(
                        bigQueryResourceManager,
                        TableId.of(bigQueryResourceManager.getDatasetId(), PEOPLE_CHANGELOG))
                    .setMinRows(NUM_RECORDS)
                    .build(),
                BigQueryRowsCheck.builder(
                        bigQueryResourceManager,
                        TableId.of(bigQueryResourceManager.getDatasetId(), PETS_CHANGELOG))
                    .setMinRows(NUM_RECORDS * 2)
                    .build());

    // Assert
    assertThatResult(result).meetsConditions();

    assertThatBigQueryRecords(bigQueryResourceManager.readTable(PEOPLE))
        .hasRecordsUnorderedCaseInsensitiveColumns(peopleData);
    assertThatBigQueryRecords(bigQueryResourceManager.readTable(PETS))
        .hasRecordsUnorderedCaseInsensitiveColumns(petsData);

    List<String> peopleChangelogData = new ArrayList<>();
    for (Map<String, Object> map : peopleData) {
      peopleChangelogData.addAll(
          map.values().stream().map(value -> value.toString()).collect(Collectors.toList()));
    }

    List<String> petsChangelogData = new ArrayList<>();
    for (Map<String, Object> map : petsData) {
      petsChangelogData.addAll(
          map.values().stream().map(value -> value.toString()).collect(Collectors.toList()));
    }

    assertEquals(bigQueryResourceManager.getRowCount(PEOPLE_CHANGELOG).intValue(), NUM_RECORDS);
    assertThatBigQueryRecords(bigQueryResourceManager.readTable(PEOPLE_CHANGELOG))
        .hasRecordsWithStrings(peopleChangelogData);
    assertEquals(bigQueryResourceManager.getRowCount(PETS_CHANGELOG).intValue(), NUM_RECORDS * 2);
    assertThatBigQueryRecords(bigQueryResourceManager.readTable(PETS_CHANGELOG))
        .hasRecordsWithStrings(petsChangelogData);
  }

  private List<Map<String, Object>> getPeopleJdbcData() {
    List<Map<String, Object>> data = new ArrayList<>();
    for (int i = 0; i < NUM_RECORDS; i++) {
      Map<String, Object> values = new HashMap<>();
      values.put(ROW_ID, i);
      values.put(NAME, RandomStringUtils.randomAlphabetic(20));
      values.put(AGE, new Random().nextInt(100));
      data.add(values);
    }

    return data;
  }

  private List<Map<String, Object>> getPetsJdbcData() {
    List<Map<String, Object>> data = new ArrayList<>();
    for (int i = 0; i < NUM_RECORDS; i++) {
      Map<String, Object> values = new HashMap<>();
      values.put(NAME, RandomStringUtils.randomAlphabetic(20));
      values.put(AGE, new Random().nextInt(100));
      values.put(OWNER, RandomStringUtils.randomAlphabetic(20));
      data.add(values);
    }

    return data;
  }

  private String parseJdbcPort(String jdbcUrl) {
    Matcher match = JDBC_PORT_PATTERN.matcher(jdbcUrl);
    if (!match.matches()) {
      throw new IllegalArgumentException(
          String.format(
              "JDBC URL is not in ^jdbc:mysql://(.*?):[0-9]+/.*$ format: \"%s\"", jdbcUrl));
    }
    return match.group(1);
  }
}
