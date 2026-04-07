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
package com.google.cloud.dataflow.cdc.connector;

import static org.junit.Assert.assertTrue;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
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
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.pubsub.conditions.PubsubMessagesCheck;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
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

/** Integration test for {@link App} embedded connector. */
@Category(TemplateIntegrationTest.class)
@RunWith(JUnit4.class)
public class CdcEmbeddedConnectorIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(CdcEmbeddedConnectorIT.class);
  private MySQLResourceManager mySQLResourceManager;
  private PubsubResourceManager pubsubResourceManager;

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final int NUM_RECORDS = 5;
  private static final String TABLE_NAME = "users";
  private static final String ROW_ID = "id";
  private static final String NAME = "name";

  private static final Pattern JDBC_PORT_PATTERN =
      Pattern.compile("^jdbc:mysql://(?<host>.*?):(?<port>[0-9]+).*$");

  @Before
  public void setUp() throws IOException {
    Credentials credentials;
    if (TestProperties.hasAccessToken()) {
      credentials = TestProperties.googleCredentials();
    } else {
      credentials = TestProperties.buildCredentialsFromEnv();
    }
    credentialsProvider = FixedCredentialsProvider.create(credentials);

    mySQLResourceManager = MySQLResourceManager.builder(testName).build();
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(mySQLResourceManager, pubsubResourceManager);
  }

  @Test
  public void testConnectorPushesToPubSub() throws Exception {
    // Arrange
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);
    mySQLResourceManager.createTable(TABLE_NAME, schema);

    // Debezium authenticates with MySQL using the sha2_password authentication (pre MySQL 8.0)
    // algorithm, or else it won't connect
    mySQLResourceManager.runSQLUpdate(
        "ALTER USER 'root'@'%' IDENTIFIED WITH mysql_native_password BY '"
            + mySQLResourceManager.getPassword()
            + "';");

    String formattedTimestamp =
        DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")
            .withZone(ZoneId.of("UTC"))
            .format(Instant.now());

    String topicName = String.format("%s_%s", "cdc_test_topic", formattedTimestamp);
    TopicName topic = pubsubResourceManager.createTopicWithoutPrefix(topicName);
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, "cdc_test_subscription");

    Path path = tmpFolder.newFile("connector.properties").toPath();
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("databaseName=" + testName + System.lineSeparator());
    stringBuilder.append("databaseUsername=" + mySQLResourceManager.getUsername() + System.lineSeparator());
    stringBuilder.append("databasePassword=" + mySQLResourceManager.getPassword() + System.lineSeparator());
    stringBuilder.append("databaseAddress=" + mySQLResourceManager.getHost() + System.lineSeparator());
    stringBuilder.append("databasePort=" + parseJdbcPort(mySQLResourceManager.getUri()) + System.lineSeparator());
    stringBuilder.append("gcpProject=" + PROJECT + System.lineSeparator());
    stringBuilder.append("gcpPubsubTopicPrefix=" + topicName + System.lineSeparator());
    stringBuilder.append("singleTopicMode=true" + System.lineSeparator());
    stringBuilder.append("inMemoryOffsetStorage=true" + System.lineSeparator());
    stringBuilder.append("debezium.offset.flush.interval.ms=300" + System.lineSeparator());
    stringBuilder.append("whitelistedTables=" + testName + "." + mySQLResourceManager.getDatabaseName() + "." + TABLE_NAME + System.lineSeparator());

    Files.write(path, stringBuilder.toString().getBytes());

    // Act
    DebeziumToPubSubDataSender dataSender = App.setup(new String[] {path.toString()});
    Thread connectorThread = new Thread(dataSender);
    connectorThread.start();

    // Wait for connector to start - max 10s
    boolean started = false;
    for (int i = 0; i < 5; i++) {
      if (connectorThread.isAlive()) {
        started = true;
        break;
      }
      TimeUnit.SECONDS.sleep(2);
    }
    assertTrue("Connector thread should be alive", started);

    // Give it a bit more time to initialize Debezium
    TimeUnit.SECONDS.sleep(10);

    // Write data
    List<Map<String, Object>> data = new ArrayList<>();
    for (int i = 0; i < NUM_RECORDS; i++) {
      Map<String, Object> values = new HashMap<>();
      values.put(ROW_ID, i);
      values.put(NAME, RandomStringUtils.randomAlphabetic(10));
      data.add(values);
    }
    mySQLResourceManager.write(TABLE_NAME, data);

    // Assert
    PubsubMessagesCheck pubsubCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, subscription)
            .setMinMessages(NUM_RECORDS)
            .build();

    boolean conditionMet = false;
    // Wait 60s max
    for (int i = 0; i < 12; i++) {
      if (pubsubCheck.get()) {
        conditionMet = true;
        break;
      }
      TimeUnit.SECONDS.sleep(5);
    }

    assertTrue("Should have received messages in Pub/Sub", conditionMet);

    // Stop connector
    try {
        dataSender.stop();
    } catch (Exception e) {
        LOG.warn("Error stopping data sender", e);
    }
  }

  private String parseJdbcPort(String jdbcUrl) {
    Matcher match = JDBC_PORT_PATTERN.matcher(jdbcUrl);
    if (!match.matches()) {
      throw new IllegalArgumentException(
          String.format(
              "JDBC URL is not in ^jdbc:mysql://(.*):[0-9]+/.*$ format: \"%s\"", jdbcUrl));
    }
    return match.group("port");
  }
}
