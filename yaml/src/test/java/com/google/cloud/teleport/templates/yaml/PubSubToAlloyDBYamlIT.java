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
package com.google.cloud.teleport.templates.yaml;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.apache.beam.it.jdbc.conditions.JDBCRowsCheck;
import org.apache.commons.lang3.RandomStringUtils;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link PubSubToAlloyDBYaml}. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(PubSubToAlloyDBYaml.class)
@RunWith(JUnit4.class)
public final class PubSubToAlloyDBYamlIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubToAlloyDBYamlIT.class);
  private static final int MESSAGES_COUNT = 10;
  private static final String ALLOYDB_TABLE_NAME = "alloydb_table";

  private PubsubResourceManager pubsubResourceManager;
  private PostgresResourceManager postgresResourceManager;

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
    postgresResourceManager = PostgresResourceManager.builder(testName).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager, postgresResourceManager);
  }

  @Test
  public void testPubSubToAlloyDB() throws IOException {
    pubSubToAlloyDB(Function.identity());
  }

  public void pubSubToAlloyDB(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {

    LOG.info("Starting pubSubToAlloyDB test.");

    // Arrange - AlloyDB (Postgres) setup
    LinkedHashMap<String, String> columns = new LinkedHashMap<>();
    columns.put("id", "INTEGER");
    columns.put("name", "VARCHAR(255)");
    JDBCResourceManager.JDBCSchema jdbcSchema = new JDBCResourceManager.JDBCSchema(columns, "id");
    postgresResourceManager.createTable(ALLOYDB_TABLE_NAME, jdbcSchema);

    // Arrange - Pub/Sub topic setup
    String nameSuffix = RandomStringUtils.randomAlphanumeric(8);
    TopicName inputTopic = pubsubResourceManager.createTopic("input-" + nameSuffix);

    String schema =
        "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"name\":{\"type\":\"string\"}}}";

    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("topic", inputTopic.toString())
                .addParameter("format", "JSON")
                .addParameter("schema", schema)
                .addParameter("jdbcUrl", postgresResourceManager.getUri())
                .addParameter("username", postgresResourceManager.getUsername())
                .addParameter("password", postgresResourceManager.getPassword())
                .addParameter("alloydbTable", ALLOYDB_TABLE_NAME)
                .addParameter("size", "10"));

    // Act - Launch pipeline
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // Publish messages to inputTopic
    LOG.info("Publishing {} messages to input topic...", MESSAGES_COUNT);
    List<Map<String, Object>> expectedMessages = new ArrayList<>();
    for (int i = 1; i <= MESSAGES_COUNT; i++) {
      Map<String, Object> message = Map.of("id", i, "name", "name_" + i);
      String messageJson = new JSONObject(message).toString();
      pubsubResourceManager.publish(inputTopic, Map.of(), ByteString.copyFromUtf8(messageJson));
      expectedMessages.add(message);
    }

    // Wait for pipeline processing
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                JDBCRowsCheck.builder(postgresResourceManager, ALLOYDB_TABLE_NAME)
                    .setMinRows(MESSAGES_COUNT)
                    .build());

    // Assert
    assertThatResult(result).meetsConditions();

    List<Map<String, Object>> records = postgresResourceManager.readTable(ALLOYDB_TABLE_NAME);
    assertThat(records).hasSize(MESSAGES_COUNT);
    assertThatRecords(records).hasRecordsUnorderedCaseInsensitiveColumns(expectedMessages);
  }
}
