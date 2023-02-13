/*
 * Copyright (C) 2022 Google LLC
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

import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatRecords;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link PubSubToBigQuery} flex template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(value = PubSubToBigQuery.class, template = "PubSub_to_BigQuery")
@RunWith(JUnit4.class)
public final class PubSubToBigQueryIT extends TemplateTestBase {

  private PubsubResourceManager pubsubResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName.getMethodName(), PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(testName.getMethodName(), PROJECT)
            .setCredentials(credentials)
            .build();
  }

  @After
  public void cleanUp() {
    pubsubResourceManager.cleanupAll();
    bigQueryResourceManager.cleanupAll();
  }

  @Test
  public void testTopicToBigQuery() throws IOException {
    testTextToBigQuery(Function.identity()); // no extra parameters
  }

  @Test
  public void testTopicToBigQueryWithStorageApi() throws IOException {
    testTextToBigQuery(
        b ->
            b.addParameter("useStorageWriteApi", "true")
                .addParameter("numStorageWriteApiStreams", "1")
                .addParameter("storageWriteApiTriggeringFrequencySec", "5"));
  }

  private void testTextToBigQuery(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {
    // Arrange
    Map<String, Object> message = Map.of("job", testName.getMethodName(), "msg", "message");
    List<Field> bqSchemaFields =
        Arrays.asList(
            Field.of("job", StandardSQLTypeName.STRING),
            Field.of("msg", StandardSQLTypeName.STRING));
    Schema bqSchema = Schema.of(bqSchemaFields);

    TopicName topic = pubsubResourceManager.createTopic("input");
    bigQueryResourceManager.createDataset(REGION);
    TableId table = bigQueryResourceManager.createTable(testName.getMethodName(), bqSchema);

    LaunchConfig.Builder options =
        paramsAdder.apply(
            LaunchConfig.builder(testName, specPath)
                .addParameter("inputTopic", topic.toString())
                .addParameter("outputTableSpec", toTableSpec(table)));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    AtomicReference<TableResult> records = new AtomicReference<>();
    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  ByteString messageData =
                      ByteString.copyFromUtf8(new JSONObject(message).toString());
                  pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);

                  TableResult values = bigQueryResourceManager.readTable(table);
                  records.set(values);
                  return values.getTotalRows() != 0;
                });

    // Assert
    assertThatResult(result).meetsConditions();
    assertThatRecords(records.get()).allMatch(message);
  }
}
