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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.it.iceberg.IcebergResourceManager;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(PubsubToIcebergYaml.class)
@RunWith(JUnit4.class)
public class PubsubToIcebergYamlIT extends TemplateTestBase {

  private PubsubResourceManager pubsubResourceManager;
  private IcebergResourceManager icebergResourceManager;

  private static final String CATALOG_NAME = "hadoop_catalog";
  private final String namespace =
      "pubsub_iceberg_ns_" + UUID.randomUUID().toString().replace("-", "");
  private static final String ICEBERG_TABLE_NAME = "iceberg_table";
  private final String icebergTableIdentifier = namespace + "." + ICEBERG_TABLE_NAME;

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();

    gcsClient.registerTempDir(namespace);

    icebergResourceManager =
        IcebergResourceManager.builder(testName)
            .setCatalogName(CATALOG_NAME)
            .setCatalogProperties(getCatalogProperties())
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager, icebergResourceManager);
  }

  @Test
  public void testPubsubToIceberg() throws IOException {
    // Pub/Sub setup
    TopicName topic = pubsubResourceManager.createTopic("input-topic");

    // Iceberg setup
    icebergResourceManager.createNamespace(namespace);
    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    icebergResourceManager.createTable(icebergTableIdentifier, icebergSchema);

    // JSON Schema for Pub/Sub reader
    String jsonSchema =
        "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"name\":{\"type\":\"string\"}}}";

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("topic", topic.toString())
            .addParameter("format", "JSON")
            .addParameter("schema", jsonSchema)
            .addParameter("table", icebergTableIdentifier)
            .addParameter("catalogName", CATALOG_NAME)
            .addParameter(
                "catalogProperties", new org.json.JSONObject(getCatalogProperties()).toString())
            .addParameter("triggeringFrequencySeconds", "10");

    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // Publish Pub/Sub message
    String message = "{\"id\": 1, \"name\": \"Alice\"}";
    pubsubResourceManager.publish(
        topic, Map.of(), ByteString.copyFrom(message, StandardCharsets.UTF_8));

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  try {
                    List<Record> records = icebergResourceManager.read(icebergTableIdentifier);
                    return records.size() >= 1;
                  } catch (Exception e) {
                    return false;
                  }
                });

    // Assert
    assertThatResult(result).meetsConditions();

    List<Record> records = icebergResourceManager.read(icebergTableIdentifier);
    assertEquals(1, records.size());
  }

  @Override
  protected PipelineOperator.Config createConfig(LaunchInfo info) {
    return PipelineOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(PROJECT)
        .setRegion(REGION)
        .build();
  }

  private Map<String, String> getCatalogProperties() {
    return Map.of(
        "type", "rest",
        "uri", "https://biglake.googleapis.com/iceberg/v1beta/restcatalog",
        "warehouse", "gs://" + gcsClient.getBucket(),
        "header.x-goog-user-project", PROJECT,
        "rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager",
        "rest-metrics-reporting-enabled", "false");
  }
}
