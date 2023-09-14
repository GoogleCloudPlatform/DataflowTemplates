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
package com.google.cloud.teleport.v2.neo4j.templates;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.neo4j.Neo4jResourceManager;
import org.apache.beam.it.neo4j.conditions.Neo4jQueryCheck;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(GoogleCloudToNeo4j.class)
@RunWith(JUnit4.class)
public class PropertyMappingsIT extends TemplateTestBase {
  private BigQueryResourceManager bigQueryClient;
  private Neo4jResourceManager neo4jClient;

  @Before
  public void setup() {
    bigQueryClient = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    neo4jClient =
        Neo4jResourceManager.builder(testName)
            .setAdminPassword("letmein!")
            .setHost(TestProperties.hostIp())
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryClient, neo4jClient);
  }

  @Test
  public void mapsBooleanPropertiesFromInlineTextSource() throws Exception {
    gcsClient.createArtifact(
        "spec.json", contentOf("/testing-specs/property-mappings/booleans-text-spec.json"));
    gcsClient.createArtifact(
        "neo4j.json",
        String.format(
            "{\n"
                + "  \"server_url\": \"%s\",\n"
                + "  \"database\": \"%s\",\n"
                + "  \"auth_type\": \"basic\",\n"
                + "  \"username\": \"neo4j\",\n"
                + "  \"pwd\": \"%s\"\n"
                + "}",
            neo4jClient.getUri(), neo4jClient.getDatabaseName(), neo4jClient.getAdminPassword()));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jobSpecUri", getGcsPath("spec.json"))
            .addParameter("neo4jConnectionUri", getGcsPath("neo4j.json"));
    LaunchInfo info = launchTemplate(options);

    assertThatPipeline(info).isRunning();
    assertBooleansArePersisted(info);
  }

  @Test
  public void mapsBooleanPropertiesFromBigQuery() throws Exception {
    TableId table =
        bigQueryClient.createTable(
            testName,
            Schema.of(
                Field.newBuilder("id", StandardSQLTypeName.STRING).build(),
                Field.newBuilder("truthy", StandardSQLTypeName.BOOL).build()));
    bigQueryClient.write(
        testName,
        List.of(
            RowToInsert.of(Map.of("id", "bool-1", "truthy", false)),
            RowToInsert.of(Map.of("id", "bool-2", "truthy", true)),
            RowToInsert.of(Map.of("id", "bool-3", "truthy", true)),
            RowToInsert.of(Map.of("id", "bool-4", "truthy", false))));
    gcsClient.createArtifact(
        "spec.json", contentOf("/testing-specs/property-mappings/booleans-bq-spec.json"));
    gcsClient.createArtifact(
        "neo4j.json",
        String.format(
            "{\n"
                + "  \"server_url\": \"%s\",\n"
                + "  \"database\": \"%s\",\n"
                + "  \"auth_type\": \"basic\",\n"
                + "  \"username\": \"neo4j\",\n"
                + "  \"pwd\": \"%s\"\n"
                + "}",
            neo4jClient.getUri(), neo4jClient.getDatabaseName(), neo4jClient.getAdminPassword()));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jobSpecUri", getGcsPath("spec.json"))
            .addParameter("neo4jConnectionUri", getGcsPath("neo4j.json"))
            .addParameter(
                "optionsJson", String.format("{\"bqtable\": \"%s\"}", toTableSpecStandard(table)));
    LaunchInfo info = launchTemplate(options);

    assertThatPipeline(info).isRunning();
    assertBooleansArePersisted(info);
  }

  private String contentOf(String resourcePath) throws IOException {
    try (BufferedReader bufferedReader =
        new BufferedReader(
            new InputStreamReader(this.getClass().getResourceAsStream(resourcePath)))) {
      return bufferedReader.lines().collect(Collectors.joining("\n"));
    }
  }

  private void assertBooleansArePersisted(LaunchInfo info) {
    assertThatResult(
            pipelineOperator()
                .waitForCondition(
                    createConfig(info),
                    Neo4jQueryCheck.builder(neo4jClient)
                        .setQuery(
                            "MATCH (n) RETURN labels(n) AS labels, properties(n) AS props ORDER BY n.id ASC")
                        .setExpectedResult(
                            List.of(
                                Map.of(
                                    "labels",
                                    List.of("Boolean"),
                                    "props",
                                    Map.of("id", "bool-1", "truthy", false)),
                                Map.of(
                                    "labels",
                                    List.of("Boolean"),
                                    "props",
                                    Map.of("id", "bool-2", "truthy", true)),
                                Map.of(
                                    "labels",
                                    List.of("Boolean"),
                                    "props",
                                    Map.of("id", "bool-3", "truthy", true)),
                                Map.of(
                                    "labels",
                                    List.of("Boolean"),
                                    "props",
                                    Map.of("id", "bool-4", "truthy", false))))
                        .build()))
        .meetsConditions();
  }
}
