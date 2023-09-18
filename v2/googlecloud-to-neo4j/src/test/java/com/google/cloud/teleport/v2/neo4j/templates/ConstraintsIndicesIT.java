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

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
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
public class ConstraintsIndicesIT extends TemplateTestBase {
  private Neo4jResourceManager neo4jClient;

  @Before
  public void setup() {
    neo4jClient =
        Neo4jResourceManager.builder(testName)
            .setAdminPassword("letmein!")
            .setHost(TestProperties.hostIp())
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(neo4jClient);
  }

  @Test
  public void does_not_create_extra_indices_when_importing_nodes() throws Exception {
    gcsClient.createArtifact(
        "spec.json", contentOf("/testing-specs/constraints-indices/node-spec.json"));
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
    Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(info),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "SHOW CONSTRAINTS YIELD * RETURN entityType, labelsOrTypes, properties")
                    .setExpectedResult(
                        List.of(
                            Map.of(
                                "entityType", "NODE",
                                "labelsOrTypes", List.of("Person"),
                                "properties", List.of("id"))))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "SHOW INDEXES YIELD * WHERE owningConstraint IS NULL AND type <> 'LOOKUP' RETURN count(*) AS count")
                    .setExpectedResult(List.of(Map.of("count", 0L)))
                    .build());
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void does_not_create_extra_indices_when_importing_relationships() throws Exception {
    gcsClient.createArtifact(
        "spec.json", contentOf("/testing-specs/constraints-indices/edge-spec.json"));
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
    Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(info),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "SHOW CONSTRAINTS YIELD * RETURN entityType, labelsOrTypes, properties ORDER BY entityType ASC")
                    .setExpectedResult(
                        List.of(
                            Map.of(
                                "entityType", "NODE",
                                "labelsOrTypes", List.of("Person"),
                                "properties", List.of("personId")),
                            Map.of(
                                "entityType", "RELATIONSHIP",
                                "labelsOrTypes", List.of("SELF_LINKS_TO"),
                                "properties", List.of("id"))))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "SHOW INDEXES YIELD * WHERE owningConstraint IS NULL AND type <> 'LOOKUP' RETURN count(*) AS count")
                    .setExpectedResult(List.of(Map.of("count", 0L)))
                    .build());
    assertThatResult(result).meetsConditions();
  }

  private String contentOf(String resourcePath) throws IOException {
    try (BufferedReader bufferedReader =
        new BufferedReader(
            new InputStreamReader(this.getClass().getResourceAsStream(resourcePath)))) {
      return bufferedReader.lines().collect(Collectors.joining("\n"));
    }
  }
}
