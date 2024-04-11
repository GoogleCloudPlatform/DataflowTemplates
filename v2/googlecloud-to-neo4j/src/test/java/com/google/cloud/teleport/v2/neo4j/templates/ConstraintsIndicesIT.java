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

import static com.google.cloud.teleport.v2.neo4j.templates.Connections.jsonBasicPayload;
import static com.google.cloud.teleport.v2.neo4j.templates.Resources.contentOf;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

public abstract class ConstraintsIndicesIT extends TemplateTestBase {
  private Neo4jResourceManager neo4jClient;

  protected abstract String neo4jTagName();

  protected abstract boolean dynamicDatabase();

  protected abstract boolean supportsNodeKeyConstraints();

  protected abstract boolean supportsRelationshipKeyConstraints();

  @Before
  public void setup() {
    neo4jClient =
        Neo4jResourceManager.builder(testName)
            .setDatabaseName(dynamicDatabase() ? null : "neo4j")
            .setAdminPassword("letmein!")
            .setHost(TestProperties.hostIp())
            .setContainerImageTag(neo4jTagName())
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(neo4jClient);
  }

  @Test
  public void doesNotCreateExtraIndicesWhenImportingNodes() throws Exception {
    gcsClient.createArtifact(
        "spec.json", contentOf("/testing-specs/constraints-indices/node-spec.json"));
    gcsClient.createArtifact("neo4j.json", jsonBasicPayload(neo4jClient));

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
                        supportsNodeKeyConstraints()
                            ? List.of(
                                Map.of(
                                    "entityType", "NODE",
                                    "labelsOrTypes", List.of("Person"),
                                    "properties", List.of("id")))
                            : Collections.emptyList())
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "SHOW INDEXES YIELD * WHERE type <> 'LOOKUP' RETURN count(*) AS count")
                    .setExpectedResult(
                        List.of(Map.of("count", supportsNodeKeyConstraints() ? 1L : 0L)))
                    .build());
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void doesNotCreateExtraIndicesWhenImportingRelationships() throws Exception {
    gcsClient.createArtifact(
        "spec.json", contentOf("/testing-specs/constraints-indices/edge-spec.json"));
    gcsClient.createArtifact("neo4j.json", jsonBasicPayload(neo4jClient));

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
                        Stream.of(
                                supportsNodeKeyConstraints()
                                    ? Map.of(
                                        "entityType", "NODE",
                                        "labelsOrTypes", List.of("Person"),
                                        "properties", List.of("personId"))
                                    : null,
                                supportsRelationshipKeyConstraints()
                                    ? Map.of(
                                        "entityType", "RELATIONSHIP",
                                        "labelsOrTypes", List.of("SELF_LINKS_TO"),
                                        "properties", List.of("id"))
                                    : null)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList()))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "SHOW INDEXES YIELD * WHERE type <> 'LOOKUP' RETURN count(*) AS count")
                    .setExpectedResult(
                        List.of(
                            Map.of(
                                "count",
                                (supportsNodeKeyConstraints() ? 1L : 0L)
                                    + (supportsRelationshipKeyConstraints() ? 1L : 0L))))
                    .build());
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void canResetDatabase() throws Exception {
    assertThat(
            neo4jClient.run(
                "UNWIND range(1, 1000) AS id CREATE (n1:From {id: id})-[:CONNECTED_TO {id: id + 1000}]->(n2:To {id: id + 2000}) RETURN id"))
        .hasSize(1000);
    if (supportsNodeKeyConstraints()) {
      assertThat(neo4jClient.run("CREATE CONSTRAINT FOR (n:From) REQUIRE n.id IS NODE KEY"))
          .isEmpty();
      assertThat(neo4jClient.run("CREATE CONSTRAINT FOR (n:To) REQUIRE n.id IS NODE KEY"))
          .isEmpty();
    }
    if (supportsRelationshipKeyConstraints()) {
      assertThat(
              neo4jClient.run(
                  "CREATE CONSTRAINT FOR ()-[r:CONNECTED_TO]-() REQUIRE r.id IS RELATIONSHIP KEY"))
          .isEmpty();
    }
    assertThat(neo4jClient.run("CREATE INDEX FOR (n:From) ON n.name")).isEmpty();

    gcsClient.createArtifact(
        "spec.json", contentOf("/testing-specs/constraints-indices/reset-database.json"));
    gcsClient.createArtifact("neo4j.json", jsonBasicPayload(neo4jClient));

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
                    .setQuery("MATCH (n:From) RETURN count(n) AS count")
                    .setExpectedResult(List.of(Map.of("count", 0L)))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery("MATCH (n:To) RETURN count(n) AS count")
                    .setExpectedResult(List.of(Map.of("count", 0L)))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery("MATCH ()-[r:CONNECTED_TO]->() RETURN count(r) AS count")
                    .setExpectedResult(List.of(Map.of("count", 0L)))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "SHOW CONSTRAINTS YIELD * RETURN entityType, labelsOrTypes, properties ORDER BY entityType ASC")
                    .setExpectedResult(
                        Stream.of(
                                supportsNodeKeyConstraints()
                                    ? Map.of(
                                        "entityType", "NODE",
                                        "labelsOrTypes", List.of("Person"),
                                        "properties", List.of("personId"))
                                    : null,
                                supportsRelationshipKeyConstraints()
                                    ? Map.of(
                                        "entityType", "RELATIONSHIP",
                                        "labelsOrTypes", List.of("SELF_LINKS_TO"),
                                        "properties", List.of("id"))
                                    : null)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList()))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "SHOW INDEXES YIELD * WHERE type <> 'LOOKUP' RETURN count(*) AS count")
                    .setExpectedResult(
                        List.of(
                            Map.of(
                                "count",
                                (supportsNodeKeyConstraints() ? 1L : 0L)
                                    + (supportsRelationshipKeyConstraints() ? 1L : 0L))))
                    .build());
    assertThatResult(result).meetsConditions();
  }

  @Category(TemplateIntegrationTest.class)
  @TemplateIntegrationTest(GoogleCloudToNeo4j.class)
  @RunWith(JUnit4.class)
  public static class Neo4j5EnterpriseIT extends ConstraintsIndicesIT {

    @Override
    protected String neo4jTagName() {
      return "5-enterprise";
    }

    @Override
    protected boolean dynamicDatabase() {
      return true;
    }

    @Override
    protected boolean supportsNodeKeyConstraints() {
      return true;
    }

    @Override
    protected boolean supportsRelationshipKeyConstraints() {
      return true;
    }
  }

  @Category(TemplateIntegrationTest.class)
  @TemplateIntegrationTest(GoogleCloudToNeo4j.class)
  @RunWith(JUnit4.class)
  public static class Neo4j5CommunityIT extends ConstraintsIndicesIT {

    @Override
    protected String neo4jTagName() {
      return "5";
    }

    @Override
    protected boolean dynamicDatabase() {
      return false;
    }

    @Override
    protected boolean supportsNodeKeyConstraints() {
      return false;
    }

    @Override
    protected boolean supportsRelationshipKeyConstraints() {
      return false;
    }
  }

  @Category(TemplateIntegrationTest.class)
  @TemplateIntegrationTest(GoogleCloudToNeo4j.class)
  @RunWith(JUnit4.class)
  public static class Neo4j44EnterpriseIT extends ConstraintsIndicesIT {

    @Override
    protected String neo4jTagName() {
      return "4.4-enterprise";
    }

    @Override
    protected boolean dynamicDatabase() {
      return true;
    }

    @Override
    protected boolean supportsNodeKeyConstraints() {
      return true;
    }

    @Override
    protected boolean supportsRelationshipKeyConstraints() {
      return false;
    }
  }

  @Category(TemplateIntegrationTest.class)
  @TemplateIntegrationTest(GoogleCloudToNeo4j.class)
  @RunWith(JUnit4.class)
  public static class Neo4j44CommunityIT extends ConstraintsIndicesIT {

    @Override
    protected String neo4jTagName() {
      return "4.4";
    }

    @Override
    protected boolean dynamicDatabase() {
      return false;
    }

    @Override
    protected boolean supportsNodeKeyConstraints() {
      return false;
    }

    @Override
    protected boolean supportsRelationshipKeyConstraints() {
      return false;
    }
  }
}
