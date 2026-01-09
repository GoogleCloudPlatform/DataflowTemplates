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
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.neo4j.Neo4jResourceManager;
import org.apache.beam.it.neo4j.conditions.Neo4jQueryCheck;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(GoogleCloudToNeo4j.class)
@RunWith(JUnit4.class)
public class CustomNeo4jWriteQueryIT extends TemplateTestBase {
  private static Neo4jResourceManager neo4jClient;

  @BeforeClass
  public static void setUpClass() {
    // Tests sharing an active neo4j test container
    neo4jClient =
        Neo4jResourceManager.builder(DataConversionIT.class.getName())
            .setAdminPassword("letmein!")
            .setHost(TestProperties.hostIp())
            .build();
  }

  @After
  public void cleanUp() {
    neo4jClient.run("MATCH (n) DETACH DELETE n;");
  }

  @AfterClass
  public static void tearDownClass() {
    ResourceManagerUtils.cleanResources(neo4jClient);
  }

  @Test
  public void testCustomQueryImport() throws IOException {
    String spec = contentOf("/testing-specs/custom-query/northwind-jobspec.json");
    gcsClient.createArtifact("inline-data-to-neo4j.json", spec);
    gcsClient.createArtifact(
        "neo4j-connection.json",
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
            .addParameter("jobSpecUri", getGcsPath("inline-data-to-neo4j.json"))
            .addParameter("neo4jConnectionUri", getGcsPath("neo4j-connection.json"));
    LaunchInfo info = launchTemplate(options);

    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(info),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "CALL db.schema.nodeTypeProperties() YIELD nodeLabels, propertyName, mandatory RETURN nodeLabels, collect(propertyName) AS propertyNames ORDER BY nodeLabels ASC")
                    .setExpectedResult(
                        List.of(
                            Map.of(
                                "nodeLabels", List.of("Customer"), "propertyNames", List.of("id")),
                            Map.of(
                                "nodeLabels", List.of("Seller"), "propertyNames", List.of("id"))))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "MATCH (n) RETURN labels(n) AS labels, count(n) AS count ORDER BY count ASC, labels ASC")
                    .setExpectedResult(
                        List.of(
                            Map.of("labels", List.of("Customer"), "count", 1L),
                            Map.of("labels", List.of("Seller"), "count", 1L)))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "CALL db.schema.relTypeProperties() YIELD relType, propertyName RETURN relType, collect(propertyName) AS propertyNames ORDER BY relType ASC")
                    .setExpectedResult(
                        List.of(
                            Map.of("relType", ":`SOLD`", "propertyNames", List.of("productId"))))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count ORDER BY count ASC")
                    .setExpectedResult(List.of(Map.of("type", "SOLD", "count", 2L)))
                    .build());
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void importsGraphInSinglePassByMergingEdgeAndItsNodes() throws IOException {
    String spec =
        contentOf("/testing-specs/inline-data/northwind-jobspec-single-pass-merge-all.json");
    gcsClient.createArtifact("inline-data-to-neo4j.json", spec);
    gcsClient.createArtifact("neo4j-connection.json", jsonBasicPayload(neo4jClient));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jobSpecUri", getGcsPath("inline-data-to-neo4j.json"))
            .addParameter("neo4jConnectionUri", getGcsPath("neo4j-connection.json"));
    LaunchInfo info = launchTemplate(options);

    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(info),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "CALL db.schema.nodeTypeProperties() YIELD nodeLabels, propertyName, mandatory RETURN nodeLabels, collect(propertyName) AS propertyNames ORDER BY nodeLabels ASC")
                    .setExpectedResult(
                        List.of(
                            Map.of(
                                "nodeLabels",
                                List.of("Customer"),
                                "propertyNames",
                                List.of("customerId")),
                            Map.of(
                                "nodeLabels",
                                List.of("Product"),
                                "propertyNames",
                                List.of("productId"))))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "MATCH (n) RETURN labels(n) AS labels, count(n) AS count ORDER BY count ASC")
                    .setExpectedResult(
                        List.of(
                            Map.of("labels", List.of("Customer"), "count", 7L),
                            Map.of("labels", List.of("Product"), "count", 52L)))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "CALL db.schema.relTypeProperties() YIELD relType, propertyName RETURN relType, collect(propertyName) AS propertyNames ORDER BY relType ASC")
                    .setExpectedResult(
                        List.of(
                            Map.of("relType", ":`PURCHASES`", "propertyNames", List.of("orderId"))))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count ORDER BY count ASC")
                    .setExpectedResult(List.of(Map.of("type", "PURCHASES", "count", 87L)))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "SHOW CONSTRAINTS YIELD type, entityType, labelsOrTypes, properties RETURN *")
                    .setExpectedResult(
                        List.of(
                            Map.of(
                                "type",
                                "NODE_KEY",
                                "entityType",
                                "NODE",
                                "labelsOrTypes",
                                List.of("Customer"),
                                "properties",
                                List.of("customerId")),
                            Map.of(
                                "type",
                                "NODE_KEY",
                                "entityType",
                                "NODE",
                                "labelsOrTypes",
                                List.of("Product"),
                                "properties",
                                List.of("productId"))))
                    .build());
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void importsGraphInSinglePassByCreatingEdgeAndMergingItsNodes() throws IOException {
    String spec =
        contentOf(
            // this contains the same order twice
            // append mode for rels, means the rels will be created twice
            // merge mode for rel nodes, means the nodes will be created only once
            "/testing-specs/inline-data/northwind-subset-jobspec-single-pass-create-edge-merge-nodes.json");
    gcsClient.createArtifact("inline-data-to-neo4j.json", spec);
    gcsClient.createArtifact("neo4j-connection.json", jsonBasicPayload(neo4jClient));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jobSpecUri", getGcsPath("inline-data-to-neo4j.json"))
            .addParameter("neo4jConnectionUri", getGcsPath("neo4j-connection.json"));
    LaunchInfo info = launchTemplate(options);

    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(info),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "CALL db.schema.nodeTypeProperties() YIELD nodeLabels, propertyName, mandatory RETURN nodeLabels, collect(propertyName) AS propertyNames ORDER BY nodeLabels ASC")
                    .setExpectedResult(
                        List.of(
                            Map.of(
                                "nodeLabels",
                                List.of("Customer"),
                                "propertyNames",
                                List.of("customerId")),
                            Map.of(
                                "nodeLabels",
                                List.of("Product"),
                                "propertyNames",
                                List.of("productId"))))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "MATCH (n) RETURN labels(n) AS labels, count(n) AS count ORDER BY count ASC")
                    .setExpectedResult(
                        List.of(
                            Map.of("labels", List.of("Customer"), "count", 1L),
                            Map.of("labels", List.of("Product"), "count", 1L)))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "CALL db.schema.relTypeProperties() YIELD relType, propertyName RETURN relType, collect(propertyName) AS propertyNames ORDER BY relType ASC")
                    .setExpectedResult(
                        List.of(
                            Map.of("relType", ":`PURCHASES`", "propertyNames", List.of("orderId"))))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count ORDER BY count ASC")
                    .setExpectedResult(List.of(Map.of("type", "PURCHASES", "count", 2L)))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "SHOW CONSTRAINTS YIELD type, entityType, labelsOrTypes, properties RETURN *")
                    .setExpectedResult(
                        List.of(
                            Map.of(
                                "type",
                                "NODE_KEY",
                                "entityType",
                                "NODE",
                                "labelsOrTypes",
                                List.of("Customer"),
                                "properties",
                                List.of("customerId")),
                            Map.of(
                                "type",
                                "NODE_KEY",
                                "entityType",
                                "NODE",
                                "labelsOrTypes",
                                List.of("Product"),
                                "properties",
                                List.of("productId"))))
                    .build());
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void importsInlineDataToNeo4J() throws IOException {
    String spec = contentOf("/testing-specs/inline-data/northwind-jobspec.json");
    gcsClient.createArtifact("inline-data-to-neo4j.json", spec);
    gcsClient.createArtifact("neo4j-connection.json", jsonBasicPayload(neo4jClient));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jobSpecUri", getGcsPath("inline-data-to-neo4j.json"))
            .addParameter("neo4jConnectionUri", getGcsPath("neo4j-connection.json"));
    LaunchInfo info = launchTemplate(options);

    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(info),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "CALL db.schema.nodeTypeProperties() "
                            + "YIELD nodeLabels, propertyName, mandatory "
                            + "WITH nodeLabels, propertyName "
                            + "ORDER BY nodeLabels, propertyName "
                            + "RETURN nodeLabels, collect(propertyName) AS propertyNames "
                            + "ORDER BY nodeLabels ASC")
                    .setExpectedResult(
                        List.of(
                            Map.of(
                                "nodeLabels",
                                List.of("Customer"),
                                "propertyNames",
                                List.of("companyName", "contactName", "customerId")),
                            Map.of(
                                "nodeLabels",
                                List.of("Product"),
                                "propertyNames",
                                List.of("amount", "productId", "productName", "quantity"))))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "MATCH (n) RETURN labels(n) AS labels, count(n) AS count ORDER BY count ASC")
                    .setExpectedResult(
                        List.of(
                            Map.of("labels", List.of("Customer"), "count", 7L),
                            Map.of("labels", List.of("Product"), "count", 52L)))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "CALL db.schema.relTypeProperties() "
                            + "YIELD relType, propertyName "
                            + "WITH relType, propertyName "
                            + "ORDER BY relType, propertyName "
                            + "RETURN relType, collect(propertyName) AS propertyNames "
                            + "ORDER BY relType ASC")
                    .setExpectedResult(
                        List.of(
                            Map.of(
                                "relType",
                                ":`PURCHASES`",
                                "propertyNames",
                                List.of("amount", "orderId", "quantity"))))
                    .build(),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count ORDER BY count ASC")
                    .setExpectedResult(List.of(Map.of("type", "PURCHASES", "count", 97L)))
                    .build());
    assertThatResult(result).meetsConditions();
  }

  @Test
  public void importsMovieGraphFromInlineData() throws IOException {
    gcsClient.createArtifact(
        "spec.json", contentOf("/testing-specs/import-spec/movie-import/spec.json"));
    gcsClient.createArtifact("neo4j.json", jsonBasicPayload(neo4jClient));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jobSpecUri", getGcsPath("spec.json"))
            .addParameter("neo4jConnectionUri", getGcsPath("neo4j.json"));
    LaunchInfo info = launchTemplate(options);

    assertThatPipeline(info).isRunning();
    assertThatResult(
            pipelineOperator()
                .waitForConditionAndCancel(
                    createConfig(info),
                    Neo4jQueryCheck.builder(neo4jClient)
                        .setQuery("MATCH (n:Person) RETURN count(n) AS count")
                        .setExpectedResult(List.of(Map.of("count", 4L)))
                        .build(),
                    Neo4jQueryCheck.builder(neo4jClient)
                        .setQuery("MATCH (n:Movie) RETURN count(n) AS count")
                        .setExpectedResult(List.of(Map.of("count", 2L)))
                        .build(),
                    Neo4jQueryCheck.builder(neo4jClient)
                        .setQuery("MATCH (:Person)-[r:ACTED_IN]->(:Movie) RETURN count(r) AS count")
                        .setExpectedResult(List.of(Map.of("count", 2L)))
                        .build(),
                    Neo4jQueryCheck.builder(neo4jClient)
                        .setQuery("MATCH (:Person)-[r:DIRECTED]->(:Movie) RETURN count(r) AS count")
                        .setExpectedResult(List.of(Map.of("count", 2L)))
                        .build()))
        .meetsConditions();
  }

  @Test
  public void allowsSameKeyNamesAcrossRelationshipsAndNodes() throws IOException {
    gcsClient.createArtifact("spec.json", contentOf("/testing-specs/validation/spec.json"));
    gcsClient.createArtifact("neo4j.json", jsonBasicPayload(neo4jClient));

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("jobSpecUri", getGcsPath("spec.json"))
            .addParameter("neo4jConnectionUri", getGcsPath("neo4j.json"));
    LaunchInfo info = launchTemplate(options);
    Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(info),
                Neo4jQueryCheck.builder(neo4jClient)
                    .setQuery(
                        "MATCH (p:Person)-[l:LIKES]->(f:Food) "
                            + "RETURN l.id AS likeId, l.rating AS rating, p.id AS personId, f.id AS foodId "
                            + "ORDER BY likeId ASC")
                    .setExpectedResult(
                        List.of(
                            Map.of(
                                "likeId", "1",
                                "rating", 5L,
                                "personId", "1",
                                "foodId", "1"),
                            Map.of(
                                "likeId", "2",
                                "rating", 4L,
                                "personId", "1",
                                "foodId", "2"),
                            Map.of(
                                "likeId", "3",
                                "rating", 1L,
                                "personId", "3",
                                "foodId", "3")))
                    .build());
    assertThatResult(result).meetsConditions();
    assertThatPipeline(info).isRunning();
  }
}
