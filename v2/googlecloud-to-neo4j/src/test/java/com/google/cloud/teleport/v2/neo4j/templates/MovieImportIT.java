/*
 * Copyright (C) 2024 Google LLC
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
import net.jcip.annotations.NotThreadSafe;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
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
@NotThreadSafe
public class MovieImportIT extends TemplateTestBase {
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
}
