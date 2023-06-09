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
package com.google.cloud.teleport.templates;

import static com.google.cloud.teleport.it.gcp.datastore.matchers.DatastoreAsserts.assertThatDatastoreRecords;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.datastore.Entity;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.utils.PipelineUtils;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.datastore.DatastoreResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link TextToDatastore}. */
@Category(TemplateIntegrationTest.class)
@RunWith(JUnit4.class)
public final class TextToDatastoreIT extends TemplateTestBase {

  private DatastoreResourceManager datastoreResourceManager;

  @Before
  public void setup() {
    testId = PipelineUtils.createJobName("test");

    datastoreResourceManager =
        DatastoreResourceManager.builder(PROJECT, testId).credentials(credentials).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(datastoreResourceManager);
  }

  @Test
  @TemplateIntegrationTest(value = TextToDatastore.class, template = "GCS_Text_to_Datastore")
  public void testTextToDatastore() throws IOException {
    baseTextToDatastore(
        params ->
            params
                .addParameter("datastoreWriteProjectId", PROJECT)
                .addParameter("datastoreHintNumWorkers", "1"));
  }

  @Test
  @TemplateIntegrationTest(value = TextToDatastore.class, template = "GCS_Text_to_Firestore")
  public void testTextToFirestore() throws IOException {
    baseTextToDatastore(
        params ->
            params
                .addParameter("firestoreWriteProjectId", PROJECT)
                .addParameter("firestoreHintNumWorkers", "1"));
  }

  public void baseTextToDatastore(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {
    // Arrange
    gcsClient.createArtifact(
        "input/records.csv",
        "1,Dog\n"
            + "2,Cat\n"
            + "3,Fox\n"
            + "4,Turtle\n"
            + "5,Shark\n"
            + "6,Hawk\n"
            + "7,Crocodile\n");
    gcsClient.createArtifact(
        "input/udf.js",
        "function transform(value) {\n"
            + "  var record = value.split(',');\n"
            + "  return JSON.stringify(\n"
            + "   {\n"
            + "     key: {\n"
            + "       partitionId: {\n"
            + "         namespaceId: '"
            + testId
            + "'\n"
            + "       },\n"
            + "       path: [\n"
            + "         {\n"
            + "           kind: 'animal',\n"
            + "           id: parseInt(record[0])\n"
            + "         }\n"
            + "       ]\n"
            + "     },\n"
            + "     properties: {\n"
            + "       id: { stringValue: record[0] },\n"
            + "       name: { stringValue: record[1] }\n"
            + "     }\n"
            + "   });\n"
            + "}\n");

    // Act
    LaunchInfo info =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("textReadPattern", getGcsPath("input") + "/*.csv")
                    .addParameter("javascriptTextTransformGcsPath", getGcsPath("input/udf.js"))
                    .addParameter("javascriptTextTransformFunctionName", "transform")
                    .addParameter("errorWritePath", getGcsPath("errorWritePath"))));
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Entity> queryResults = datastoreResourceManager.query("SELECT * from animal");
    assertThat(queryResults).isNotEmpty();

    assertThatDatastoreRecords(queryResults)
        .hasRecordsUnordered(
            List.of(
                Map.of("id", "1", "name", "Dog"),
                Map.of("id", "2", "name", "Cat"),
                Map.of("id", "3", "name", "Fox"),
                Map.of("id", "7", "name", "Crocodile")));
  }
}
