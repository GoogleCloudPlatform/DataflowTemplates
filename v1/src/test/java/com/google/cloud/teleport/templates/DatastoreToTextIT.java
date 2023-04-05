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

import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatArtifacts;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.teleport.it.PipelineUtils;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.artifacts.Artifact;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import com.google.cloud.teleport.it.datastore.DatastoreResourceManager;
import com.google.cloud.teleport.it.datastore.DefaultDatastoreResourceManager;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.re2j.Pattern;
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
public final class DatastoreToTextIT extends TemplateTestBase {

  private DatastoreResourceManager datastoreResourceManager;

  @Before
  public void setup() {
    testId = PipelineUtils.createJobName("test");

    datastoreResourceManager =
        DefaultDatastoreResourceManager.builder(PROJECT, testId).credentials(credentials).build();

    gcsClient.createArtifact(
        "input/udf.js",
        "function uppercaseName(value) {\n"
            + "  const data = JSON.parse(value);\n"
            + "  data.properties.name.stringValue = data.properties.name.stringValue.toUpperCase();\n"
            + "  return JSON.stringify(data);\n"
            + "}");
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(datastoreResourceManager);
  }

  @Test
  @TemplateIntegrationTest(value = DatastoreToText.class, template = "Datastore_to_GCS_Text")
  public void testDatastoreToText() throws IOException {
    baseTextToDatastore(
        params ->
            params
                .addParameter("datastoreReadGqlQuery", "SELECT * from animal")
                .addParameter("datastoreReadProjectId", PROJECT)
                .addParameter("datastoreReadNamespace", testId));
  }

  @Test
  @TemplateIntegrationTest(value = DatastoreToText.class, template = "Firestore_to_GCS_Text")
  public void testFirestoreToText() throws IOException {
    baseTextToDatastore(
        params ->
            params
                .addParameter("firestoreReadGqlQuery", "SELECT * from animal")
                .addParameter("firestoreReadProjectId", PROJECT)
                .addParameter("firestoreReadNamespace", testId));
  }

  public void baseTextToDatastore(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {
    // Arrange
    datastoreResourceManager.insert(
        "animal",
        Map.of(
            1L,
            createEntity("Dog"),
            2L,
            createEntity("Cat"),
            3L,
            createEntity("Fox"),
            4L,
            createEntity("Turtle"),
            5L,
            createEntity("Shark"),
            6L,
            createEntity("Hawk"),
            7L,
            createEntity("Crocodile")));

    // Act
    LaunchInfo info =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("javascriptTextTransformGcsPath", getGcsPath("input/udf.js"))
                    .addParameter("javascriptTextTransformFunctionName", "uppercaseName")
                    .addParameter("textWritePrefix", getGcsPath("output/write-"))));
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> artifacts = gcsClient.listArtifacts("output/", Pattern.compile(".*"));
    assertThat(artifacts).isNotEmpty();

    assertThatArtifacts(artifacts)
        .asJsonRecords()
        .hasRecordsWithStrings(
            List.of(
                "path=[{kind=animal, id=1}]}, properties={name={stringValue=DOG}}",
                "path=[{kind=animal, id=2}]}, properties={name={stringValue=CAT}}",
                "path=[{kind=animal, id=3}]}, properties={name={stringValue=FOX}}",
                "path=[{kind=animal, id=4}]}, properties={name={stringValue=TURTLE}}",
                "path=[{kind=animal, id=5}]}, properties={name={stringValue=SHARK}}",
                "path=[{kind=animal, id=6}]}, properties={name={stringValue=HAWK}}",
                "path=[{kind=animal, id=7}]}, properties={name={stringValue=CROCODILE}}"));
  }

  private FullEntity<IncompleteKey> createEntity(String name) {
    return Entity.newBuilder().set("name", name).build();
  }
}
