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

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatResult;
import static com.google.cloud.teleport.it.gcp.datastore.matchers.DatastoreAsserts.assertThatDatastoreRecords;

import com.google.cloud.datastore.Entity;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
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

/** Integration test for {@link DatastoreToDatastoreDelete}. */
@Category(TemplateIntegrationTest.class)
@RunWith(JUnit4.class)
public final class DatastoreToDatastoreDeleteIT extends TemplateTestBase {

  private DatastoreResourceManager datastoreResourceManager;

  @Before
  public void setup() {
    datastoreResourceManager =
        DatastoreResourceManager.builder(PROJECT, testId).credentials(credentials).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(datastoreResourceManager);
  }

  @Test
  @TemplateIntegrationTest(
      value = DatastoreToDatastoreDelete.class,
      template = "Datastore_to_Datastore_Delete")
  public void testDatastoreToDatastore() throws IOException {
    baseTestDatastoreToDatastore(
        "persondatastore",
        params ->
            params
                .addParameter(
                    "datastoreReadGqlQuery",
                    "SELECT * FROM persondatastore WHERE name = \"John Doe\"")
                .addParameter("datastoreReadProjectId", PROJECT)
                .addParameter("datastoreReadNamespace", testId)
                .addParameter("datastoreDeleteProjectId", PROJECT)
                .addParameter("datastoreHintNumWorkers", "1"));
  }

  @Test
  @TemplateIntegrationTest(
      value = DatastoreToDatastoreDelete.class,
      template = "Firestore_to_Firestore_Delete")
  public void testFirestoreToFirestore() throws IOException {
    baseTestDatastoreToDatastore(
        "personfirestore",
        params ->
            params
                .addParameter(
                    "firestoreReadGqlQuery",
                    "SELECT * FROM personfirestore WHERE name = \"John Doe\"")
                .addParameter("firestoreReadProjectId", PROJECT)
                .addParameter("firestoreReadNamespace", testId)
                .addParameter("firestoreDeleteProjectId", PROJECT)
                .addParameter("firestoreHintNumWorkers", "1"));
  }

  public void baseTestDatastoreToDatastore(
      String tableName, Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {

    // Arrange
    datastoreResourceManager.insert(
        tableName,
        Map.of(
            1L,
            Entity.newBuilder().set("name", "John Doe").build(),
            2L,
            Entity.newBuilder().set("name", "John Doe").build(),
            3L,
            Entity.newBuilder().set("name", "Archimedes").build()));

    LaunchConfig.Builder options = paramsAdder.apply(LaunchConfig.builder(testName, specPath));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    // Make sure that records got deleted
    List<Entity> queryResults = datastoreResourceManager.query("SELECT * from " + tableName);
    assertThatDatastoreRecords(queryResults).hasRows(1);
    assertThatDatastoreRecords(queryResults).hasRecordUnordered(Map.of("name", "Archimedes"));
  }
}
