/*
 * Copyright (C) 2025 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.base.MoreObjects;
import com.google.firestore.admin.v1.Database.DatabaseEdition;
import com.google.firestore.admin.v1.Database.DatabaseType;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.firestore.FirestoreAdminResourceManager;
import org.apache.beam.it.gcp.firestore.FirestoreResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link FirestoreToFirestore}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(FirestoreToFirestore.class)
@RunWith(JUnit4.class)
public final class FirestoreToFirestoreIT extends TemplateTestBase {

  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/flex/Firestore_to_Firestore");

  private FirestoreAdminResourceManager firestoreAdminResourceManager;

  private FirestoreResourceManager sourceFirestoreResourceManager;

  private FirestoreResourceManager destinationFirestoreResourceManager;

  private static final String PROJECT = TestProperties.project();
  private static final String REGION = TestProperties.region();
  private static final String SOURCE_DATABASE_ID = "source-database";
  private static final String DESTINATION_DATABASE_ID = "destination-database";

  @Before
  public void setUp() {
    firestoreAdminResourceManager =
        FirestoreAdminResourceManager.builder(testName)
            .setProject(PROJECT)
            .setRegion(REGION)
            .build();
    firestoreAdminResourceManager.createDatabase(
        SOURCE_DATABASE_ID, DatabaseType.FIRESTORE_NATIVE, DatabaseEdition.STANDARD);
    firestoreAdminResourceManager.createDatabase(
        DESTINATION_DATABASE_ID, DatabaseType.FIRESTORE_NATIVE, DatabaseEdition.ENTERPRISE);
    sourceFirestoreResourceManager =
        FirestoreResourceManager.builder(testName)
            .setProject(PROJECT)
            .setDatabase(SOURCE_DATABASE_ID)
            .setCredentials(TestProperties.googleCredentials())
            .build();
    destinationFirestoreResourceManager =
        FirestoreResourceManager.builder(testName)
            .setProject(PROJECT)
            .setDatabase(DESTINATION_DATABASE_ID)
            .setCredentials(TestProperties.googleCredentials())
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(sourceFirestoreResourceManager);
    ResourceManagerUtils.cleanResources(destinationFirestoreResourceManager);
    ResourceManagerUtils.cleanResources(firestoreAdminResourceManager);
  }

  @Test
  public void testFirestoreToFirestore_collectionIdProvided_copySingleCollection()
      throws IOException {
    String collectionId = "input-" + testName;
    int numDocuments = 10;

    Map<String, Map<String, Object>> inputData = generateTestDocuments(numDocuments);

    sourceFirestoreResourceManager.write(collectionId, inputData);

    LaunchConfig options =
        LaunchConfig.builder(testName, SPEC_PATH)
            .addParameter("sourceProjectId", PROJECT)
            .addParameter("sourceDatabaseId", SOURCE_DATABASE_ID)
            .addParameter("destinationProjectId", PROJECT)
            .addParameter("destinationDatabaseId", DESTINATION_DATABASE_ID)
            .addParameter("collectionIds", collectionId)
            .addParameter("maxNumWorkers", "10")
            .build();

    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info, Duration.ofMinutes(10)));
    assertThatResult(result).isLaunchFinished();

    List<QueryDocumentSnapshot> documents = destinationFirestoreResourceManager.read(collectionId);
    assertThat(documents).hasSize(numDocuments);

    for (QueryDocumentSnapshot document : documents) {
      assertThat(document.getData()).containsEntry("name", "test-doc-" + document.get("id"));
    }
  }

  @Test
  public void testFirestoreToFirestore_collectionIdNotProvided_copyAllCollections()
      throws IOException {
    String collectionId1 = "inputA-" + testName;
    int numDocs1 = 5;
    Map<String, Map<String, Object>> inputData1 = generateTestDocuments(numDocs1);
    sourceFirestoreResourceManager.write(collectionId1, inputData1);

    String collectionId2 = "inputB-" + testName;
    int numDocs2 = 5;
    Map<String, Map<String, Object>> inputData2 = generateTestDocuments(numDocs2);
    sourceFirestoreResourceManager.write(collectionId2, inputData2);

    LaunchConfig options =
        LaunchConfig.builder(testName + "-all", SPEC_PATH)
            .addParameter("sourceProjectId", PROJECT)
            .addParameter("sourceDatabaseId", SOURCE_DATABASE_ID)
            .addParameter("destinationProjectId", PROJECT)
            .addParameter("destinationDatabaseId", DESTINATION_DATABASE_ID)
            .addParameter("maxNumWorkers", "10")
            .build();

    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info, Duration.ofMinutes(15)));
    assertThatResult(result).isLaunchFinished();

    List<QueryDocumentSnapshot> documents1 =
        destinationFirestoreResourceManager.read(collectionId1);
    assertThat(documents1).hasSize(numDocs1);
    for (QueryDocumentSnapshot document : documents1) {
      assertThat(document.getData()).containsEntry("name", "test-doc-" + document.get("id"));
    }

    List<QueryDocumentSnapshot> documents2 =
        destinationFirestoreResourceManager.read(collectionId2);
    assertThat(documents2).hasSize(numDocs2);
    for (QueryDocumentSnapshot document : documents2) {
      assertThat(document.getData()).containsEntry("name", "test-doc-" + document.get("id"));
    }
  }

  private Map<String, Map<String, Object>> generateTestDocuments(int numDocuments) {
    Map<String, Map<String, Object>> testDocuments = new HashMap<>();
    for (int i = 1; i <= numDocuments; i++) {
      Map<String, Object> data = Map.of("id", i, "name", "test-doc-" + i);
      testDocuments.put("doc-" + i, data);
    }
    return testDocuments;
  }
}
