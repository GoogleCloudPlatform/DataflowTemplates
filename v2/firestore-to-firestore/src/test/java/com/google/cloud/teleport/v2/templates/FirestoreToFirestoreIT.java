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

import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.firestore.FirestoreResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link FirestoreToFirestore}. */
@Category(TemplateIntegrationTest.class)
@RunWith(JUnit4.class)
public final class FirestoreToFirestoreIT extends TemplateTestBase {

  private FirestoreResourceManager sourceFirestoreResourceManager;

  private FirestoreResourceManager destinationFirestoreResourceManager;

  private static final String PROJECT = TestProperties.project();
  private static final String REGION = TestProperties.region();
  // TODO: configure these on pipeline options.
  private static final String SOURCE_DATABASE_ID = "sourceDatabase";
  private static final String DESTINATION_DATABASE_ID = "destinationDatabase";

  @Before
  public void setUp() {
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
  }

  private Map<String, Map<String, Object>> generateTestDocuments(int numDocuments) {
    Map<String, Map<String, Object>> testDocuments = new HashMap<>();
    for (int i = 1; i <= numDocuments; i++) {
      Map<String, Object> data = Map.of("id", i, "name", "test-doc-" + i);
      testDocuments.put("doc-" + i, data);
    }
    return testDocuments;
  }

  @Test
  public void testFirestoreToFirestore() throws IOException {
    String collectionId = "input-" + testName;
    int numDocuments = 10;

    Map<String, Map<String, Object>> inputData = generateTestDocuments(numDocuments);

    sourceFirestoreResourceManager.write(collectionId, inputData);

    // TODO: use actual pipeline params.
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, "Firestore_to_Firestore")
            .addParameter("sourceProjectId", PROJECT)
            .addParameter("sourceDatabaseId", SOURCE_DATABASE_ID)
            .addParameter("destinationProjectId", PROJECT)
            .addParameter("destinationDatabaseId", DESTINATION_DATABASE_ID)
            .addParameter("collectionIds", collectionId)
            .addParameter("maxNumWorkers", "10");

    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options.build());
    assertThatPipeline(info).isRunning();

    List<QueryDocumentSnapshot> documents = destinationFirestoreResourceManager.read(collectionId);
    assertThat(documents).hasSize(numDocuments);

    for (QueryDocumentSnapshot document : documents) {
      assertThat(document.getData()).containsEntry("name", "test-doc-" + document.get("id"));
    }
  }
}
