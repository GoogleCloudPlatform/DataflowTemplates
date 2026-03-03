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

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.GeoPoint;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.base.MoreObjects;
import com.google.firestore.admin.v1.Database.DatabaseEdition;
import com.google.firestore.admin.v1.Database.DatabaseType;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
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

/**
 * Integration test for {@link FirestoreToFirestore}.
 */
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
  private Random random;

  private static final String PROJECT = TestProperties.project();
  private static final String REGION = TestProperties.region();

  @Before
  public void setUp() {
    firestoreAdminResourceManager =
        FirestoreAdminResourceManager.builder(testName)
            .setProject(PROJECT)
            .setRegion(REGION)
            .build();
    firestoreAdminResourceManager.createDatabase(
        sourceDatabaseId(), DatabaseType.FIRESTORE_NATIVE, DatabaseEdition.STANDARD);
    firestoreAdminResourceManager.createDatabase(
        destinationDatabaseId(), DatabaseType.FIRESTORE_NATIVE, DatabaseEdition.ENTERPRISE);
    sourceFirestoreResourceManager =
        FirestoreResourceManager.builder(testName)
            .setProject(PROJECT)
            .setDatabase(sourceDatabaseId())
            .setCredentials(TestProperties.googleCredentials())
            .build();
    destinationFirestoreResourceManager =
        FirestoreResourceManager.builder(testName)
            .setProject(PROJECT)
            .setDatabase(destinationDatabaseId())
            .setCredentials(TestProperties.googleCredentials())
            .build();
    random = new Random();
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

    LaunchInfo info = launchPipeline(/*testName=*/ "copySingleCollection", collectionId);
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

    LaunchInfo info = launchPipeline(/*testName=*/ "copyAll", /*collectionIds=*/ "");
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

  @Test
  public void testFirestoreToFirestore_fuzzDataTypes() throws IOException {
    String collectionId = "fuzz-" + testName;
    int numDocuments = 20;

    Map<String, Map<String, Object>> inputData = new HashMap<>();
    for (int i = 0; i < numDocuments; i++) {

      String documentId = "fuzzDocument-" + i + "-" + UUID.randomUUID();
      inputData.put(documentId, generateRandomDocument());
    }

    sourceFirestoreResourceManager.write(collectionId, inputData);

    LaunchInfo info = launchPipeline(/*testName=*/ "copyFuzz", collectionId);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info, Duration.ofMinutes(10)));
    assertThatResult(result).isLaunchFinished();

    List<QueryDocumentSnapshot> documents = destinationFirestoreResourceManager.read(collectionId);
    assertThat(documents).hasSize(numDocuments);

    for (QueryDocumentSnapshot destDoc : documents) {
      Map<String, Object> expectedData = inputData.get(destDoc.getId());
      assertThat(expectedData).isNotNull();

      Map<String, Object> destData = destDoc.getData();
      assertThat(destData.size()).isEqualTo(expectedData.size());

      for (Map.Entry<String, Object> entry : expectedData.entrySet()) {
        String key = entry.getKey();
        Object expectedValue = entry.getValue();
        Object actualValue = destData.get(key);

        if (expectedValue == null) {
          assertThat(actualValue).isNull();
        } else {
          assertThat(actualValue).isInstanceOf(expectedValue.getClass());
        }
      }
    }
  }


  private LaunchInfo launchPipeline(String testName, String collectionIds)
      throws IOException {
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, SPEC_PATH)
            .addParameter("sourceProjectId", PROJECT)
            .addParameter("sourceDatabaseId", sourceDatabaseId())
            .addParameter("destinationProjectId", PROJECT)
            .addParameter("destinationDatabaseId", destinationDatabaseId())
            .addParameter("maxNumWorkers", "10");

    if (!collectionIds.isEmpty()) {
      options.addParameter("collectionIds", collectionIds);
    }

    return pipelineLauncher.launch(PROJECT, REGION, options.build());
  }

  private String sourceDatabaseId() {
    return ("src-" + testName).toLowerCase().replaceAll("[^a-z0-9]", "-").replaceAll("-+", "-")
        .substring(0, 10);
  }

  private String destinationDatabaseId() {
    return ("dest-" + testName).toLowerCase().replaceAll("[^a-z0-9]", "-").replaceAll("-+", "-")
        .substring(0, 10);
  }

  public Map<String, Object> generateRandomDocument() {
    Map<String, Object> documentData = new HashMap<>();

    // Add a few random types to each document
    int numFields = random.nextInt(5) + 3; // 3 to 7 fields
    for (int j = 0; j < numFields; j++) {
      documentData.put("field-" + j, getRandomFirestoreType(1));
    }

    // Occasionally add fields with challenging names
    if (random.nextBoolean()) {
      documentData.put("field with spaces", random.nextInt());
    }
    if (random.nextBoolean()) {
      documentData.put("field.with.dots", randomString(10));
    }
    if (random.nextBoolean()) {
      documentData.put("field-with-hyphen", random.nextBoolean());
    }
    if (random.nextBoolean()) {
      documentData.put("utf8_日本", randomString(10));
    }

    return documentData;
  }

  private Object getRandomFirestoreType(int depth) {
    if (depth > 3) { // Limit nesting depth
      return randomString(random.nextInt(10));
    }
    int type = random.nextInt(11);
    switch (type) {
      case 0 -> {
        return null;
      }
      case 1 -> {
        return random.nextBoolean();
      }
      case 2 -> {
        return random.nextLong();
      }
      case 3 -> {
        return random.nextDouble() * 2000 - 1000; // Range -1000 to 1000
      }
      case 4 -> {
        return Timestamp.of(new Date(random.nextLong() % 3000000000000L)); // Random date
      }
      case 5 -> {
        return randomString(random.nextInt(50));
      }
      case 6 -> {
        return Blob.fromByteString(randomByteString(random.nextInt(50)));
      }
      case 7 -> {
        List<Object> list = new ArrayList<>();
        int size = random.nextInt(5);
        for (int i = 0; i < size; i++) {
          Object element = getRandomFirestoreType(depth + 1);
          while (element instanceof List) {
            // Firestore does not allow nested arrays
            element = getRandomFirestoreType(depth + 1);
          }
          list.add(element);
        }
        return list;
      }
      case 8 -> {
        Map<String, Object> map = new HashMap<>();
        int mapSize = random.nextInt(5);
        for (int i = 0; i < mapSize; i++) {
          map.put(randomString(random.nextInt(10) + 1), getRandomFirestoreType(depth + 1));
        }
        return map;
      }
      case 9 -> {
        return new GeoPoint(random.nextDouble() * 180 - 90, random.nextDouble() * 360 - 180);
      }
      case 10 -> {
        return sourceFirestoreResourceManager
            .getFirestore()
            .collection("refCol" + random.nextInt(5))
            .document("refDoc" + random.nextInt(5));
      }
      default -> {
        return randomString(10);
      }
    }
  }

  private String randomString(int length) {
    String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_./ ~";
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(characters.charAt(random.nextInt(characters.length())));
    }
    return sb.toString();
  }

  private ByteString randomByteString(int length) {
    byte[] array = new byte[length];
    random.nextBytes(array);
    return ByteString.copyFrom(array);
  }
}
