/*
 * Copyright (C) 2026 Google LLC
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
import static com.google.common.truth.Truth.assertWithMessage;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.GeoPoint;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.firestore.admin.v1.Database;
import com.google.firestore.admin.v1.Database.DataAccessMode;
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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link FirestoreToFirestore}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(FirestoreToFirestore.class)
@RunWith(JUnit4.class)
public final class FirestoreToFirestoreIT extends TemplateTestBase {

  private static final String PROJECT = TestProperties.project();
  private static final String REGION = TestProperties.region();
  private static final String SUB_COLLECTION_GROUP_ID = "subCol";
  private static final String SUB_SUB_COLLECTION_GROUP_ID = "subSubCol";

  private FirestoreAdminResourceManager firestoreAdminResourceManager;

  private FirestoreResourceManager sourceFirestoreResourceManager;

  private FirestoreResourceManager destinationFirestoreResourceManager;

  private FirestoreResourceManager defaultSourceDatabaseFirestoreResourceManager;

  private Random random;
  private String sourceDatabaseId;
  private String destinationDatabaseId;

  @Before
  public void setUp() {
    random = new Random();
    sourceDatabaseId = "src-" + UUID.randomUUID();
    destinationDatabaseId = "dst-" + UUID.randomUUID();

    firestoreAdminResourceManager =
        FirestoreAdminResourceManager.builder(testName)
            .setProject(PROJECT)
            .setRegion(REGION)
            .setCredentials(TestProperties.googleCredentials())
            .build();
    firestoreAdminResourceManager.createDatabase(
        sourceDatabaseId, DatabaseType.FIRESTORE_NATIVE, DatabaseEdition.STANDARD);
    firestoreAdminResourceManager.createDatabase(
        destinationDatabaseId,
        Database.newBuilder()
            .setName(destinationDatabaseId)
            .setType(DatabaseType.FIRESTORE_NATIVE)
            .setDatabaseEdition(DatabaseEdition.ENTERPRISE)
            .setFirestoreDataAccessMode(DataAccessMode.DATA_ACCESS_MODE_ENABLED)
            .setLocationId(REGION)
            .build());
    sourceFirestoreResourceManager =
        FirestoreResourceManager.builder(testName)
            .setProject(PROJECT)
            .setDatabase(sourceDatabaseId)
            .setCredentials(TestProperties.googleCredentials())
            .build();
    destinationFirestoreResourceManager =
        FirestoreResourceManager.builder(testName)
            .setProject(PROJECT)
            .setDatabase(destinationDatabaseId)
            .setCredentials(TestProperties.googleCredentials())
            .build();
    defaultSourceDatabaseFirestoreResourceManager =
        FirestoreResourceManager.builder(testName)
            .setProject(PROJECT)
            .setDatabase("(default)")
            .setCredentials(TestProperties.googleCredentials())
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(sourceFirestoreResourceManager);
    ResourceManagerUtils.cleanResources(destinationFirestoreResourceManager);
    ResourceManagerUtils.cleanResources(defaultSourceDatabaseFirestoreResourceManager);
    ResourceManagerUtils.cleanResources(firestoreAdminResourceManager);
  }

  @Test
  public void testFirestoreToFirestore_copiesOnlySelectedCollections() throws IOException {
    String collectionId1 = "input1-" + randomString(6).toLowerCase();
    String collectionId2 = "input2-" + randomString(6).toLowerCase();
    String collectionId3 = "input3-" + randomString(6).toLowerCase();
    int numDocuments = 10;

    Map<String, Map<String, Object>> inputData1 = generateTestDocuments(numDocuments);
    Map<String, Map<String, Object>> inputData2 = generateTestDocuments(numDocuments);
    Map<String, Map<String, Object>> inputData3 = generateTestDocuments(numDocuments);
    Map<String, Map<String, Object>> subData = generateTestDocuments(numDocuments);

    sourceFirestoreResourceManager.write(collectionId1, inputData1);
    sourceFirestoreResourceManager.write(collectionId2, inputData2);
    sourceFirestoreResourceManager.write(collectionId3, inputData3);

    // Add a subcollection to one of the documents in collection 2
    String docId2 = inputData2.keySet().iterator().next();
    String subCollectionPath2 = collectionId2 + "/" + docId2 + "/" + SUB_COLLECTION_GROUP_ID;
    sourceFirestoreResourceManager.write(subCollectionPath2, subData);

    // Add a subcollection to one of the documents in collection 3
    String docId3 = inputData3.keySet().iterator().next();
    String subCollectionPath3 = collectionId3 + "/" + docId3 + "/" + SUB_COLLECTION_GROUP_ID;
    sourceFirestoreResourceManager.write(subCollectionPath3, subData);

    String collectionGroupIds = collectionId1 + "," + SUB_COLLECTION_GROUP_ID;

    LaunchInfo info = launchPipeline(/* testName= */ "copyFiltered", collectionGroupIds);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info, Duration.ofMinutes(20)));
    assertThatResult(result).isLaunchFinished();

    // Verify collection 1
    List<QueryDocumentSnapshot> documents1 =
        destinationFirestoreResourceManager.read(collectionId1);
    assertThat(documents1).hasSize(numDocuments);

    // Verify subcollection under collection 2
    List<QueryDocumentSnapshot> subDocuments2 =
        destinationFirestoreResourceManager.read(subCollectionPath2);
    assertThat(subDocuments2).hasSize(numDocuments);

    // Verify subcollection under collection 3
    List<QueryDocumentSnapshot> subDocuments3 =
        destinationFirestoreResourceManager.read(subCollectionPath3);
    assertThat(subDocuments3).hasSize(numDocuments);

    // Verify collection 2 (should be empty because not in filter)
    List<QueryDocumentSnapshot> documents2 =
        destinationFirestoreResourceManager.read(collectionId2);
    assertThat(documents2).isEmpty();

    // Verify collection 3 (should be empty because not in filter)
    List<QueryDocumentSnapshot> documents3 =
        destinationFirestoreResourceManager.read(collectionId3);
    assertThat(documents3).isEmpty();
  }

  @Test
  public void testFirestoreToFirestore_withReadTime() throws IOException, InterruptedException {
    String collectionId = "readTime-" + randomString(6).toLowerCase();
    int numInitialDocuments = 5;
    int numLaterDocuments = 5;

    // 1. Write initial documents
    Map<String, Map<String, Object>> initialData = generateTestDocuments(numInitialDocuments);
    sourceFirestoreResourceManager.write(collectionId, initialData);

    // 2. Capture readTime
    // We wait a bit to ensure clear separation in Firestore's commit timestamps
    Thread.sleep(2000);
    String readTime = java.time.Instant.now().toString();
    Thread.sleep(2000);

    // 3. Write more documents after readTime
    Map<String, Map<String, Object>> laterData = new HashMap<>();
    for (int i = 1; i <= numLaterDocuments; i++) {
      Map<String, Object> data = Map.of("id", i + numInitialDocuments, "name", "later-doc-" + i);
      laterData.put("later-doc-" + i, data);
    }
    sourceFirestoreResourceManager.write(collectionId, laterData);

    // 4. Launch pipeline with readTime
    LaunchInfo info = launchPipeline(/* testName= */ "copyWithReadTime", collectionId, readTime);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info, Duration.ofMinutes(20)));
    assertThatResult(result).isLaunchFinished();

    // 5. Verify only initial documents were copied
    List<QueryDocumentSnapshot> documents = destinationFirestoreResourceManager.read(collectionId);
    assertThat(documents).hasSize(numInitialDocuments);

    for (QueryDocumentSnapshot document : documents) {
      assertThat(document.getId()).startsWith("doc-");
      assertThat(document.get("name").toString()).startsWith("test-doc-");
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
  public void testFirestoreToFirestore_fuzzDataTypes() throws Exception {
    // Populate data for two different top-level collections
    Map<String, Map<String, Map<String, Object>>> inputData = new HashMap<>();
    String rootCollectionId1 = "fuzz1-" + randomString(6).toLowerCase();
    String rootCollectionId2 = "fuzz2-" + randomString(6).toLowerCase();
    populateFuzzData(inputData, rootCollectionId1);
    populateFuzzData(inputData, rootCollectionId2);

    // Write all data to source
    for (Map.Entry<String, Map<String, Map<String, Object>>> entry : inputData.entrySet()) {
      sourceFirestoreResourceManager.write(entry.getKey(), entry.getValue());
    }

    // Launch without collection ids flag to test that all collection groups are included by default
    LaunchInfo info = launchPipeline(/* testName= */ "copyFuzz");
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info, Duration.ofMinutes(20)));
    assertThatResult(result).isLaunchFinished();

    // Verify all collections
    for (Map.Entry<String, Map<String, Map<String, Object>>> entry : inputData.entrySet()) {
      String collId = entry.getKey();
      Map<String, Map<String, Object>> expectedCollData = entry.getValue();

      List<QueryDocumentSnapshot> destDocuments = destinationFirestoreResourceManager.read(collId);
      assertWithMessage("size for collection " + collId)
          .that(destDocuments)
          .hasSize(expectedCollData.size());

      for (QueryDocumentSnapshot destDoc : destDocuments) {
        Map<String, Object> expectedDocData = expectedCollData.get(destDoc.getId());
        assertThat(expectedDocData).isNotNull();

        Map<String, Object> destDocData = destDoc.getData();
        assertThat(destDocData.size()).isEqualTo(expectedDocData.size());

        for (Map.Entry<String, Object> docEntry : expectedDocData.entrySet()) {
          String key = docEntry.getKey();
          Object expectedValue = docEntry.getValue();
          Object actualValue = destDocData.get(key);

          assertValuesEqual(expectedValue, actualValue);
        }
      }
    }
  }

  @Ignore("Integration test project has a default database in Datastore mode.")
  @Test
  public void testFirestoreToFirestore_withDefaultSourceDatabase() throws IOException {
    String collectionId = "inputDef-" + randomString(6).toLowerCase();
    int numDocuments = 10;
    Map<String, Map<String, Object>> inputData = generateTestDocuments(numDocuments);

    // Populates data directly into the existing (default) database
    defaultSourceDatabaseFirestoreResourceManager.write(collectionId, inputData);

    // Act
    LaunchInfo info =
        launchPipelineWithSourceDatabase(/* testName= */ "copyFromDefault", "", "", "(default)");
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info, Duration.ofMinutes(20)));
    assertThatResult(result).isLaunchFinished();

    // Verify it successfully copied to destination database
    List<QueryDocumentSnapshot> destDocuments =
        destinationFirestoreResourceManager.read(collectionId);
    assertThat(destDocuments).hasSize(numDocuments);
  }

  private LaunchInfo launchPipeline(String testName) throws IOException {
    return launchPipeline(testName, /* collectionGroupIds= */ "");
  }

  private LaunchInfo launchPipeline(String testName, String collectionGroupIds) throws IOException {
    return launchPipeline(testName, collectionGroupIds, /* readTime= */ "");
  }

  private LaunchInfo launchPipeline(String testName, String collectionGroupIds, String readTime)
      throws IOException {
    return launchPipelineWithSourceDatabase(
        testName, collectionGroupIds, readTime, sourceDatabaseId);
  }

  private LaunchInfo launchPipelineWithSourceDatabase(
      String testName, String collectionGroupIds, String readTime, String inputSourceDatabaseId)
      throws IOException {
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("sourceProjectId", PROJECT)
            .addParameter("sourceDatabaseId", inputSourceDatabaseId)
            .addParameter("destinationProjectId", PROJECT)
            .addParameter("destinationDatabaseId", destinationDatabaseId)
            .addParameter("maxNumWorkers", "10");

    if (collectionGroupIds != null && !collectionGroupIds.isEmpty()) {
      options.addParameter("collectionGroupIds", collectionGroupIds);
    }
    if (readTime != null && !readTime.isEmpty()) {
      options.addParameter("readTime", readTime);
    }

    return pipelineLauncher.launch(PROJECT, REGION, options.build());
  }

  private void populateFuzzData(
      Map<String, Map<String, Map<String, Object>>> inputData, String rootCollectionId) {
    int numRootDocuments = 100;
    Map<String, Map<String, Object>> rootData = new HashMap<>();
    for (int i = 0; i < numRootDocuments; i++) {
      String documentId = "fuzzDocument-" + i + "-" + UUID.randomUUID();
      rootData.put(documentId, generateRandomDocument());

      // Ensure at least one subcollection exists and then 30% chance for others
      if (i == 0 || random.nextInt(10) < 3) {
        String subCollectionPath =
            rootCollectionId + "/" + documentId + "/" + SUB_COLLECTION_GROUP_ID;
        Map<String, Map<String, Object>> subData = new HashMap<>();
        int numSubDocs = random.nextInt(3) + 1;
        for (int j = 0; j < numSubDocs; j++) {
          String subDocId = "subDoc-" + j + "-" + UUID.randomUUID();
          subData.put(subDocId, generateRandomDocument());

          // Ensure at least one nested subcollection exists and then 20% chance for others
          if (j == 0 || random.nextInt(10) < 2) {
            String subSubCollectionPath =
                subCollectionPath + "/" + subDocId + "/" + SUB_SUB_COLLECTION_GROUP_ID;
            Map<String, Map<String, Object>> subSubData = new HashMap<>();
            int numSubSubDocs = random.nextInt(2) + 1;
            for (int k = 0; k < numSubSubDocs; k++) {
              subSubData.put("subSubDoc-" + k + "-" + UUID.randomUUID(), generateRandomDocument());
            }
            inputData.put(subSubCollectionPath, subSubData);
          }
        }
        inputData.put(subCollectionPath, subData);
      }
    }
    inputData.put(rootCollectionId, rootData);
  }

  private void assertValuesEqual(Object expectedValue, Object actualValue) {
    if (expectedValue == null) {
      assertThat(actualValue).isNull();
    } else if (expectedValue instanceof DocumentReference) {
      // Only compare the path of DocumentReference since the plain equality check will
      // fail since they're in different databases.
      assertThat(((DocumentReference) actualValue).getPath())
          .isEqualTo(((DocumentReference) expectedValue).getPath());
    } else if (expectedValue instanceof List) {
      // Lists and Maps must have each element compared since plain equals check will fail.
      assertThat(actualValue).isInstanceOf(List.class);
      List<?> expectedList = (List<?>) expectedValue;
      List<?> actualList = (List<?>) actualValue;
      assertThat(actualList).hasSize(expectedList.size());

      for (int i = 0; i < expectedList.size(); i++) {
        assertValuesEqual(expectedList.get(i), actualList.get(i));
      }
    } else if (expectedValue instanceof Map) {
      assertThat(actualValue).isInstanceOf(Map.class);
      Map<?, ?> expectedMap = (Map<?, ?>) expectedValue;
      Map<?, ?> actualMap = (Map<?, ?>) actualValue;
      assertThat(actualMap).hasSize(expectedMap.size());
      for (Object key : expectedMap.keySet()) {
        assertThat(actualMap).containsKey(key);
        assertValuesEqual(expectedMap.get(key), actualMap.get(key));
      }
    } else {
      // Not a special type. Plain equals will suffice.
      assertThat(actualValue).isEqualTo(expectedValue);
    }
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
      documentData.put("field with spaces", random.nextLong());
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
        // DocumentReference type.
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
    String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
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
