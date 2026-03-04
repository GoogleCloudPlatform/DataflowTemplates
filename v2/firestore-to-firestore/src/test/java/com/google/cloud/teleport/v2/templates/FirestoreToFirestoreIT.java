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
import static com.google.common.truth.Truth.assertWithMessage;
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
        // TODO: pacoavila - We need to use an ENTERPRISE db once the data access mode can be set
        // from the Java SDK.
        destinationDatabaseId(), DatabaseType.FIRESTORE_NATIVE, DatabaseEdition.STANDARD);
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

  // @Test
  // public void testFirestoreToFirestore_collectionIdProvided_copySingleCollection()
  //     throws IOException {
  //   String collectionId = "input-" + testName;
  //   int numDocuments = 10;
  //
  //   Map<String, Map<String, Object>> inputData = generateTestDocuments(numDocuments);
  //
  //   sourceFirestoreResourceManager.write(collectionId, inputData);
  //
  //   LaunchInfo info = launchPipeline(/*testName=*/ "copySingleCollection", collectionId);
  //   assertThatPipeline(info).isRunning();
  //
  //   Result result = pipelineOperator().waitUntilDone(createConfig(info, Duration.ofMinutes(10)));
  //   assertThatResult(result).isLaunchFinished();
  //
  //   List<QueryDocumentSnapshot> documents = destinationFirestoreResourceManager.read(collectionId);
  //   assertThat(documents).hasSize(numDocuments);
  //
  //   for (QueryDocumentSnapshot document : documents) {
  //     assertThat(document.getData()).containsEntry("name", "test-doc-" + document.get("id"));
  //   }
  // }
  //
  // @Test
  // public void testFirestoreToFirestore_collectionIdNotProvided_copyAllCollections()
  //     throws IOException {
  //   String collectionId1 = "inputA-" + testName;
  //   int numDocs1 = 5;
  //   Map<String, Map<String, Object>> inputData1 = generateTestDocuments(numDocs1);
  //   sourceFirestoreResourceManager.write(collectionId1, inputData1);
  //
  //   String collectionId2 = "inputB-" + testName;
  //   int numDocs2 = 5;
  //   Map<String, Map<String, Object>> inputData2 = generateTestDocuments(numDocs2);
  //   sourceFirestoreResourceManager.write(collectionId2, inputData2);
  //
  //   LaunchInfo info = launchPipeline(/*testName=*/ "copyAll", /*collectionIds=*/ "");
  //   assertThatPipeline(info).isRunning();
  //
  //   Result result = pipelineOperator().waitUntilDone(createConfig(info, Duration.ofMinutes(15)));
  //   assertThatResult(result).isLaunchFinished();
  //
  //   List<QueryDocumentSnapshot> documents1 =
  //       destinationFirestoreResourceManager.read(collectionId1);
  //   assertThat(documents1).hasSize(numDocs1);
  //   for (QueryDocumentSnapshot document : documents1) {
  //     assertThat(document.getData()).containsEntry("name", "test-doc-" + document.get("id"));
  //   }
  //
  //   List<QueryDocumentSnapshot> documents2 =
  //       destinationFirestoreResourceManager.read(collectionId2);
  //   assertThat(documents2).hasSize(numDocs2);
  //   for (QueryDocumentSnapshot document : documents2) {
  //     assertThat(document.getData()).containsEntry("name", "test-doc-" + document.get("id"));
  //   }
  // }
  //
  // private Map<String, Map<String, Object>> generateTestDocuments(int numDocuments) {
  //   Map<String, Map<String, Object>> testDocuments = new HashMap<>();
  //   for (int i = 1; i <= numDocuments; i++) {
  //     Map<String, Object> data = Map.of("id", i, "name", "test-doc-" + i);
  //     testDocuments.put("doc-" + i, data);
  //   }
  //   return testDocuments;
  // }

  @Test
  public void testFirestoreToFirestore_fuzzDataTypes() throws IOException {
    String rootCollectionId = "fuzz-" + testName;
    int numRootDocuments = 10;

    Map<String, Map<String, Map<String, Object>>> inputData = new HashMap<>();

    Map<String, Map<String, Object>> rootData = new HashMap<>();
    for (int i = 0; i < numRootDocuments; i++) {
      String documentId = "fuzzDocument-" + i + "-" + UUID.randomUUID();
      rootData.put(documentId, generateRandomDocument());

      // 30% chance to create a subcollection
      if (random.nextInt(10) < 3) {
        String subCollectionId = rootCollectionId + "/" + documentId + "/sub-" + UUID.randomUUID();
        Map<String, Map<String, Object>> subData = new HashMap<>();
        int numSubDocs = random.nextInt(3) + 1;
        for (int k = 0; k < numSubDocs; k++) {
          String subDocId = "subDoc-" + k + "-" + UUID.randomUUID();
          subData.put(subDocId, generateRandomDocument());

          // 20% chance to create a nested subcollection (sub-subcollection)
          if (random.nextInt(10) < 2) {
            String subSubCollectionId =
                subCollectionId + "/" + subDocId + "/subsub-" + UUID.randomUUID();
            Map<String, Map<String, Object>> subSubData = new HashMap<>();
            int numSubSubDocs = random.nextInt(2) + 1;
            for (int l = 0; l < numSubSubDocs; l++) {
              subSubData.put("subSubDoc-" + l + "-" + UUID.randomUUID(), generateRandomDocument());
            }
            inputData.put(subSubCollectionId, subSubData);
          }
        }
        inputData.put(subCollectionId, subData);
      }
    }
    inputData.put(rootCollectionId, rootData);

    // Write all data to source
    for (Map.Entry<String, Map<String, Map<String, Object>>> entry : inputData.entrySet()) {
      sourceFirestoreResourceManager.write(entry.getKey(), entry.getValue());
    }

    LaunchInfo info = launchPipeline(/*testName=*/ "copyFuzz", /*collectionIdFilter=*/ "");
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info, Duration.ofMinutes(15)));
    assertThatResult(result).isLaunchFinished();

    // Verify all collections
    for (Map.Entry<String, Map<String, Map<String, Object>>> entry : inputData.entrySet()) {
      String collId = entry.getKey();
      Map<String, Map<String, Object>> expectedCollData = entry.getValue();

      List<QueryDocumentSnapshot> destDocuments = destinationFirestoreResourceManager.read(collId);
      assertWithMessage("size for collection " + collId).that(destDocuments)
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

          if (expectedValue == null) {
            assertThat(actualValue).isNull();
          } else {
            assertThat(actualValue).isInstanceOf(expectedValue.getClass());
            assertThat(actualValue).isEqualTo(expectedValue);
          }
        }
      }
    }
  }


  private LaunchInfo launchPipeline(String testName, String collectionIdFilter)
      throws IOException {
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, SPEC_PATH)
            .addParameter("sourceProjectId", PROJECT)
            .addParameter("sourceDatabaseId", sourceDatabaseId())
            .addParameter("destinationProjectId", PROJECT)
            .addParameter("destinationDatabaseId", destinationDatabaseId())
            .addParameter("maxNumWorkers", "10");

    if (!collectionIdFilter.isEmpty()) {
      options.addParameter("collectionIds", collectionIdFilter);
    }

    return pipelineLauncher.launch(PROJECT, REGION, options.build());
  }

  private String sourceDatabaseId() {
    String sanitizedName = testName.toLowerCase().replaceAll("[^a-z0-9]", "-");
    if (sanitizedName.length() > 50) {
      sanitizedName = sanitizedName.substring(sanitizedName.length() - 50);
    }
    return ("s-" + sanitizedName).replaceAll("-+", "-").replaceAll("^-", "");
  }

  private String destinationDatabaseId() {
    String sanitizedName = testName.toLowerCase().replaceAll("[^a-z0-9]", "-");
    if (sanitizedName.length() > 50) {
      sanitizedName = sanitizedName.substring(sanitizedName.length() - 50);
    }
    return ("d-" + sanitizedName).replaceAll("-+", "-").replaceAll("^-", "");
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
