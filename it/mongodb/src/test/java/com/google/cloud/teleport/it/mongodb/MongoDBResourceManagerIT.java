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
package com.google.cloud.teleport.it.mongodb;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.testcontainers.TestContainersIntegrationTest;
import com.mongodb.client.FindIterable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link MongoDBResourceManager}. */
@Category(TestContainersIntegrationTest.class)
@RunWith(JUnit4.class)
public class MongoDBResourceManagerIT {

  public static final String COLLECTION_NAME = "dummy-collection";
  private MongoDBResourceManager mongoResourceManager;

  @Before
  public void setUp() {
    mongoResourceManager = MongoDBResourceManager.builder("dummy").build();
  }

  @Test
  public void testResourceManagerE2E() {
    boolean createIndex = mongoResourceManager.createCollection(COLLECTION_NAME);
    assertThat(createIndex).isTrue();

    List<Document> documents = new ArrayList<>();
    documents.add(new Document(Map.of("id", 1, "company", "Google")));
    documents.add(new Document(Map.of("id", 2, "company", "Alphabet")));

    boolean insertDocuments = mongoResourceManager.insertDocuments(COLLECTION_NAME, documents);
    assertThat(insertDocuments).isTrue();

    FindIterable<Document> got = mongoResourceManager.readCollection(COLLECTION_NAME);

    List<Document> fetchRecords =
        StreamSupport.stream(got.spliterator(), false).collect(Collectors.toList());

    assertThat(fetchRecords).hasSize(2);
    assertThat(fetchRecords).containsExactlyElementsIn(documents);
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(mongoResourceManager);
  }
}
