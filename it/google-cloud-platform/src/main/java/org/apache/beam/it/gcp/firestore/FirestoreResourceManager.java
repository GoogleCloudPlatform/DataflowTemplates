/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.it.gcp.firestore;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.core.ApiFuture;
import com.google.auth.Credentials;
import com.google.cloud.firestore.AggregateField;
import com.google.cloud.firestore.AggregateQuerySnapshot;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.WriteBatch;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.beam.it.common.ResourceManager;

/** Default implementation of {@link FirestoreResourceManager}. */
public class FirestoreResourceManager implements ResourceManager {

  private final String projectId;
  private final String databaseId;
  private final Firestore firestore;
  private final Set<String> collectionIds;

  private FirestoreResourceManager(Builder builder) {
    this.projectId = builder.projectId;
    this.databaseId = builder.databaseId;
    this.collectionIds = new HashSet<>();
    this.firestore =
        FirestoreOptions.newBuilder()
            .setCredentials(builder.credentials)
            .setProjectId(projectId)
            .setDatabaseId(databaseId)
            .build()
            .getService();
  }

  @VisibleForTesting
  FirestoreResourceManager(Firestore firestore) {
    this.projectId = firestore.getOptions().getProjectId();
    this.databaseId = firestore.getOptions().getDatabaseId();
    this.collectionIds = new HashSet<>();
    this.firestore = firestore;
  }

  public static Builder builder(String testId) {
    checkArgument(!Strings.isNullOrEmpty(testId), "testId can not be empty");
    return new Builder(testId);
  }

  public void write(String collectionName, Map<String, Map<String, Object>> documents) {
    collectionIds.add(collectionName);
    WriteBatch batch = firestore.batch();

    for (Map.Entry<String, Map<String, Object>> entry : documents.entrySet()) {
      DocumentReference docRef = firestore.collection(collectionName).document(entry.getKey());
      batch.set(docRef, entry.getValue());
    }

    try {
      batch.commit().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new FirestoreResourceManagerException("Error writing documents to Firestore", e);
    }
  }

  public List<QueryDocumentSnapshot> read(String collectionName) {
    try {
      ApiFuture<QuerySnapshot> query = firestore.collection(collectionName).get();
      return query.get().getDocuments();
    } catch (InterruptedException | ExecutionException e) {
      throw new FirestoreResourceManagerException("Error reading documents from Firestore", e);
    }
  }

  public long readDocCount(String collectionName) {
    try {
      AggregateField countField = AggregateField.count();
      ApiFuture<AggregateQuerySnapshot> query = firestore.collection(collectionName).aggregate(countField).get();
      return query.get().getLong(countField);
    } catch (InterruptedException | ExecutionException e) {
      throw new FirestoreResourceManagerException("Error reading the document count from Firestore", e);
    }
  }

  @Override
  public void cleanupAll() {
    try {
      for (String collectionName : collectionIds) {
        deleteCollection(collectionName);
      }
    } catch (Exception e) {
      throw new FirestoreResourceManagerException("Error cleaning up Firestore resources", e);
    } finally {
      collectionIds.clear();
      try {
        firestore.close();
      } catch (Exception e) {
        throw new FirestoreResourceManagerException("Error closing Firestore client", e);
      }
    }
  }

  private void deleteCollection(String collectionName)
      throws ExecutionException, InterruptedException {
    CollectionReference collection = firestore.collection(collectionName);
    int batchSize = 100;
    ApiFuture<QuerySnapshot> future = collection.limit(batchSize).get();
    List<QueryDocumentSnapshot> documents = future.get().getDocuments();

    while (!documents.isEmpty()) {
      WriteBatch batch = firestore.batch();
      for (QueryDocumentSnapshot document : documents) {
        batch.delete(document.getReference());
      }
      batch.commit().get();
      future = collection.limit(batchSize).get();
      documents = future.get().getDocuments();
    }
  }

  /** Builder for {@link FirestoreResourceManager}. */
  public static final class Builder {

    private final String testId;
    private String projectId;
    private String databaseId;
    private Credentials credentials;

    private Builder(String testId) {
      this.testId = testId;
    }

    public Builder setProject(String projectId) {
      this.projectId = projectId;
      return this;
    }

    public Builder setDatabase(String databaseId) {
      this.databaseId = databaseId;
      return this;
    }

    public Builder setCredentials(Credentials credentials) {
      this.credentials = credentials;
      return this;
    }

    public FirestoreResourceManager build() {
      if (credentials == null) {
        throw new IllegalArgumentException("Credentials must be provided");
      }
      if (projectId == null) {
        throw new IllegalArgumentException("Project ID must be provided");
      }
      if (databaseId == null) {
        throw new IllegalArgumentException("Database ID must be provided");
      }
      return new FirestoreResourceManager(this);
    }
  }
}
