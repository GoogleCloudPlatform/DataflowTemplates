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

import com.google.auth.Credentials;
import com.google.cloud.firestore.v1.FirestoreAdminClient;
import com.google.cloud.firestore.v1.FirestoreAdminSettings;
import com.google.common.base.Strings;
import com.google.firestore.admin.v1.CreateDatabaseRequest;
import com.google.firestore.admin.v1.Database;
import com.google.firestore.admin.v1.Database.DatabaseEdition;
import com.google.firestore.admin.v1.Database.DatabaseType;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.beam.it.common.ResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FirestoreAdminResourceManager implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(FirestoreAdminResourceManager.class);
  private final FirestoreAdminClient firestoreAdminClient;

  private final String projectId;
  private final String region;

  private final Set<String> databaseIds;

  private FirestoreAdminResourceManager(Builder builder) {
    try {
      FirestoreAdminSettings.Builder settingsBuilder = FirestoreAdminSettings.newBuilder();
      if (builder.credentials != null) {
        settingsBuilder.setCredentialsProvider(() -> builder.credentials);
      }
      this.firestoreAdminClient = FirestoreAdminClient.create(settingsBuilder.build());
    } catch (Exception e) {
      throw new RuntimeException("Failed to create FirestoreAdminClient", e);
    }
    this.projectId = builder.projectId;
    this.region = builder.region;
    this.databaseIds = new HashSet<>();
  }

  @Override
  public void cleanupAll() {
    LOG.info("Cleaning up {} Firestore databases...", databaseIds.size());
    boolean hasError = false;
    for (String databaseId : databaseIds) {
      try {
        LOG.info("Deleting Firestore database: {}", databaseId);
        deleteDatabase(databaseId);
      } catch (Exception e) {
        LOG.error("Failed to delete Firestore database {}: {}", databaseId, e.getMessage(), e);
        hasError = true;
      }
    }
    databaseIds.clear();
    try {
      firestoreAdminClient.close();
    } catch (Exception e) {
      LOG.error("Error closing Firestore client", e);
      hasError = true;
    }
    if (hasError) {
      throw new FirestoreAdminResourceManagerException(
          "Error cleaning up Firestore resources. Check logs for details.");
    }
  }

  public void createDatabase(String databaseId, DatabaseType type, DatabaseEdition edition) {
    createDatabase(
        databaseId,
        Database.newBuilder()
            .setName(databaseId)
            .setType(type)
            .setDatabaseEdition(edition)
            .setLocationId(region)
            .build());
  }

  public void createDatabase(String databaseId, Database database) {
    try {
      LOG.info("Creating Firestore database: {}", databaseId);
      firestoreAdminClient
          .createDatabaseAsync(
              CreateDatabaseRequest.newBuilder()
                  .setParent("projects/" + projectId)
                  .setDatabaseId(databaseId)
                  .setDatabase(database)
                  .build())
          .get();
      databaseIds.add(databaseId);
    } catch (InterruptedException | ExecutionException e) {
      throw new FirestoreAdminResourceManagerException("Error creating Firestore database", e);
    }
  }

  public void deleteDatabase(String databaseId) {
    try {
      firestoreAdminClient
          .deleteDatabaseAsync("projects/" + projectId + "/databases/" + databaseId)
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new FirestoreAdminResourceManagerException("Error deleting Firestore database", e);
    }
  }

  public static FirestoreAdminResourceManager.Builder builder(String testId) {
    checkArgument(!Strings.isNullOrEmpty(testId), "testId can not be empty");
    return new FirestoreAdminResourceManager.Builder(testId);
  }

  /** Builder for {@link FirestoreAdminResourceManager}. */
  public static final class Builder {

    private final String testId;
    private String projectId;
    private String region;
    private Credentials credentials;

    private Builder(String testId) {
      this.testId = testId;
    }

    public FirestoreAdminResourceManager.Builder setProject(String projectId) {
      this.projectId = projectId;
      return this;
    }

    public FirestoreAdminResourceManager.Builder setRegion(String region) {
      this.region = region;
      return this;
    }

    public FirestoreAdminResourceManager.Builder setCredentials(Credentials credentials) {
      this.credentials = credentials;
      return this;
    }

    public FirestoreAdminResourceManager build() {
      if (projectId == null) {
        throw new IllegalArgumentException("Project ID must be provided");
      }
      if (region == null) {
        throw new IllegalArgumentException("Region must be provided");
      }
      return new FirestoreAdminResourceManager(this);
    }
  }
}
