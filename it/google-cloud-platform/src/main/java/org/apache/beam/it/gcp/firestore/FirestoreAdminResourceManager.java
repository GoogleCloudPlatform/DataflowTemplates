package org.apache.beam.it.gcp.firestore;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auth.Credentials;
import com.google.cloud.firestore.v1.FirestoreAdminClient;
import com.google.common.base.Strings;
import com.google.firestore.admin.v1.CreateDatabaseRequest;
import com.google.firestore.admin.v1.Database;
import com.google.firestore.admin.v1.Database.DatabaseEdition;
import com.google.firestore.admin.v1.Database.DatabaseType;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.beam.it.common.ResourceManager;

public class FirestoreAdminResourceManager implements ResourceManager {

  private FirestoreAdminClient firestoreAdminClient;

  private final String projectId;
  private final String region;

  private final Set<String> databaseIds;

  private FirestoreAdminResourceManager(Builder builder) {
    try {
      this.firestoreAdminClient = FirestoreAdminClient.create();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create FirestoreAdminClient", e);
    }
    this.projectId = builder.projectId;
    this.region = builder.region;
    this.databaseIds = new HashSet<>();
  }

  @Override
  public void cleanupAll() {
    try {
      for (String databaseId : databaseIds) {
        deleteDatabase(databaseId);
      }
    } catch (Exception e) {
      throw new FirestoreAdminResourceManagerException("Error cleaning up Firestore resources", e);
    } finally {
      databaseIds.clear();
      try {
        firestoreAdminClient.close();
      } catch (Exception e) {
        throw new FirestoreAdminResourceManagerException("Error closing Firestore client", e);
      }
    }
  }

  public void createDatabase(String databaseId, DatabaseType type, DatabaseEdition edition) {
    try {
      firestoreAdminClient
          .createDatabaseAsync(
              CreateDatabaseRequest.newBuilder()
                  .setParent("projects/" + projectId)
                  .setDatabaseId(databaseId)
                  .setDatabase(
                      Database.newBuilder()
                          .setName(databaseId)
                          .setType(type)
                          .setDatabaseEdition(edition)
                          .setLocationId(region))
                  .build())
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new FirestoreAdminResourceManagerException("Error creating Firestore database", e);
    }
  }

  public void deleteDatabase(String databaseId) {
    try {
      firestoreAdminClient.deleteDatabaseAsync(databaseId).get();
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
      if (credentials == null) {
        throw new IllegalArgumentException("Credentials must be provided");
      }
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
