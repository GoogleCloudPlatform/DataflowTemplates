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
package com.google.cloud.teleport.it.gcp.storage;

import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Notification;
import com.google.cloud.storage.NotificationInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.teleport.it.common.ResourceManager;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client for managing Google Cloud Storage resources. */
public class GcsResourceManager implements ResourceManager {
  private static final Logger LOG = LoggerFactory.getLogger(GcsResourceManager.class);

  private final String project;
  private final String bucket;
  private final Credentials credentials;

  GcsResourceManager(String project, String bucket, Credentials credentials) {
    this.project = project;
    this.bucket = bucket;
    this.credentials = credentials;
  }

  private static final List<Notification> NOTIFICATION_LIST = new ArrayList<>();
  private static final List<BlobId> GCS_BLOBS = new ArrayList<>();
  private static final List<String> MANAGED_TEMP_DIRS = new ArrayList<>();

  Storage getStorageClient() {
    StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(project);
    if (credentials != null) {
      builder = builder.setCredentials(credentials);
    }
    return builder.build().getService();
  }

  /**
   * Creates a new notification for the given topic and GCS prefix.
   *
   * @param topicName the name of the Pub/Sub topic to which the notification should be sent.
   * @param gcsPrefix the prefix of the object names to which the notification applies.
   * @return the created notification.
   */
  public Notification createNotification(String topicName, String gcsPrefix) {
    Storage storage = getStorageClient();
    NotificationInfo notificationInfo =
        NotificationInfo.newBuilder(topicName)
            .setEventTypes(NotificationInfo.EventType.OBJECT_FINALIZE)
            .setObjectNamePrefix(gcsPrefix)
            .setPayloadFormat(NotificationInfo.PayloadFormat.JSON_API_V1)
            .build();
    try {
      Notification notification = storage.createNotification(bucket, notificationInfo);
      LOG.info("Successfully created notification {}", notification);
      NOTIFICATION_LIST.add(notification);
      return notification;
    } catch (StorageException e) {
      throw new RuntimeException(
          String.format(
              "Unable to create notification for bucket %s. Notification: %s",
              bucket, notificationInfo),
          e);
    }
  }

  /**
   * Copies a file from a local path to a specified object name in Google Cloud Storage.
   *
   * @param localPath the path of the file to be copied.
   * @param objectName the name of the object to be created in Google Cloud Storage.
   * @return the URI of the copied object in Google Cloud Storage.
   * @throws IOException if there is an error reading the file at the specified local path.
   */
  public String copyFileToGcs(Path localPath, String objectName) throws IOException {
    Storage storage = getStorageClient();

    BlobId blobId = BlobId.of(bucket, objectName);
    LOG.info("Copying file {} into {}", localPath, blobId);
    Blob resultBlob =
        storage.create(BlobInfo.newBuilder(blobId).build(), Files.readAllBytes(localPath));
    GCS_BLOBS.add(resultBlob.getBlobId());
    return resultBlob.getBlobId().toGsUtilUri();
  }

  /**
   * Register a temporary directory that will be cleaned up after test.
   *
   * @param dirName name of the temporary directory
   */
  public void registerTempDir(String dirName) {
    MANAGED_TEMP_DIRS.add(dirName);
  }

  /**
   * Cleans up all resources created by the manager instance, including notifications and objects in
   * Google Cloud Storage.
   */
  @Override
  public void cleanupAll() {
    if (GCS_BLOBS.size() > 0) {
      Storage storage = getStorageClient();
      storage.delete(GCS_BLOBS);
    }

    if (MANAGED_TEMP_DIRS.size() > 0) {
      Storage storage = getStorageClient();
      StorageBatch batch = storage.batch();
      boolean needCleanup = false;
      for (String tempDir : MANAGED_TEMP_DIRS) {
        Page<Blob> blobs = storage.list(bucket, Storage.BlobListOption.prefix(tempDir));
        for (Blob blob : blobs.iterateAll()) {
          batch.delete(blob.getBlobId());
          needCleanup = true;
        }
      }
      if (needCleanup) {
        batch.submit();
      }
    }

    if (NOTIFICATION_LIST.size() > 0) {
      Storage storage = getStorageClient();
      for (Notification notification : NOTIFICATION_LIST) {
        storage.deleteNotification(bucket, notification.getNotificationId());
      }
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  /** A builder class for creating instances of {@link GcsResourceManager}. */
  public static class Builder {
    private String project;
    private String bucket;
    private Credentials credentials;

    /**
     * Sets the GCP project for the builder.
     *
     * @param project the GCP project ID.
     * @return the builder instance.
     */
    public Builder setProject(String project) {
      this.project = project;
      return this;
    }

    /**
     * Sets the GCS bucket for the builder.
     *
     * @param bucket the name of the GCS bucket.
     * @return the builder instance.
     */
    public Builder setBucket(String bucket) {
      this.bucket = bucket;
      return this;
    }

    public Builder setCredentials(Credentials credentials) {
      this.credentials = credentials;
      return this;
    }

    /**
     * Builds a new instance of {@link GcsResourceManager} with the specified project and bucket.
     *
     * @return a new instance of {@link GcsResourceManager}.
     * @throws IllegalArgumentException if either project or bucket is not set.
     */
    public GcsResourceManager build() {
      if (project == null) {
        throw new IllegalArgumentException(
            "A GCP project must be provided to build a GCS resource manager.");
      }
      if (bucket == null) {
        throw new IllegalArgumentException(
            "A GCS bucket must be provided to build a GCS resource manager.");
      }
      return new GcsResourceManager(project, bucket, credentials);
    }
  }
}
