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
package com.google.cloud.teleport.it.storage;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Notification;
import com.google.cloud.storage.NotificationInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default implementation for {@link GcsResourceManager}. */
public class DefaultGcsResourceManager implements GcsResourceManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultGcsResourceManager.class);

  private final String project;
  private final String bucket;

  DefaultGcsResourceManager(String project, String bucket) {
    this.project = project;
    this.bucket = bucket;
  }

  private static final List<Notification> NOTIFICATION_LIST = new ArrayList<>();
  private static final List<BlobId> GCS_BLOBS = new ArrayList<>();

  Storage getStorageClient() {
    return StorageOptions.newBuilder().setProjectId(project).build().getService();
  }

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

  public String copyFileToGcs(Path localPath, String objectName) throws IOException {
    Storage storage = getStorageClient();

    BlobId blobId = BlobId.of(bucket, objectName);
    LOG.info("Copying file {} into {}", localPath, blobId);
    Blob resultBlob =
        storage.create(BlobInfo.newBuilder(blobId).build(), Files.readAllBytes(localPath));
    GCS_BLOBS.add(resultBlob.getBlobId());
    return resultBlob.getBlobId().toGsUtilUri();
  }

  @Override
  public void cleanupAll() {
    if (GCS_BLOBS.size() > 0) {
      Storage storage = getStorageClient();
      storage.delete(GCS_BLOBS);
    }

    if (NOTIFICATION_LIST.size() > 0) {
      Storage storage = getStorageClient();
      for (Notification notification : NOTIFICATION_LIST) {
        storage.deleteNotification(bucket, notification.getNotificationId());
      }
    }
  }
}
