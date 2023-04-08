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

import com.google.cloud.storage.Notification;
import com.google.cloud.teleport.it.common.ResourceManager;
import java.io.IOException;
import java.nio.file.Path;

/** An interface for managing Google Cloud Storage resources. */
public interface GcsResourceManager extends ResourceManager {

  /**
   * Creates a new notification for the given topic and GCS prefix.
   *
   * @param topicName the name of the Pub/Sub topic to which the notification should be sent.
   * @param gcsPrefix the prefix of the object names to which the notification applies.
   * @return the created notification.
   */
  Notification createNotification(String topicName, String gcsPrefix);

  /**
   * Copies a file from a local path to a specified object name in Google Cloud Storage.
   *
   * @param localPath the path of the file to be copied.
   * @param objectName the name of the object to be created in Google Cloud Storage.
   * @return the URI of the copied object in Google Cloud Storage.
   * @throws IOException if there is an error reading the file at the specified local path.
   */
  String copyFileToGcs(Path localPath, String objectName) throws IOException;

  /**
   * Cleans up all resources created by the manager instance, including notifications and objects in
   * Google Cloud Storage.
   */
  void cleanupAll();

  static Builder builder() {
    return new Builder();
  }

  /** A builder class for creating instances of {@link GcsResourceManager}. */
  class Builder {
    private String project;
    private String bucket;

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
      return new DefaultGcsResourceManager(project, bucket);
    }
  }
}
