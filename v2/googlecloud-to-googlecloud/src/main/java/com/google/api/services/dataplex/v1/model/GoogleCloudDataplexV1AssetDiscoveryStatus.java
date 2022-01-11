/*
 * Copyright (C) 2021 Google LLC
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
package com.google.api.services.dataplex.v1.model;

/**
 * Status of discovery for an asset.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1AssetDiscoveryStatus
    extends com.google.api.client.json.GenericJson {

  /**
   * The duration of the last run of the discovery job of the asset. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private String lastRunDuration;

  /** The time when the last discovery job started. The value may be {@code null}. */
  @com.google.api.client.util.Key private String lastRunTime;

  /**
   * Timestamp of the latest change to data that has been processed. This is only valid when
   * discovery is in PROCESSING_CHANGES state. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private String latestProcessedChangeTime;

  /** Additional information about the current state. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String message;

  /** The time when the next scheduled discovery job will start. The value may be {@code null}. */
  @com.google.api.client.util.Key private String nextRunTime;

  /** The current status of the discovery feature. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String state;

  /** Data Stats of the asset reported by discovery. The value may be {@code null}. */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1AssetDiscoveryStatusStats stats;

  /** Last update time of the status. The value may be {@code null}. */
  @com.google.api.client.util.Key private String updateTime;

  /**
   * The duration of the last run of the discovery job of the asset.
   *
   * @return value or {@code null} for none
   */
  public String getLastRunDuration() {
    return lastRunDuration;
  }

  /**
   * The duration of the last run of the discovery job of the asset.
   *
   * @param lastRunDuration lastRunDuration or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoveryStatus setLastRunDuration(String lastRunDuration) {
    this.lastRunDuration = lastRunDuration;
    return this;
  }

  /**
   * The time when the last discovery job started.
   *
   * @return value or {@code null} for none
   */
  public String getLastRunTime() {
    return lastRunTime;
  }

  /**
   * The time when the last discovery job started.
   *
   * @param lastRunTime lastRunTime or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoveryStatus setLastRunTime(String lastRunTime) {
    this.lastRunTime = lastRunTime;
    return this;
  }

  /**
   * Timestamp of the latest change to data that has been processed. This is only valid when
   * discovery is in PROCESSING_CHANGES state.
   *
   * @return value or {@code null} for none
   */
  public String getLatestProcessedChangeTime() {
    return latestProcessedChangeTime;
  }

  /**
   * Timestamp of the latest change to data that has been processed. This is only valid when
   * discovery is in PROCESSING_CHANGES state.
   *
   * @param latestProcessedChangeTime latestProcessedChangeTime or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoveryStatus setLatestProcessedChangeTime(
      String latestProcessedChangeTime) {
    this.latestProcessedChangeTime = latestProcessedChangeTime;
    return this;
  }

  /**
   * Additional information about the current state.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getMessage() {
    return message;
  }

  /**
   * Additional information about the current state.
   *
   * @param message message or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoveryStatus setMessage(java.lang.String message) {
    this.message = message;
    return this;
  }

  /**
   * The time when the next scheduled discovery job will start.
   *
   * @return value or {@code null} for none
   */
  public String getNextRunTime() {
    return nextRunTime;
  }

  /**
   * The time when the next scheduled discovery job will start.
   *
   * @param nextRunTime nextRunTime or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoveryStatus setNextRunTime(String nextRunTime) {
    this.nextRunTime = nextRunTime;
    return this;
  }

  /**
   * The current status of the discovery feature.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getState() {
    return state;
  }

  /**
   * The current status of the discovery feature.
   *
   * @param state state or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoveryStatus setState(java.lang.String state) {
    this.state = state;
    return this;
  }

  /**
   * Data Stats of the asset reported by discovery.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoveryStatusStats getStats() {
    return stats;
  }

  /**
   * Data Stats of the asset reported by discovery.
   *
   * @param stats stats or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoveryStatus setStats(
      GoogleCloudDataplexV1AssetDiscoveryStatusStats stats) {
    this.stats = stats;
    return this;
  }

  /**
   * Last update time of the status.
   *
   * @return value or {@code null} for none
   */
  public String getUpdateTime() {
    return updateTime;
  }

  /**
   * Last update time of the status.
   *
   * @param updateTime updateTime or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetDiscoveryStatus setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1AssetDiscoveryStatus set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1AssetDiscoveryStatus) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1AssetDiscoveryStatus clone() {
    return (GoogleCloudDataplexV1AssetDiscoveryStatus) super.clone();
  }
}
