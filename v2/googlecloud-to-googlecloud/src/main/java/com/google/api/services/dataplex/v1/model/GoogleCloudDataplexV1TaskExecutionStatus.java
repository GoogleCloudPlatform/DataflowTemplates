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
 * Status of the latest task execution.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1TaskExecutionStatus
    extends com.google.api.client.json.GenericJson {

  /** Output only. Latest execution state. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String executionState;

  /** The duration of the last run of the task. The value may be {@code null}. */
  @com.google.api.client.util.Key private String lastRunDuration;

  /** The time when the last task execution started. The value may be {@code null}. */
  @com.google.api.client.util.Key private String lastRunTime;

  /** Output only. Additional information about the current state. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String message;

  /** The time when the next task execution will start. The value may be {@code null}. */
  @com.google.api.client.util.Key private String nextRunTime;

  /** Output only. Last update time of the status. The value may be {@code null}. */
  @com.google.api.client.util.Key private String updateTime;

  /**
   * Output only. Latest execution state.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getExecutionState() {
    return executionState;
  }

  /**
   * Output only. Latest execution state.
   *
   * @param executionState executionState or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskExecutionStatus setExecutionState(
      java.lang.String executionState) {
    this.executionState = executionState;
    return this;
  }

  /**
   * The duration of the last run of the task.
   *
   * @return value or {@code null} for none
   */
  public String getLastRunDuration() {
    return lastRunDuration;
  }

  /**
   * The duration of the last run of the task.
   *
   * @param lastRunDuration lastRunDuration or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskExecutionStatus setLastRunDuration(String lastRunDuration) {
    this.lastRunDuration = lastRunDuration;
    return this;
  }

  /**
   * The time when the last task execution started.
   *
   * @return value or {@code null} for none
   */
  public String getLastRunTime() {
    return lastRunTime;
  }

  /**
   * The time when the last task execution started.
   *
   * @param lastRunTime lastRunTime or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskExecutionStatus setLastRunTime(String lastRunTime) {
    this.lastRunTime = lastRunTime;
    return this;
  }

  /**
   * Output only. Additional information about the current state.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getMessage() {
    return message;
  }

  /**
   * Output only. Additional information about the current state.
   *
   * @param message message or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskExecutionStatus setMessage(java.lang.String message) {
    this.message = message;
    return this;
  }

  /**
   * The time when the next task execution will start.
   *
   * @return value or {@code null} for none
   */
  public String getNextRunTime() {
    return nextRunTime;
  }

  /**
   * The time when the next task execution will start.
   *
   * @param nextRunTime nextRunTime or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskExecutionStatus setNextRunTime(String nextRunTime) {
    this.nextRunTime = nextRunTime;
    return this;
  }

  /**
   * Output only. Last update time of the status.
   *
   * @return value or {@code null} for none
   */
  public String getUpdateTime() {
    return updateTime;
  }

  /**
   * Output only. Last update time of the status.
   *
   * @param updateTime updateTime or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskExecutionStatus setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1TaskExecutionStatus set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1TaskExecutionStatus) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1TaskExecutionStatus clone() {
    return (GoogleCloudDataplexV1TaskExecutionStatus) super.clone();
  }
}
