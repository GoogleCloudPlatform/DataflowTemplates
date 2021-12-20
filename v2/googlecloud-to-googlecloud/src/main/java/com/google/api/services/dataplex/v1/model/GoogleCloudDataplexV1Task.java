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
 * A task represents a user-visible job.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1Task extends com.google.api.client.json.GenericJson {

  /** Output only. The time when the task was created. The value may be {@code null}. */
  @com.google.api.client.util.Key private String createTime;

  /** Optional. Description of the task. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String description;

  /** Config related to running ad-hoc Discovery tasks. The value may be {@code null}. */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1TaskDiscoveryTaskConfig discovery;

  /** Optional. User friendly display name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String displayName;

  /** Required. Spec related to how a task is executed. The value may be {@code null}. */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1TaskExecutionSpec executionSpec;

  /** Output only. Status of the latest task executions. The value may be {@code null}. */
  @com.google.api.client.util.Key
  private java.util.List<GoogleCloudDataplexV1TaskExecutionStatus> executionStatus;

  /** Optional. User-defined labels for the task. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.String> labels;

  /**
   * Output only. The relative resource name of the task, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/ tasks/{task_id}. The value
   * may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String name;

  /** Config related to running scheduled Notebooks. The value may be {@code null}. */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1TaskNotebookTaskConfig notebook;

  /** Config related to running custom Spark tasks. The value may be {@code null}. */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1TaskSparkTaskConfig spark;

  /** Output only. Current state of the task. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String state;

  /**
   * Required. Spec related to how often and when a task should be triggered. The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1TaskTriggerSpec triggerSpec;

  /**
   * Output only. System generated globally unique ID for the task. This ID will be different if the
   * task is deleted and re-created with the same name. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String uid;

  /** Output only. The time when the task was last updated. The value may be {@code null}. */
  @com.google.api.client.util.Key private String updateTime;

  /**
   * Output only. The time when the task was created.
   *
   * @return value or {@code null} for none
   */
  public String getCreateTime() {
    return createTime;
  }

  /**
   * Output only. The time when the task was created.
   *
   * @param createTime createTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Task setCreateTime(String createTime) {
    this.createTime = createTime;
    return this;
  }

  /**
   * Optional. Description of the task.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDescription() {
    return description;
  }

  /**
   * Optional. Description of the task.
   *
   * @param description description or {@code null} for none
   */
  public GoogleCloudDataplexV1Task setDescription(java.lang.String description) {
    this.description = description;
    return this;
  }

  /**
   * Config related to running ad-hoc Discovery tasks.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskDiscoveryTaskConfig getDiscovery() {
    return discovery;
  }

  /**
   * Config related to running ad-hoc Discovery tasks.
   *
   * @param discovery discovery or {@code null} for none
   */
  public GoogleCloudDataplexV1Task setDiscovery(
      GoogleCloudDataplexV1TaskDiscoveryTaskConfig discovery) {
    this.discovery = discovery;
    return this;
  }

  /**
   * Optional. User friendly display name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDisplayName() {
    return displayName;
  }

  /**
   * Optional. User friendly display name.
   *
   * @param displayName displayName or {@code null} for none
   */
  public GoogleCloudDataplexV1Task setDisplayName(java.lang.String displayName) {
    this.displayName = displayName;
    return this;
  }

  /**
   * Required. Spec related to how a task is executed.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskExecutionSpec getExecutionSpec() {
    return executionSpec;
  }

  /**
   * Required. Spec related to how a task is executed.
   *
   * @param executionSpec executionSpec or {@code null} for none
   */
  public GoogleCloudDataplexV1Task setExecutionSpec(
      GoogleCloudDataplexV1TaskExecutionSpec executionSpec) {
    this.executionSpec = executionSpec;
    return this;
  }

  /**
   * Output only. Status of the latest task executions.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<GoogleCloudDataplexV1TaskExecutionStatus> getExecutionStatus() {
    return executionStatus;
  }

  /**
   * Output only. Status of the latest task executions.
   *
   * @param executionStatus executionStatus or {@code null} for none
   */
  public GoogleCloudDataplexV1Task setExecutionStatus(
      java.util.List<GoogleCloudDataplexV1TaskExecutionStatus> executionStatus) {
    this.executionStatus = executionStatus;
    return this;
  }

  /**
   * Optional. User-defined labels for the task.
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.String> getLabels() {
    return labels;
  }

  /**
   * Optional. User-defined labels for the task.
   *
   * @param labels labels or {@code null} for none
   */
  public GoogleCloudDataplexV1Task setLabels(java.util.Map<String, java.lang.String> labels) {
    this.labels = labels;
    return this;
  }

  /**
   * Output only. The relative resource name of the task, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/ tasks/{task_id}.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Output only. The relative resource name of the task, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/ tasks/{task_id}.
   *
   * @param name name or {@code null} for none
   */
  public GoogleCloudDataplexV1Task setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * Config related to running scheduled Notebooks.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskNotebookTaskConfig getNotebook() {
    return notebook;
  }

  /**
   * Config related to running scheduled Notebooks.
   *
   * @param notebook notebook or {@code null} for none
   */
  public GoogleCloudDataplexV1Task setNotebook(
      GoogleCloudDataplexV1TaskNotebookTaskConfig notebook) {
    this.notebook = notebook;
    return this;
  }

  /**
   * Config related to running custom Spark tasks.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskSparkTaskConfig getSpark() {
    return spark;
  }

  /**
   * Config related to running custom Spark tasks.
   *
   * @param spark spark or {@code null} for none
   */
  public GoogleCloudDataplexV1Task setSpark(GoogleCloudDataplexV1TaskSparkTaskConfig spark) {
    this.spark = spark;
    return this;
  }

  /**
   * Output only. Current state of the task.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getState() {
    return state;
  }

  /**
   * Output only. Current state of the task.
   *
   * @param state state or {@code null} for none
   */
  public GoogleCloudDataplexV1Task setState(java.lang.String state) {
    this.state = state;
    return this;
  }

  /**
   * Required. Spec related to how often and when a task should be triggered.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskTriggerSpec getTriggerSpec() {
    return triggerSpec;
  }

  /**
   * Required. Spec related to how often and when a task should be triggered.
   *
   * @param triggerSpec triggerSpec or {@code null} for none
   */
  public GoogleCloudDataplexV1Task setTriggerSpec(
      GoogleCloudDataplexV1TaskTriggerSpec triggerSpec) {
    this.triggerSpec = triggerSpec;
    return this;
  }

  /**
   * Output only. System generated globally unique ID for the task. This ID will be different if the
   * task is deleted and re-created with the same name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getUid() {
    return uid;
  }

  /**
   * Output only. System generated globally unique ID for the task. This ID will be different if the
   * task is deleted and re-created with the same name.
   *
   * @param uid uid or {@code null} for none
   */
  public GoogleCloudDataplexV1Task setUid(java.lang.String uid) {
    this.uid = uid;
    return this;
  }

  /**
   * Output only. The time when the task was last updated.
   *
   * @return value or {@code null} for none
   */
  public String getUpdateTime() {
    return updateTime;
  }

  /**
   * Output only. The time when the task was last updated.
   *
   * @param updateTime updateTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Task setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1Task set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1Task) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1Task clone() {
    return (GoogleCloudDataplexV1Task) super.clone();
  }
}
