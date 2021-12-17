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
 * A job represents an instance of a task.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1Job extends com.google.api.client.json.GenericJson {

  /** Output only. The time when the job ended. The value may be {@code null}. */
  @com.google.api.client.util.Key private String endTime;

  /**
   * Output only. The relative resource name of the job, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/
   * tasks/{task_id}/jobs/{job_id}. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String name;

  /**
   * Output only. The number of times the job has been retried (excluding the initial attempt). The
   * value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.Long retryCount;

  /** Output only. The underlying service running a job. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String service;

  /**
   * Output only. The full resource name for the job run under a particular service. The value may
   * be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String serviceJob;

  /** Output only. The time when the job was started. The value may be {@code null}. */
  @com.google.api.client.util.Key private String startTime;

  /** Output only. Execution state for the job. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String state;

  /**
   * Output only. System generated globally unique ID for the job. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String uid;

  /**
   * Output only. The time when the job ended.
   *
   * @return value or {@code null} for none
   */
  public String getEndTime() {
    return endTime;
  }

  /**
   * Output only. The time when the job ended.
   *
   * @param endTime endTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Job setEndTime(String endTime) {
    this.endTime = endTime;
    return this;
  }

  /**
   * Output only. The relative resource name of the job, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/
   * tasks/{task_id}/jobs/{job_id}.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Output only. The relative resource name of the job, of the form:
   * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/
   * tasks/{task_id}/jobs/{job_id}.
   *
   * @param name name or {@code null} for none
   */
  public GoogleCloudDataplexV1Job setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * Output only. The number of times the job has been retried (excluding the initial attempt).
   *
   * @return value or {@code null} for none
   */
  public java.lang.Long getRetryCount() {
    return retryCount;
  }

  /**
   * Output only. The number of times the job has been retried (excluding the initial attempt).
   *
   * @param retryCount retryCount or {@code null} for none
   */
  public GoogleCloudDataplexV1Job setRetryCount(java.lang.Long retryCount) {
    this.retryCount = retryCount;
    return this;
  }

  /**
   * Output only. The underlying service running a job.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getService() {
    return service;
  }

  /**
   * Output only. The underlying service running a job.
   *
   * @param service service or {@code null} for none
   */
  public GoogleCloudDataplexV1Job setService(java.lang.String service) {
    this.service = service;
    return this;
  }

  /**
   * Output only. The full resource name for the job run under a particular service.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getServiceJob() {
    return serviceJob;
  }

  /**
   * Output only. The full resource name for the job run under a particular service.
   *
   * @param serviceJob serviceJob or {@code null} for none
   */
  public GoogleCloudDataplexV1Job setServiceJob(java.lang.String serviceJob) {
    this.serviceJob = serviceJob;
    return this;
  }

  /**
   * Output only. The time when the job was started.
   *
   * @return value or {@code null} for none
   */
  public String getStartTime() {
    return startTime;
  }

  /**
   * Output only. The time when the job was started.
   *
   * @param startTime startTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Job setStartTime(String startTime) {
    this.startTime = startTime;
    return this;
  }

  /**
   * Output only. Execution state for the job.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getState() {
    return state;
  }

  /**
   * Output only. Execution state for the job.
   *
   * @param state state or {@code null} for none
   */
  public GoogleCloudDataplexV1Job setState(java.lang.String state) {
    this.state = state;
    return this;
  }

  /**
   * Output only. System generated globally unique ID for the job.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getUid() {
    return uid;
  }

  /**
   * Output only. System generated globally unique ID for the job.
   *
   * @param uid uid or {@code null} for none
   */
  public GoogleCloudDataplexV1Job setUid(java.lang.String uid) {
    this.uid = uid;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1Job set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1Job) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1Job clone() {
    return (GoogleCloudDataplexV1Job) super.clone();
  }
}
