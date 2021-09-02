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
 * The payload associated with Job logs that contains events describing jobs that have run within a
 * Lake.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1JobEvent extends com.google.api.client.json.GenericJson {

  /** Details about the discovery job. The value may be {@code null}. */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1JobEventDiscoveryDetails discovery;

  /** The time when the job ended running. The value may be {@code null}. */
  @com.google.api.client.util.Key private String endTime;

  /** The unique id identifying the job. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String jobId;

  /** The log message. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String message;

  /** The time when the job started running. The value may be {@code null}. */
  @com.google.api.client.util.Key private String startTime;

  /**
   * Details about the discovery job.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1JobEventDiscoveryDetails getDiscovery() {
    return discovery;
  }

  /**
   * Details about the discovery job.
   *
   * @param discovery discovery or {@code null} for none
   */
  public GoogleCloudDataplexV1JobEvent setDiscovery(
      GoogleCloudDataplexV1JobEventDiscoveryDetails discovery) {
    this.discovery = discovery;
    return this;
  }

  /**
   * The time when the job ended running.
   *
   * @return value or {@code null} for none
   */
  public String getEndTime() {
    return endTime;
  }

  /**
   * The time when the job ended running.
   *
   * @param endTime endTime or {@code null} for none
   */
  public GoogleCloudDataplexV1JobEvent setEndTime(String endTime) {
    this.endTime = endTime;
    return this;
  }

  /**
   * The unique id identifying the job.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getJobId() {
    return jobId;
  }

  /**
   * The unique id identifying the job.
   *
   * @param jobId jobId or {@code null} for none
   */
  public GoogleCloudDataplexV1JobEvent setJobId(java.lang.String jobId) {
    this.jobId = jobId;
    return this;
  }

  /**
   * The log message.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getMessage() {
    return message;
  }

  /**
   * The log message.
   *
   * @param message message or {@code null} for none
   */
  public GoogleCloudDataplexV1JobEvent setMessage(java.lang.String message) {
    this.message = message;
    return this;
  }

  /**
   * The time when the job started running.
   *
   * @return value or {@code null} for none
   */
  public String getStartTime() {
    return startTime;
  }

  /**
   * The time when the job started running.
   *
   * @param startTime startTime or {@code null} for none
   */
  public GoogleCloudDataplexV1JobEvent setStartTime(String startTime) {
    this.startTime = startTime;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1JobEvent set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1JobEvent) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1JobEvent clone() {
    return (GoogleCloudDataplexV1JobEvent) super.clone();
  }
}
