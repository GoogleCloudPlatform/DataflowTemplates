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
 * Represents an active analyze session running for a user.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1Session extends com.google.api.client.json.GenericJson {

  /** Output only. Session start time. The value may be {@code null}. */
  @com.google.api.client.util.Key private String createTime;

  /**
   * Output only. The relative resource name of the content, of the form: projects/{project_id}/loca
   * tions/{location_id}/lakes/{lake_id}/environment/{environment_id}/sessions/{session_id} The
   * value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String name;

  /** The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String state;

  /** Output only. Email of user running the session. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String userId;

  /**
   * Output only. Session start time.
   *
   * @return value or {@code null} for none
   */
  public String getCreateTime() {
    return createTime;
  }

  /**
   * Output only. Session start time.
   *
   * @param createTime createTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Session setCreateTime(String createTime) {
    this.createTime = createTime;
    return this;
  }

  /**
   * Output only. The relative resource name of the content, of the form: projects/{project_id}/loca
   * tions/{location_id}/lakes/{lake_id}/environment/{environment_id}/sessions/{session_id}
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Output only. The relative resource name of the content, of the form: projects/{project_id}/loca
   * tions/{location_id}/lakes/{lake_id}/environment/{environment_id}/sessions/{session_id}
   *
   * @param name name or {@code null} for none
   */
  public GoogleCloudDataplexV1Session setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * @return value or {@code null} for none
   */
  public java.lang.String getState() {
    return state;
  }

  /**
   * @param state state or {@code null} for none
   */
  public GoogleCloudDataplexV1Session setState(java.lang.String state) {
    this.state = state;
    return this;
  }

  /**
   * Output only. Email of user running the session.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getUserId() {
    return userId;
  }

  /**
   * Output only. Email of user running the session.
   *
   * @param userId userId or {@code null} for none
   */
  public GoogleCloudDataplexV1Session setUserId(java.lang.String userId) {
    this.userId = userId;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1Session set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1Session) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1Session clone() {
    return (GoogleCloudDataplexV1Session) super.clone();
  }
}
