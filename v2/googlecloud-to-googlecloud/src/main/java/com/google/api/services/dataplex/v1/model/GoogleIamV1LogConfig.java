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
 * Specifies what kind of log the caller must write
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleIamV1LogConfig extends com.google.api.client.json.GenericJson {

  /** Cloud audit options. The value may be {@code null}. */
  @com.google.api.client.util.Key private GoogleIamV1LogConfigCloudAuditOptions cloudAudit;

  /** Counter options. The value may be {@code null}. */
  @com.google.api.client.util.Key private GoogleIamV1LogConfigCounterOptions counter;

  /** Data access options. The value may be {@code null}. */
  @com.google.api.client.util.Key private GoogleIamV1LogConfigDataAccessOptions dataAccess;

  /**
   * Cloud audit options.
   *
   * @return value or {@code null} for none
   */
  public GoogleIamV1LogConfigCloudAuditOptions getCloudAudit() {
    return cloudAudit;
  }

  /**
   * Cloud audit options.
   *
   * @param cloudAudit cloudAudit or {@code null} for none
   */
  public GoogleIamV1LogConfig setCloudAudit(GoogleIamV1LogConfigCloudAuditOptions cloudAudit) {
    this.cloudAudit = cloudAudit;
    return this;
  }

  /**
   * Counter options.
   *
   * @return value or {@code null} for none
   */
  public GoogleIamV1LogConfigCounterOptions getCounter() {
    return counter;
  }

  /**
   * Counter options.
   *
   * @param counter counter or {@code null} for none
   */
  public GoogleIamV1LogConfig setCounter(GoogleIamV1LogConfigCounterOptions counter) {
    this.counter = counter;
    return this;
  }

  /**
   * Data access options.
   *
   * @return value or {@code null} for none
   */
  public GoogleIamV1LogConfigDataAccessOptions getDataAccess() {
    return dataAccess;
  }

  /**
   * Data access options.
   *
   * @param dataAccess dataAccess or {@code null} for none
   */
  public GoogleIamV1LogConfig setDataAccess(GoogleIamV1LogConfigDataAccessOptions dataAccess) {
    this.dataAccess = dataAccess;
    return this;
  }

  @Override
  public GoogleIamV1LogConfig set(String fieldName, Object value) {
    return (GoogleIamV1LogConfig) super.set(fieldName, value);
  }

  @Override
  public GoogleIamV1LogConfig clone() {
    return (GoogleIamV1LogConfig) super.clone();
  }
}
