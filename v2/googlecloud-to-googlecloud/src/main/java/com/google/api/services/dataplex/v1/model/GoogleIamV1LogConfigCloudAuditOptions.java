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
 * Write a Cloud Audit log
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleIamV1LogConfigCloudAuditOptions
    extends com.google.api.client.json.GenericJson {

  /** Information used by the Cloud Audit Logging pipeline. The value may be {@code null}. */
  @com.google.api.client.util.Key
  private GoogleCloudAuditAuthorizationLoggingOptions authorizationLoggingOptions;

  /** The log_name to populate in the Cloud Audit Record. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String logName;

  /**
   * Information used by the Cloud Audit Logging pipeline.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudAuditAuthorizationLoggingOptions getAuthorizationLoggingOptions() {
    return authorizationLoggingOptions;
  }

  /**
   * Information used by the Cloud Audit Logging pipeline.
   *
   * @param authorizationLoggingOptions authorizationLoggingOptions or {@code null} for none
   */
  public GoogleIamV1LogConfigCloudAuditOptions setAuthorizationLoggingOptions(
      GoogleCloudAuditAuthorizationLoggingOptions authorizationLoggingOptions) {
    this.authorizationLoggingOptions = authorizationLoggingOptions;
    return this;
  }

  /**
   * The log_name to populate in the Cloud Audit Record.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getLogName() {
    return logName;
  }

  /**
   * The log_name to populate in the Cloud Audit Record.
   *
   * @param logName logName or {@code null} for none
   */
  public GoogleIamV1LogConfigCloudAuditOptions setLogName(java.lang.String logName) {
    this.logName = logName;
    return this;
  }

  @Override
  public GoogleIamV1LogConfigCloudAuditOptions set(String fieldName, Object value) {
    return (GoogleIamV1LogConfigCloudAuditOptions) super.set(fieldName, value);
  }

  @Override
  public GoogleIamV1LogConfigCloudAuditOptions clone() {
    return (GoogleIamV1LogConfigCloudAuditOptions) super.clone();
  }
}
