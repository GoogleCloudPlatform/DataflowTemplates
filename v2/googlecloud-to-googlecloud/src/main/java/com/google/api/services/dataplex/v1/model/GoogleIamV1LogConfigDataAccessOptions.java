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
 * Write a Data Access (Gin) log
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleIamV1LogConfigDataAccessOptions
    extends com.google.api.client.json.GenericJson {

  /** The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String logMode;

  /**
   * @return value or {@code null} for none
   */
  public java.lang.String getLogMode() {
    return logMode;
  }

  /**
   * @param logMode logMode or {@code null} for none
   */
  public GoogleIamV1LogConfigDataAccessOptions setLogMode(java.lang.String logMode) {
    this.logMode = logMode;
    return this;
  }

  @Override
  public GoogleIamV1LogConfigDataAccessOptions set(String fieldName, Object value) {
    return (GoogleIamV1LogConfigDataAccessOptions) super.set(fieldName, value);
  }

  @Override
  public GoogleIamV1LogConfigDataAccessOptions clone() {
    return (GoogleIamV1LogConfigDataAccessOptions) super.clone();
  }
}
