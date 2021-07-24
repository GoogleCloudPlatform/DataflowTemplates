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
package com.google.api.services.datastream.v1alpha1.model;

/**
 * Oracle data source configuration
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class OracleSourceConfig extends com.google.api.client.json.GenericJson {

  /** Oracle objects to retrieve from the data source The value may be {@code null}. */
  @com.google.api.client.util.Key private OracleRdbms allowlist;

  /** Oracle objects to avoid retrieving The value may be {@code null}. */
  @com.google.api.client.util.Key private OracleRdbms rejectlist;

  /**
   * Oracle objects to retrieve from the data source
   *
   * @return value or {@code null} for none
   */
  public OracleRdbms getAllowlist() {
    return allowlist;
  }

  /**
   * Oracle objects to retrieve from the data source
   *
   * @param allowlist allowlist or {@code null} for none
   */
  public OracleSourceConfig setAllowlist(OracleRdbms allowlist) {
    this.allowlist = allowlist;
    return this;
  }

  /**
   * Oracle objects to avoid retrieving
   *
   * @return value or {@code null} for none
   */
  public OracleRdbms getRejectlist() {
    return rejectlist;
  }

  /**
   * Oracle objects to avoid retrieving
   *
   * @param rejectlist rejectlist or {@code null} for none
   */
  public OracleSourceConfig setRejectlist(OracleRdbms rejectlist) {
    this.rejectlist = rejectlist;
    return this;
  }

  @Override
  public OracleSourceConfig set(String fieldName, Object value) {
    return (OracleSourceConfig) super.set(fieldName, value);
  }

  @Override
  public OracleSourceConfig clone() {
    return (OracleSourceConfig) super.clone();
  }
}
