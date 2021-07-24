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
 * MySQL data source configuration
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class MysqlSourceConfig extends com.google.api.client.json.GenericJson {

  /** MySQL objects to retrieve from the data source The value may be {@code null}. */
  @com.google.api.client.util.Key private MysqlRdbms allowlist;

  /** MySQL objects to avoid retrieving The value may be {@code null}. */
  @com.google.api.client.util.Key private MysqlRdbms rejectlist;

  /**
   * MySQL objects to retrieve from the data source
   *
   * @return value or {@code null} for none
   */
  public MysqlRdbms getAllowlist() {
    return allowlist;
  }

  /**
   * MySQL objects to retrieve from the data source
   *
   * @param allowlist allowlist or {@code null} for none
   */
  public MysqlSourceConfig setAllowlist(MysqlRdbms allowlist) {
    this.allowlist = allowlist;
    return this;
  }

  /**
   * MySQL objects to avoid retrieving
   *
   * @return value or {@code null} for none
   */
  public MysqlRdbms getRejectlist() {
    return rejectlist;
  }

  /**
   * MySQL objects to avoid retrieving
   *
   * @param rejectlist rejectlist or {@code null} for none
   */
  public MysqlSourceConfig setRejectlist(MysqlRdbms rejectlist) {
    this.rejectlist = rejectlist;
    return this;
  }

  @Override
  public MysqlSourceConfig set(String fieldName, Object value) {
    return (MysqlSourceConfig) super.set(fieldName, value);
  }

  @Override
  public MysqlSourceConfig clone() {
    return (MysqlSourceConfig) super.clone();
  }
}
