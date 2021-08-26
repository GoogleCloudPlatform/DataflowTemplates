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
 * The configuration of the data source.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class SourceConfig extends com.google.api.client.json.GenericJson {

  /** MySQL data source configuration The value may be {@code null}. */
  @com.google.api.client.util.Key private MysqlSourceConfig mysqlSourceConfig;

  /** Oracle data source configuration The value may be {@code null}. */
  @com.google.api.client.util.Key private OracleSourceConfig oracleSourceConfig;

  /** Required. Source connection profile identifier. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String sourceConnectionProfileName;

  /**
   * MySQL data source configuration
   *
   * @return value or {@code null} for none
   */
  public MysqlSourceConfig getMysqlSourceConfig() {
    return mysqlSourceConfig;
  }

  /**
   * MySQL data source configuration
   *
   * @param mysqlSourceConfig mysqlSourceConfig or {@code null} for none
   */
  public SourceConfig setMysqlSourceConfig(MysqlSourceConfig mysqlSourceConfig) {
    this.mysqlSourceConfig = mysqlSourceConfig;
    return this;
  }

  /**
   * Oracle data source configuration
   *
   * @return value or {@code null} for none
   */
  public OracleSourceConfig getOracleSourceConfig() {
    return oracleSourceConfig;
  }

  /**
   * Oracle data source configuration
   *
   * @param oracleSourceConfig oracleSourceConfig or {@code null} for none
   */
  public SourceConfig setOracleSourceConfig(OracleSourceConfig oracleSourceConfig) {
    this.oracleSourceConfig = oracleSourceConfig;
    return this;
  }

  /**
   * Required. Source connection profile identifier.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getSourceConnectionProfileName() {
    return sourceConnectionProfileName;
  }

  /**
   * Required. Source connection profile identifier.
   *
   * @param sourceConnectionProfileName sourceConnectionProfileName or {@code null} for none
   */
  public SourceConfig setSourceConnectionProfileName(java.lang.String sourceConnectionProfileName) {
    this.sourceConnectionProfileName = sourceConnectionProfileName;
    return this;
  }

  @Override
  public SourceConfig set(String fieldName, Object value) {
    return (SourceConfig) super.set(fieldName, value);
  }

  @Override
  public SourceConfig clone() {
    return (SourceConfig) super.clone();
  }
}
