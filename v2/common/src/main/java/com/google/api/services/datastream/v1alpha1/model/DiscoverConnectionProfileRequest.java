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
 * Request message for 'discover' ConnectionProfile request.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class DiscoverConnectionProfileRequest extends com.google.api.client.json.GenericJson {

  /** An ad-hoc ConnectionProfile configuration. The value may be {@code null}. */
  @com.google.api.client.util.Key private ConnectionProfile connectionProfile;

  /** A reference to an existing ConnectionProfile. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String connectionProfileName;

  /** MySQL RDBMS to enrich with child data objects and metadata. The value may be {@code null}. */
  @com.google.api.client.util.Key private MysqlRdbms mysqlRdbms;

  /** Oracle RDBMS to enrich with child data objects and metadata. The value may be {@code null}. */
  @com.google.api.client.util.Key private OracleRdbms oracleRdbms;

  /**
   * Whether or not to retrieve the full hierarchy of data objects in the discover request. The
   * value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.Boolean recursive;

  /**
   * An ad-hoc ConnectionProfile configuration.
   *
   * @return value or {@code null} for none
   */
  public ConnectionProfile getConnectionProfile() {
    return connectionProfile;
  }

  /**
   * An ad-hoc ConnectionProfile configuration.
   *
   * @param connectionProfile connectionProfile or {@code null} for none
   */
  public DiscoverConnectionProfileRequest setConnectionProfile(
      ConnectionProfile connectionProfile) {
    this.connectionProfile = connectionProfile;
    return this;
  }

  /**
   * A reference to an existing ConnectionProfile.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getConnectionProfileName() {
    return connectionProfileName;
  }

  /**
   * A reference to an existing ConnectionProfile.
   *
   * @param connectionProfileName connectionProfileName or {@code null} for none
   */
  public DiscoverConnectionProfileRequest setConnectionProfileName(
      java.lang.String connectionProfileName) {
    this.connectionProfileName = connectionProfileName;
    return this;
  }

  /**
   * MySQL RDBMS to enrich with child data objects and metadata.
   *
   * @return value or {@code null} for none
   */
  public MysqlRdbms getMysqlRdbms() {
    return mysqlRdbms;
  }

  /**
   * MySQL RDBMS to enrich with child data objects and metadata.
   *
   * @param mysqlRdbms mysqlRdbms or {@code null} for none
   */
  public DiscoverConnectionProfileRequest setMysqlRdbms(MysqlRdbms mysqlRdbms) {
    this.mysqlRdbms = mysqlRdbms;
    return this;
  }

  /**
   * Oracle RDBMS to enrich with child data objects and metadata.
   *
   * @return value or {@code null} for none
   */
  public OracleRdbms getOracleRdbms() {
    return oracleRdbms;
  }

  /**
   * Oracle RDBMS to enrich with child data objects and metadata.
   *
   * @param oracleRdbms oracleRdbms or {@code null} for none
   */
  public DiscoverConnectionProfileRequest setOracleRdbms(OracleRdbms oracleRdbms) {
    this.oracleRdbms = oracleRdbms;
    return this;
  }

  /**
   * Whether or not to retrieve the full hierarchy of data objects in the discover request.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getRecursive() {
    return recursive;
  }

  /**
   * Whether or not to retrieve the full hierarchy of data objects in the discover request.
   *
   * @param recursive recursive or {@code null} for none
   */
  public DiscoverConnectionProfileRequest setRecursive(java.lang.Boolean recursive) {
    this.recursive = recursive;
    return this;
  }

  @Override
  public DiscoverConnectionProfileRequest set(String fieldName, Object value) {
    return (DiscoverConnectionProfileRequest) super.set(fieldName, value);
  }

  @Override
  public DiscoverConnectionProfileRequest clone() {
    return (DiscoverConnectionProfileRequest) super.clone();
  }
}
