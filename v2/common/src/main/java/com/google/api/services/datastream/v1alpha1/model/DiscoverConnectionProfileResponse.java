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
 * Model definition for DiscoverConnectionProfileResponse.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class DiscoverConnectionProfileResponse
    extends com.google.api.client.json.GenericJson {

  /** Enriched MySQL RDBMS object. The value may be {@code null}. */
  @com.google.api.client.util.Key private MysqlRdbms mysqlRdbms;

  /** Enriched Oracle RDBMS object. The value may be {@code null}. */
  @com.google.api.client.util.Key private OracleRdbms oracleRdbms;

  /**
   * Enriched MySQL RDBMS object.
   *
   * @return value or {@code null} for none
   */
  public MysqlRdbms getMysqlRdbms() {
    return mysqlRdbms;
  }

  /**
   * Enriched MySQL RDBMS object.
   *
   * @param mysqlRdbms mysqlRdbms or {@code null} for none
   */
  public DiscoverConnectionProfileResponse setMysqlRdbms(MysqlRdbms mysqlRdbms) {
    this.mysqlRdbms = mysqlRdbms;
    return this;
  }

  /**
   * Enriched Oracle RDBMS object.
   *
   * @return value or {@code null} for none
   */
  public OracleRdbms getOracleRdbms() {
    return oracleRdbms;
  }

  /**
   * Enriched Oracle RDBMS object.
   *
   * @param oracleRdbms oracleRdbms or {@code null} for none
   */
  public DiscoverConnectionProfileResponse setOracleRdbms(OracleRdbms oracleRdbms) {
    this.oracleRdbms = oracleRdbms;
    return this;
  }

  @Override
  public DiscoverConnectionProfileResponse set(String fieldName, Object value) {
    return (DiscoverConnectionProfileResponse) super.set(fieldName, value);
  }

  @Override
  public DiscoverConnectionProfileResponse clone() {
    return (DiscoverConnectionProfileResponse) super.clone();
  }
}
