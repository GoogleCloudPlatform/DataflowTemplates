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
 * Oracle table.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class OracleTable extends com.google.api.client.json.GenericJson {

  /**
   * Oracle columns in the schema. When unspecified as part of allow/reject lists, includes/excludes
   * everything. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<OracleColumn> oracleColumns;

  /** Table name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String tableName;

  /**
   * Oracle columns in the schema. When unspecified as part of allow/reject lists, includes/excludes
   * everything.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<OracleColumn> getOracleColumns() {
    return oracleColumns;
  }

  /**
   * Oracle columns in the schema. When unspecified as part of allow/reject lists, includes/excludes
   * everything.
   *
   * @param oracleColumns oracleColumns or {@code null} for none
   */
  public OracleTable setOracleColumns(java.util.List<OracleColumn> oracleColumns) {
    this.oracleColumns = oracleColumns;
    return this;
  }

  /**
   * Table name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getTableName() {
    return tableName;
  }

  /**
   * Table name.
   *
   * @param tableName tableName or {@code null} for none
   */
  public OracleTable setTableName(java.lang.String tableName) {
    this.tableName = tableName;
    return this;
  }

  @Override
  public OracleTable set(String fieldName, Object value) {
    return (OracleTable) super.set(fieldName, value);
  }

  @Override
  public OracleTable clone() {
    return (OracleTable) super.clone();
  }
}
