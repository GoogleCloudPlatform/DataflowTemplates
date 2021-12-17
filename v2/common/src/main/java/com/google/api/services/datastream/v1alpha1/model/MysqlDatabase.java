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
 * MySQL database.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class MysqlDatabase extends com.google.api.client.json.GenericJson {

  /** Database name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String databaseName;

  /** Tables in the database. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.List<MysqlTable> mysqlTables;

  /**
   * Database name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDatabaseName() {
    return databaseName;
  }

  /**
   * Database name.
   *
   * @param databaseName databaseName or {@code null} for none
   */
  public MysqlDatabase setDatabaseName(java.lang.String databaseName) {
    this.databaseName = databaseName;
    return this;
  }

  /**
   * Tables in the database.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<MysqlTable> getMysqlTables() {
    return mysqlTables;
  }

  /**
   * Tables in the database.
   *
   * @param mysqlTables mysqlTables or {@code null} for none
   */
  public MysqlDatabase setMysqlTables(java.util.List<MysqlTable> mysqlTables) {
    this.mysqlTables = mysqlTables;
    return this;
  }

  @Override
  public MysqlDatabase set(String fieldName, Object value) {
    return (MysqlDatabase) super.set(fieldName, value);
  }

  @Override
  public MysqlDatabase clone() {
    return (MysqlDatabase) super.clone();
  }
}
