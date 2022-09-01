/*
 * Copyright (C) 2022 Google LLC
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
package com.google.api.services.datastream.v1.model;

/**
 * Mysql data source object identifier.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Datastream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class MysqlObjectIdentifier extends com.google.api.client.json.GenericJson {

  /** Required. The database name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String database;

  /** Required. The table name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String table;

  /**
   * Required. The database name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDatabase() {
    return database;
  }

  /**
   * Required. The database name.
   *
   * @param database database or {@code null} for none
   */
  public MysqlObjectIdentifier setDatabase(java.lang.String database) {
    this.database = database;
    return this;
  }

  /**
   * Required. The table name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getTable() {
    return table;
  }

  /**
   * Required. The table name.
   *
   * @param table table or {@code null} for none
   */
  public MysqlObjectIdentifier setTable(java.lang.String table) {
    this.table = table;
    return this;
  }

  @Override
  public MysqlObjectIdentifier set(String fieldName, Object value) {
    return (MysqlObjectIdentifier) super.set(fieldName, value);
  }

  @Override
  public MysqlObjectIdentifier clone() {
    return (MysqlObjectIdentifier) super.clone();
  }
}
