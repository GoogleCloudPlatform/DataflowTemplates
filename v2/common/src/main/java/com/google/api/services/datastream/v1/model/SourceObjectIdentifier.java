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
 * Represents an identifier of an object in the data source.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Datastream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class SourceObjectIdentifier extends com.google.api.client.json.GenericJson {

  /** Mysql data source object identifier. The value may be {@code null}. */
  @com.google.api.client.util.Key private MysqlObjectIdentifier mysqlIdentifier;

  /** Oracle data source object identifier. The value may be {@code null}. */
  @com.google.api.client.util.Key private OracleObjectIdentifier oracleIdentifier;

  /**
   * Mysql data source object identifier.
   *
   * @return value or {@code null} for none
   */
  public MysqlObjectIdentifier getMysqlIdentifier() {
    return mysqlIdentifier;
  }

  /**
   * Mysql data source object identifier.
   *
   * @param mysqlIdentifier mysqlIdentifier or {@code null} for none
   */
  public SourceObjectIdentifier setMysqlIdentifier(MysqlObjectIdentifier mysqlIdentifier) {
    this.mysqlIdentifier = mysqlIdentifier;
    return this;
  }

  /**
   * Oracle data source object identifier.
   *
   * @return value or {@code null} for none
   */
  public OracleObjectIdentifier getOracleIdentifier() {
    return oracleIdentifier;
  }

  /**
   * Oracle data source object identifier.
   *
   * @param oracleIdentifier oracleIdentifier or {@code null} for none
   */
  public SourceObjectIdentifier setOracleIdentifier(OracleObjectIdentifier oracleIdentifier) {
    this.oracleIdentifier = oracleIdentifier;
    return this;
  }

  @Override
  public SourceObjectIdentifier set(String fieldName, Object value) {
    return (SourceObjectIdentifier) super.set(fieldName, value);
  }

  @Override
  public SourceObjectIdentifier clone() {
    return (SourceObjectIdentifier) super.clone();
  }
}
