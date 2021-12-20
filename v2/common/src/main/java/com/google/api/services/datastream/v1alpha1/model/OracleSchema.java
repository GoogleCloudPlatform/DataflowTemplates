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
 * Oracle schema.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class OracleSchema extends com.google.api.client.json.GenericJson {

  /** Tables in the schema. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.List<OracleTable> oracleTables;

  /** Schema name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String schemaName;

  /**
   * Tables in the schema.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<OracleTable> getOracleTables() {
    return oracleTables;
  }

  /**
   * Tables in the schema.
   *
   * @param oracleTables oracleTables or {@code null} for none
   */
  public OracleSchema setOracleTables(java.util.List<OracleTable> oracleTables) {
    this.oracleTables = oracleTables;
    return this;
  }

  /**
   * Schema name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getSchemaName() {
    return schemaName;
  }

  /**
   * Schema name.
   *
   * @param schemaName schemaName or {@code null} for none
   */
  public OracleSchema setSchemaName(java.lang.String schemaName) {
    this.schemaName = schemaName;
    return this;
  }

  @Override
  public OracleSchema set(String fieldName, Object value) {
    return (OracleSchema) super.set(fieldName, value);
  }

  @Override
  public OracleSchema clone() {
    return (OracleSchema) super.clone();
  }
}
