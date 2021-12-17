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
 * Backfill Strategy to backfill all Stream’s source objects.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class BackfillAllStrategy extends com.google.api.client.json.GenericJson {

  /** MySQL data source objects to avoid backfilling. The value may be {@code null}. */
  @com.google.api.client.util.Key private MysqlRdbms mysqlExcludedObjects;

  /** Oracle data source objects to avoid backfilling. The value may be {@code null}. */
  @com.google.api.client.util.Key private OracleRdbms oracleExcludedObjects;

  /**
   * MySQL data source objects to avoid backfilling.
   *
   * @return value or {@code null} for none
   */
  public MysqlRdbms getMysqlExcludedObjects() {
    return mysqlExcludedObjects;
  }

  /**
   * MySQL data source objects to avoid backfilling.
   *
   * @param mysqlExcludedObjects mysqlExcludedObjects or {@code null} for none
   */
  public BackfillAllStrategy setMysqlExcludedObjects(MysqlRdbms mysqlExcludedObjects) {
    this.mysqlExcludedObjects = mysqlExcludedObjects;
    return this;
  }

  /**
   * Oracle data source objects to avoid backfilling.
   *
   * @return value or {@code null} for none
   */
  public OracleRdbms getOracleExcludedObjects() {
    return oracleExcludedObjects;
  }

  /**
   * Oracle data source objects to avoid backfilling.
   *
   * @param oracleExcludedObjects oracleExcludedObjects or {@code null} for none
   */
  public BackfillAllStrategy setOracleExcludedObjects(OracleRdbms oracleExcludedObjects) {
    this.oracleExcludedObjects = oracleExcludedObjects;
    return this;
  }

  @Override
  public BackfillAllStrategy set(String fieldName, Object value) {
    return (BackfillAllStrategy) super.set(fieldName, value);
  }

  @Override
  public BackfillAllStrategy clone() {
    return (BackfillAllStrategy) super.clone();
  }
}
