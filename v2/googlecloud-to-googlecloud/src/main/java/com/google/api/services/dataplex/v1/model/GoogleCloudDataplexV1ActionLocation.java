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
package com.google.api.services.dataplex.v1.model;

/**
 * The list of data locations associated with this action. Paths reflect the underlying storage
 * service. Cloud Storage locations are represented as URI paths. BigQuery locations refer to
 * resource names.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1ActionLocation
    extends com.google.api.client.json.GenericJson {

  /**
   * The fileset containing invalid data referenced by its catalog name. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.lang.String fileset;

  /**
   * Paths (e.g., Cloud Storage paths) where issues identified in this action are found. The value
   * may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> paths;

  /**
   * The table containing invalid data referenced by its catalog name. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.lang.String table;

  /**
   * The fileset containing invalid data referenced by its catalog name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getFileset() {
    return fileset;
  }

  /**
   * The fileset containing invalid data referenced by its catalog name.
   *
   * @param fileset fileset or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionLocation setFileset(java.lang.String fileset) {
    this.fileset = fileset;
    return this;
  }

  /**
   * Paths (e.g., Cloud Storage paths) where issues identified in this action are found.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getPaths() {
    return paths;
  }

  /**
   * Paths (e.g., Cloud Storage paths) where issues identified in this action are found.
   *
   * @param paths paths or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionLocation setPaths(java.util.List<java.lang.String> paths) {
    this.paths = paths;
    return this;
  }

  /**
   * The table containing invalid data referenced by its catalog name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getTable() {
    return table;
  }

  /**
   * The table containing invalid data referenced by its catalog name.
   *
   * @param table table or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionLocation setTable(java.lang.String table) {
    this.table = table;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1ActionLocation set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1ActionLocation) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1ActionLocation clone() {
    return (GoogleCloudDataplexV1ActionLocation) super.clone();
  }
}
