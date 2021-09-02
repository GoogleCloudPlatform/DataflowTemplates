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
 * Action details for incompatible schemas detected by discovery.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1ActionIncompatibleDataSchema
    extends com.google.api.client.json.GenericJson {

  /**
   * The existing and expected schema of the table. The schema is provided as a JSON formatted
   * structure listing columns and data types. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String existingSchema;

  /**
   * The new and incompatible schema within the table. The schema is provided as a JSON formatted
   * structured listing columns and data types. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String newSchema;

  /**
   * The list of data locations sampled and used for format/schema inference. The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> sampledDataLocations;

  /** The name of the table containing invalid data. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String table;

  /**
   * The existing and expected schema of the table. The schema is provided as a JSON formatted
   * structure listing columns and data types.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getExistingSchema() {
    return existingSchema;
  }

  /**
   * The existing and expected schema of the table. The schema is provided as a JSON formatted
   * structure listing columns and data types.
   *
   * @param existingSchema existingSchema or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionIncompatibleDataSchema setExistingSchema(
      java.lang.String existingSchema) {
    this.existingSchema = existingSchema;
    return this;
  }

  /**
   * The new and incompatible schema within the table. The schema is provided as a JSON formatted
   * structured listing columns and data types.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getNewSchema() {
    return newSchema;
  }

  /**
   * The new and incompatible schema within the table. The schema is provided as a JSON formatted
   * structured listing columns and data types.
   *
   * @param newSchema newSchema or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionIncompatibleDataSchema setNewSchema(
      java.lang.String newSchema) {
    this.newSchema = newSchema;
    return this;
  }

  /**
   * The list of data locations sampled and used for format/schema inference.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getSampledDataLocations() {
    return sampledDataLocations;
  }

  /**
   * The list of data locations sampled and used for format/schema inference.
   *
   * @param sampledDataLocations sampledDataLocations or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionIncompatibleDataSchema setSampledDataLocations(
      java.util.List<java.lang.String> sampledDataLocations) {
    this.sampledDataLocations = sampledDataLocations;
    return this;
  }

  /**
   * The name of the table containing invalid data.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getTable() {
    return table;
  }

  /**
   * The name of the table containing invalid data.
   *
   * @param table table or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionIncompatibleDataSchema setTable(java.lang.String table) {
    this.table = table;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1ActionIncompatibleDataSchema set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1ActionIncompatibleDataSchema) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1ActionIncompatibleDataSchema clone() {
    return (GoogleCloudDataplexV1ActionIncompatibleDataSchema) super.clone();
  }
}
