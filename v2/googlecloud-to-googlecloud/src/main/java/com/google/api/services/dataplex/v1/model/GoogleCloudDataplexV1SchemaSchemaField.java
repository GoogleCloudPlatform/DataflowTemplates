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
 * Represents a column field within a table schema.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1SchemaSchemaField
    extends com.google.api.client.json.GenericJson {

  /** Optional. User friendly field description. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String description;

  /** Optional. Any nested field for complex types. The value may be {@code null}. */
  @com.google.api.client.util.Key
  private java.util.List<GoogleCloudDataplexV1SchemaSchemaField> fields;

  /** Required. Additional field semantics. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String mode;

  /** Required. The name of the field. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String name;

  /** Required. The type of the field. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String type;

  /**
   * Optional. User friendly field description.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDescription() {
    return description;
  }

  /**
   * Optional. User friendly field description.
   *
   * @param description description or {@code null} for none
   */
  public GoogleCloudDataplexV1SchemaSchemaField setDescription(java.lang.String description) {
    this.description = description;
    return this;
  }

  /**
   * Optional. Any nested field for complex types.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<GoogleCloudDataplexV1SchemaSchemaField> getFields() {
    return fields;
  }

  /**
   * Optional. Any nested field for complex types.
   *
   * @param fields fields or {@code null} for none
   */
  public GoogleCloudDataplexV1SchemaSchemaField setFields(
      java.util.List<GoogleCloudDataplexV1SchemaSchemaField> fields) {
    this.fields = fields;
    return this;
  }

  /**
   * Required. Additional field semantics.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getMode() {
    return mode;
  }

  /**
   * Required. Additional field semantics.
   *
   * @param mode mode or {@code null} for none
   */
  public GoogleCloudDataplexV1SchemaSchemaField setMode(java.lang.String mode) {
    this.mode = mode;
    return this;
  }

  /**
   * Required. The name of the field.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Required. The name of the field.
   *
   * @param name name or {@code null} for none
   */
  public GoogleCloudDataplexV1SchemaSchemaField setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * Required. The type of the field.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getType() {
    return type;
  }

  /**
   * Required. The type of the field.
   *
   * @param type type or {@code null} for none
   */
  public GoogleCloudDataplexV1SchemaSchemaField setType(java.lang.String type) {
    this.type = type;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1SchemaSchemaField set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1SchemaSchemaField) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1SchemaSchemaField clone() {
    return (GoogleCloudDataplexV1SchemaSchemaField) super.clone();
  }
}
