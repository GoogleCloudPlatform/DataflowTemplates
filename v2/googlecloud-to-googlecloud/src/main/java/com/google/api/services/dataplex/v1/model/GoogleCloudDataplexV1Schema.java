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
 * The schema information describing the structure and layout of the data.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1Schema extends com.google.api.client.json.GenericJson {

  /**
   * Optional. The sequence of fields describing data in table entities. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key
  private java.util.List<GoogleCloudDataplexV1SchemaSchemaField> fields;

  /**
   * Optional. The sequence of fields describing the partition structure in entities. If this is
   * empty, there are no partitions within the data. The value may be {@code null}.
   */
  @com.google.api.client.util.Key
  private java.util.List<GoogleCloudDataplexV1SchemaPartitionField> partitionFields;

  /**
   * Optional. The structure of paths containing partition data within the entity. The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String partitionStyle;

  /**
   * Required. Whether the schema is user managed, or managed by the service. User managed schemas
   * are not automatically updated by discovery jobs. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.Boolean userManaged;

  /**
   * Optional. The sequence of fields describing data in table entities.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<GoogleCloudDataplexV1SchemaSchemaField> getFields() {
    return fields;
  }

  /**
   * Optional. The sequence of fields describing data in table entities.
   *
   * @param fields fields or {@code null} for none
   */
  public GoogleCloudDataplexV1Schema setFields(
      java.util.List<GoogleCloudDataplexV1SchemaSchemaField> fields) {
    this.fields = fields;
    return this;
  }

  /**
   * Optional. The sequence of fields describing the partition structure in entities. If this is
   * empty, there are no partitions within the data.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<GoogleCloudDataplexV1SchemaPartitionField> getPartitionFields() {
    return partitionFields;
  }

  /**
   * Optional. The sequence of fields describing the partition structure in entities. If this is
   * empty, there are no partitions within the data.
   *
   * @param partitionFields partitionFields or {@code null} for none
   */
  public GoogleCloudDataplexV1Schema setPartitionFields(
      java.util.List<GoogleCloudDataplexV1SchemaPartitionField> partitionFields) {
    this.partitionFields = partitionFields;
    return this;
  }

  /**
   * Optional. The structure of paths containing partition data within the entity.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getPartitionStyle() {
    return partitionStyle;
  }

  /**
   * Optional. The structure of paths containing partition data within the entity.
   *
   * @param partitionStyle partitionStyle or {@code null} for none
   */
  public GoogleCloudDataplexV1Schema setPartitionStyle(java.lang.String partitionStyle) {
    this.partitionStyle = partitionStyle;
    return this;
  }

  /**
   * Required. Whether the schema is user managed, or managed by the service. User managed schemas
   * are not automatically updated by discovery jobs.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getUserManaged() {
    return userManaged;
  }

  /**
   * Required. Whether the schema is user managed, or managed by the service. User managed schemas
   * are not automatically updated by discovery jobs.
   *
   * @param userManaged userManaged or {@code null} for none
   */
  public GoogleCloudDataplexV1Schema setUserManaged(java.lang.Boolean userManaged) {
    this.userManaged = userManaged;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1Schema set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1Schema) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1Schema clone() {
    return (GoogleCloudDataplexV1Schema) super.clone();
  }
}
