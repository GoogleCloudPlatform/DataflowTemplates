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
 * Details about table entities being created, updated or deleted.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1MetadataEventTable
    extends com.google.api.client.json.GenericJson {

  /** The format of the data within the table. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String dataFormat;

  /** The number of data items in the table. The value may be {@code null}. */
  @com.google.api.client.util.Key @com.google.api.client.json.JsonString
  private java.lang.Long dataItemsCount;

  /**
   * The list of associated external tables published for the table. This is formatted using fully-
   * qualified names such as: dpms:project.location.service.database.table and
   * bigquery:project.location.dataset.table The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> externalTables;

  /** The data location of the table. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String location;

  /** The name of the table entity. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String name;

  /** The schema of the table. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String schema;

  /**
   * The format of the data within the table.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDataFormat() {
    return dataFormat;
  }

  /**
   * The format of the data within the table.
   *
   * @param dataFormat dataFormat or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEventTable setDataFormat(java.lang.String dataFormat) {
    this.dataFormat = dataFormat;
    return this;
  }

  /**
   * The number of data items in the table.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Long getDataItemsCount() {
    return dataItemsCount;
  }

  /**
   * The number of data items in the table.
   *
   * @param dataItemsCount dataItemsCount or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEventTable setDataItemsCount(java.lang.Long dataItemsCount) {
    this.dataItemsCount = dataItemsCount;
    return this;
  }

  /**
   * The list of associated external tables published for the table. This is formatted using fully-
   * qualified names such as: dpms:project.location.service.database.table and
   * bigquery:project.location.dataset.table
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getExternalTables() {
    return externalTables;
  }

  /**
   * The list of associated external tables published for the table. This is formatted using fully-
   * qualified names such as: dpms:project.location.service.database.table and
   * bigquery:project.location.dataset.table
   *
   * @param externalTables externalTables or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEventTable setExternalTables(
      java.util.List<java.lang.String> externalTables) {
    this.externalTables = externalTables;
    return this;
  }

  /**
   * The data location of the table.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getLocation() {
    return location;
  }

  /**
   * The data location of the table.
   *
   * @param location location or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEventTable setLocation(java.lang.String location) {
    this.location = location;
    return this;
  }

  /**
   * The name of the table entity.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * The name of the table entity.
   *
   * @param name name or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEventTable setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * The schema of the table.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getSchema() {
    return schema;
  }

  /**
   * The schema of the table.
   *
   * @param schema schema or {@code null} for none
   */
  public GoogleCloudDataplexV1MetadataEventTable setSchema(java.lang.String schema) {
    this.schema = schema;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1MetadataEventTable set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1MetadataEventTable) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1MetadataEventTable clone() {
    return (GoogleCloudDataplexV1MetadataEventTable) super.clone();
  }
}
