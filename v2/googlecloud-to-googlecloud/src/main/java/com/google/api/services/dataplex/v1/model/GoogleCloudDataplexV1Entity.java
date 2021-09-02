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
 * Represents tables and fileset metadata contained within a Zone.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1Entity extends com.google.api.client.json.GenericJson {

  /**
   * Required. The name of the asset associated with the storage location containing the entity
   * data. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String asset;

  /** Output only. The name of the associated Data Catalog entry. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String catalogEntry;

  /** Output only. Metadata stores the entity is compatible with. The value may be {@code null}. */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1EntityCompatibilityStatus compatibility;

  /** Output only. Metadata stores the entity is compatible with. The value may be {@code null}. */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1EntityCompatibility compatibilityDeprecated;

  /** Output only. The time when the entity was created. The value may be {@code null}. */
  @com.google.api.client.util.Key private String createTime;

  /**
   * Required. Immutable. The storage path of the entity data. For Cloud Storage data, this is the
   * fully-qualified path to the entity, such as gs://bucket/path/to/data. For BigQuery data, this
   * is the name of the table resource such as
   * projects/project_id/datasets/dataset_id/tables/table_id. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String dataPath;

  /**
   * Optional. The set of items within the data path constituting the data in the entity represented
   * as a glob path. Eg. gs://bucket/path/to/data*.csv. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String dataPathPattern;

  /** Optional. User friendly longer description text. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String description;

  /** Optional. User friendly display name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String displayName;

  /**
   * Optional. The etag for this entity. Required for update requests, it must match the server's
   * etag. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String etag;

  /**
   * Required. Identifies the storage format of the entity data. This does not apply to entities
   * with data stored in BigQuery. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1StorageFormat format;

  /**
   * Required. A user provide entity ID. It is mutable and will be used for the published table
   * name. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String id;

  /**
   * Output only. Immutable. The resource name of the entity, of the form: projects/{project_number}
   * /locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity} The {entity} is a
   * generated unique ID. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String name;

  /**
   * Required. The description of the data structure and layout. Schema is not included in list
   * responses and only included in SCHEMA and FULL entity views of get entity response. The value
   * may be {@code null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1Schema schema;

  /** Required. Identifies the storage system of the entity data. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String system;

  /** Required. The type of entity. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String type;

  /** Output only. The time when the entity was last updated. The value may be {@code null}. */
  @com.google.api.client.util.Key private String updateTime;

  /**
   * Required. The name of the asset associated with the storage location containing the entity
   * data.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getAsset() {
    return asset;
  }

  /**
   * Required. The name of the asset associated with the storage location containing the entity
   * data.
   *
   * @param asset asset or {@code null} for none
   */
  public GoogleCloudDataplexV1Entity setAsset(java.lang.String asset) {
    this.asset = asset;
    return this;
  }

  /**
   * Output only. The name of the associated Data Catalog entry.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getCatalogEntry() {
    return catalogEntry;
  }

  /**
   * Output only. The name of the associated Data Catalog entry.
   *
   * @param catalogEntry catalogEntry or {@code null} for none
   */
  public GoogleCloudDataplexV1Entity setCatalogEntry(java.lang.String catalogEntry) {
    this.catalogEntry = catalogEntry;
    return this;
  }

  /**
   * Output only. Metadata stores the entity is compatible with.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1EntityCompatibilityStatus getCompatibility() {
    return compatibility;
  }

  /**
   * Output only. Metadata stores the entity is compatible with.
   *
   * @param compatibility compatibility or {@code null} for none
   */
  public GoogleCloudDataplexV1Entity setCompatibility(
      GoogleCloudDataplexV1EntityCompatibilityStatus compatibility) {
    this.compatibility = compatibility;
    return this;
  }

  /**
   * Output only. Metadata stores the entity is compatible with.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1EntityCompatibility getCompatibilityDeprecated() {
    return compatibilityDeprecated;
  }

  /**
   * Output only. Metadata stores the entity is compatible with.
   *
   * @param compatibilityDeprecated compatibilityDeprecated or {@code null} for none
   */
  public GoogleCloudDataplexV1Entity setCompatibilityDeprecated(
      GoogleCloudDataplexV1EntityCompatibility compatibilityDeprecated) {
    this.compatibilityDeprecated = compatibilityDeprecated;
    return this;
  }

  /**
   * Output only. The time when the entity was created.
   *
   * @return value or {@code null} for none
   */
  public String getCreateTime() {
    return createTime;
  }

  /**
   * Output only. The time when the entity was created.
   *
   * @param createTime createTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Entity setCreateTime(String createTime) {
    this.createTime = createTime;
    return this;
  }

  /**
   * Required. Immutable. The storage path of the entity data. For Cloud Storage data, this is the
   * fully-qualified path to the entity, such as gs://bucket/path/to/data. For BigQuery data, this
   * is the name of the table resource such as
   * projects/project_id/datasets/dataset_id/tables/table_id.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDataPath() {
    return dataPath;
  }

  /**
   * Required. Immutable. The storage path of the entity data. For Cloud Storage data, this is the
   * fully-qualified path to the entity, such as gs://bucket/path/to/data. For BigQuery data, this
   * is the name of the table resource such as
   * projects/project_id/datasets/dataset_id/tables/table_id.
   *
   * @param dataPath dataPath or {@code null} for none
   */
  public GoogleCloudDataplexV1Entity setDataPath(java.lang.String dataPath) {
    this.dataPath = dataPath;
    return this;
  }

  /**
   * Optional. The set of items within the data path constituting the data in the entity represented
   * as a glob path. Eg. gs://bucket/path/to/data*.csv.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDataPathPattern() {
    return dataPathPattern;
  }

  /**
   * Optional. The set of items within the data path constituting the data in the entity represented
   * as a glob path. Eg. gs://bucket/path/to/data*.csv.
   *
   * @param dataPathPattern dataPathPattern or {@code null} for none
   */
  public GoogleCloudDataplexV1Entity setDataPathPattern(java.lang.String dataPathPattern) {
    this.dataPathPattern = dataPathPattern;
    return this;
  }

  /**
   * Optional. User friendly longer description text.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDescription() {
    return description;
  }

  /**
   * Optional. User friendly longer description text.
   *
   * @param description description or {@code null} for none
   */
  public GoogleCloudDataplexV1Entity setDescription(java.lang.String description) {
    this.description = description;
    return this;
  }

  /**
   * Optional. User friendly display name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDisplayName() {
    return displayName;
  }

  /**
   * Optional. User friendly display name.
   *
   * @param displayName displayName or {@code null} for none
   */
  public GoogleCloudDataplexV1Entity setDisplayName(java.lang.String displayName) {
    this.displayName = displayName;
    return this;
  }

  /**
   * Optional. The etag for this entity. Required for update requests, it must match the server's
   * etag.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getEtag() {
    return etag;
  }

  /**
   * Optional. The etag for this entity. Required for update requests, it must match the server's
   * etag.
   *
   * @param etag etag or {@code null} for none
   */
  public GoogleCloudDataplexV1Entity setEtag(java.lang.String etag) {
    this.etag = etag;
    return this;
  }

  /**
   * Required. Identifies the storage format of the entity data. This does not apply to entities
   * with data stored in BigQuery.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1StorageFormat getFormat() {
    return format;
  }

  /**
   * Required. Identifies the storage format of the entity data. This does not apply to entities
   * with data stored in BigQuery.
   *
   * @param format format or {@code null} for none
   */
  public GoogleCloudDataplexV1Entity setFormat(GoogleCloudDataplexV1StorageFormat format) {
    this.format = format;
    return this;
  }

  /**
   * Required. A user provide entity ID. It is mutable and will be used for the published table
   * name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getId() {
    return id;
  }

  /**
   * Required. A user provide entity ID. It is mutable and will be used for the published table
   * name.
   *
   * @param id id or {@code null} for none
   */
  public GoogleCloudDataplexV1Entity setId(java.lang.String id) {
    this.id = id;
    return this;
  }

  /**
   * Output only. Immutable. The resource name of the entity, of the form: projects/{project_number}
   * /locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity} The {entity} is a
   * generated unique ID.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Output only. Immutable. The resource name of the entity, of the form: projects/{project_number}
   * /locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity} The {entity} is a
   * generated unique ID.
   *
   * @param name name or {@code null} for none
   */
  public GoogleCloudDataplexV1Entity setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * Required. The description of the data structure and layout. Schema is not included in list
   * responses and only included in SCHEMA and FULL entity views of get entity response.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1Schema getSchema() {
    return schema;
  }

  /**
   * Required. The description of the data structure and layout. Schema is not included in list
   * responses and only included in SCHEMA and FULL entity views of get entity response.
   *
   * @param schema schema or {@code null} for none
   */
  public GoogleCloudDataplexV1Entity setSchema(GoogleCloudDataplexV1Schema schema) {
    this.schema = schema;
    return this;
  }

  /**
   * Required. Identifies the storage system of the entity data.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getSystem() {
    return system;
  }

  /**
   * Required. Identifies the storage system of the entity data.
   *
   * @param system system or {@code null} for none
   */
  public GoogleCloudDataplexV1Entity setSystem(java.lang.String system) {
    this.system = system;
    return this;
  }

  /**
   * Required. The type of entity.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getType() {
    return type;
  }

  /**
   * Required. The type of entity.
   *
   * @param type type or {@code null} for none
   */
  public GoogleCloudDataplexV1Entity setType(java.lang.String type) {
    this.type = type;
    return this;
  }

  /**
   * Output only. The time when the entity was last updated.
   *
   * @return value or {@code null} for none
   */
  public String getUpdateTime() {
    return updateTime;
  }

  /**
   * Output only. The time when the entity was last updated.
   *
   * @param updateTime updateTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Entity setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1Entity set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1Entity) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1Entity clone() {
    return (GoogleCloudDataplexV1Entity) super.clone();
  }
}
