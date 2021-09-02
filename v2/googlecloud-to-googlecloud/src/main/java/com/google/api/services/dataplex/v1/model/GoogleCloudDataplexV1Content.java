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
 * Content represents a user-visible notebook or a sql script
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1Content extends com.google.api.client.json.GenericJson {

  /** Output only. Content creation time. The value may be {@code null}. */
  @com.google.api.client.util.Key private String createTime;

  /** Required. Content data in string format. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String dataText;

  /** Optional. Description of the content. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String description;

  /** Optional. User defined labels for the content. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.String> labels;

  /**
   * Output only. The relative resource name of the content, of the form:
   * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id} The value
   * may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String name;

  /** Notebook related configurations. The value may be {@code null}. */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1ContentNotebook notebook;

  /**
   * Required. The path for the Content file, represented as directory structure. Unique within a
   * lake. Limited to alphanumerics, hyphens, underscores, dots and slashes. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.lang.String path;

  /** Sql Script related configurations. The value may be {@code null}. */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1ContentSqlScript sqlScript;

  /**
   * Output only. System generated globally unique ID for the content. This ID will be different if
   * the content is deleted and re-created with the same name. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String uid;

  /** Output only. The time when the content was last updated. The value may be {@code null}. */
  @com.google.api.client.util.Key private String updateTime;

  /**
   * Output only. Content creation time.
   *
   * @return value or {@code null} for none
   */
  public String getCreateTime() {
    return createTime;
  }

  /**
   * Output only. Content creation time.
   *
   * @param createTime createTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Content setCreateTime(String createTime) {
    this.createTime = createTime;
    return this;
  }

  /**
   * Required. Content data in string format.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDataText() {
    return dataText;
  }

  /**
   * Required. Content data in string format.
   *
   * @param dataText dataText or {@code null} for none
   */
  public GoogleCloudDataplexV1Content setDataText(java.lang.String dataText) {
    this.dataText = dataText;
    return this;
  }

  /**
   * Optional. Description of the content.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDescription() {
    return description;
  }

  /**
   * Optional. Description of the content.
   *
   * @param description description or {@code null} for none
   */
  public GoogleCloudDataplexV1Content setDescription(java.lang.String description) {
    this.description = description;
    return this;
  }

  /**
   * Optional. User defined labels for the content.
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.String> getLabels() {
    return labels;
  }

  /**
   * Optional. User defined labels for the content.
   *
   * @param labels labels or {@code null} for none
   */
  public GoogleCloudDataplexV1Content setLabels(java.util.Map<String, java.lang.String> labels) {
    this.labels = labels;
    return this;
  }

  /**
   * Output only. The relative resource name of the content, of the form:
   * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id}
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Output only. The relative resource name of the content, of the form:
   * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id}
   *
   * @param name name or {@code null} for none
   */
  public GoogleCloudDataplexV1Content setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * Notebook related configurations.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ContentNotebook getNotebook() {
    return notebook;
  }

  /**
   * Notebook related configurations.
   *
   * @param notebook notebook or {@code null} for none
   */
  public GoogleCloudDataplexV1Content setNotebook(GoogleCloudDataplexV1ContentNotebook notebook) {
    this.notebook = notebook;
    return this;
  }

  /**
   * Required. The path for the Content file, represented as directory structure. Unique within a
   * lake. Limited to alphanumerics, hyphens, underscores, dots and slashes.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getPath() {
    return path;
  }

  /**
   * Required. The path for the Content file, represented as directory structure. Unique within a
   * lake. Limited to alphanumerics, hyphens, underscores, dots and slashes.
   *
   * @param path path or {@code null} for none
   */
  public GoogleCloudDataplexV1Content setPath(java.lang.String path) {
    this.path = path;
    return this;
  }

  /**
   * Sql Script related configurations.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1ContentSqlScript getSqlScript() {
    return sqlScript;
  }

  /**
   * Sql Script related configurations.
   *
   * @param sqlScript sqlScript or {@code null} for none
   */
  public GoogleCloudDataplexV1Content setSqlScript(
      GoogleCloudDataplexV1ContentSqlScript sqlScript) {
    this.sqlScript = sqlScript;
    return this;
  }

  /**
   * Output only. System generated globally unique ID for the content. This ID will be different if
   * the content is deleted and re-created with the same name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getUid() {
    return uid;
  }

  /**
   * Output only. System generated globally unique ID for the content. This ID will be different if
   * the content is deleted and re-created with the same name.
   *
   * @param uid uid or {@code null} for none
   */
  public GoogleCloudDataplexV1Content setUid(java.lang.String uid) {
    this.uid = uid;
    return this;
  }

  /**
   * Output only. The time when the content was last updated.
   *
   * @return value or {@code null} for none
   */
  public String getUpdateTime() {
    return updateTime;
  }

  /**
   * Output only. The time when the content was last updated.
   *
   * @param updateTime updateTime or {@code null} for none
   */
  public GoogleCloudDataplexV1Content setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1Content set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1Content) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1Content clone() {
    return (GoogleCloudDataplexV1Content) super.clone();
  }
}
