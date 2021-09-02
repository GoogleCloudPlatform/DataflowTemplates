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
 * Describes the format of the data within its storage location.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1StorageFormat
    extends com.google.api.client.json.GenericJson {

  /**
   * Optional. The compression type associated with the stored data. If this is not specified, the
   * data is uncompressed. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String compressionFormat;

  /** Optional. Additional information about CSV formatted data. The value may be {@code null}. */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1StorageFormatCsvOptions csv;

  /**
   * Required. The data format associated with the stored data, represented using content type
   * values. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String format;

  /** Optional. Additional information about CSV formatted data. The value may be {@code null}. */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1StorageFormatJsonOptions json;

  /**
   * Optional. The mime type descriptor for the data. This is valid for formats other than UNKNOWN
   * and MIXED. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String mimeType;

  /**
   * Required. Whether the format and content type are user-managed or managed by the service. User
   * managed formats are not automatically updated by discovery jobs. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.Boolean userManaged;

  /**
   * Optional. The compression type associated with the stored data. If this is not specified, the
   * data is uncompressed.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getCompressionFormat() {
    return compressionFormat;
  }

  /**
   * Optional. The compression type associated with the stored data. If this is not specified, the
   * data is uncompressed.
   *
   * @param compressionFormat compressionFormat or {@code null} for none
   */
  public GoogleCloudDataplexV1StorageFormat setCompressionFormat(
      java.lang.String compressionFormat) {
    this.compressionFormat = compressionFormat;
    return this;
  }

  /**
   * Optional. Additional information about CSV formatted data.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1StorageFormatCsvOptions getCsv() {
    return csv;
  }

  /**
   * Optional. Additional information about CSV formatted data.
   *
   * @param csv csv or {@code null} for none
   */
  public GoogleCloudDataplexV1StorageFormat setCsv(
      GoogleCloudDataplexV1StorageFormatCsvOptions csv) {
    this.csv = csv;
    return this;
  }

  /**
   * Required. The data format associated with the stored data, represented using content type
   * values.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getFormat() {
    return format;
  }

  /**
   * Required. The data format associated with the stored data, represented using content type
   * values.
   *
   * @param format format or {@code null} for none
   */
  public GoogleCloudDataplexV1StorageFormat setFormat(java.lang.String format) {
    this.format = format;
    return this;
  }

  /**
   * Optional. Additional information about CSV formatted data.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1StorageFormatJsonOptions getJson() {
    return json;
  }

  /**
   * Optional. Additional information about CSV formatted data.
   *
   * @param json json or {@code null} for none
   */
  public GoogleCloudDataplexV1StorageFormat setJson(
      GoogleCloudDataplexV1StorageFormatJsonOptions json) {
    this.json = json;
    return this;
  }

  /**
   * Optional. The mime type descriptor for the data. This is valid for formats other than UNKNOWN
   * and MIXED.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getMimeType() {
    return mimeType;
  }

  /**
   * Optional. The mime type descriptor for the data. This is valid for formats other than UNKNOWN
   * and MIXED.
   *
   * @param mimeType mimeType or {@code null} for none
   */
  public GoogleCloudDataplexV1StorageFormat setMimeType(java.lang.String mimeType) {
    this.mimeType = mimeType;
    return this;
  }

  /**
   * Required. Whether the format and content type are user-managed or managed by the service. User
   * managed formats are not automatically updated by discovery jobs.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getUserManaged() {
    return userManaged;
  }

  /**
   * Required. Whether the format and content type are user-managed or managed by the service. User
   * managed formats are not automatically updated by discovery jobs.
   *
   * @param userManaged userManaged or {@code null} for none
   */
  public GoogleCloudDataplexV1StorageFormat setUserManaged(java.lang.Boolean userManaged) {
    this.userManaged = userManaged;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1StorageFormat set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1StorageFormat) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1StorageFormat clone() {
    return (GoogleCloudDataplexV1StorageFormat) super.clone();
  }
}
