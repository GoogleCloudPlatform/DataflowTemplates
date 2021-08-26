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
 * Google Cloud Storage Destination configuration
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GcsDestinationConfig extends com.google.api.client.json.GenericJson {

  /** AVRO file format configuration. The value may be {@code null}. */
  @com.google.api.client.util.Key private AvroFileFormat avroFileFormat;

  /**
   * The maximum duration for which new events are added before a file is closed and a new file is
   * created. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private String fileRotationInterval;

  /** The maximum file size to be saved in the bucket. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Integer fileRotationMb;

  /**
   * File format that data should be written to. Deprecated field (b/169501737) - use file_format
   * instead. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String gcsFileFormat;

  /** JSON file format configuration. The value may be {@code null}. */
  @com.google.api.client.util.Key private JsonFileFormat jsonFileFormat;

  /** Path inside the Cloud Storage bucket to write data to. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String path;

  /**
   * AVRO file format configuration.
   *
   * @return value or {@code null} for none
   */
  public AvroFileFormat getAvroFileFormat() {
    return avroFileFormat;
  }

  /**
   * AVRO file format configuration.
   *
   * @param avroFileFormat avroFileFormat or {@code null} for none
   */
  public GcsDestinationConfig setAvroFileFormat(AvroFileFormat avroFileFormat) {
    this.avroFileFormat = avroFileFormat;
    return this;
  }

  /**
   * The maximum duration for which new events are added before a file is closed and a new file is
   * created.
   *
   * @return value or {@code null} for none
   */
  public String getFileRotationInterval() {
    return fileRotationInterval;
  }

  /**
   * The maximum duration for which new events are added before a file is closed and a new file is
   * created.
   *
   * @param fileRotationInterval fileRotationInterval or {@code null} for none
   */
  public GcsDestinationConfig setFileRotationInterval(String fileRotationInterval) {
    this.fileRotationInterval = fileRotationInterval;
    return this;
  }

  /**
   * The maximum file size to be saved in the bucket.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Integer getFileRotationMb() {
    return fileRotationMb;
  }

  /**
   * The maximum file size to be saved in the bucket.
   *
   * @param fileRotationMb fileRotationMb or {@code null} for none
   */
  public GcsDestinationConfig setFileRotationMb(java.lang.Integer fileRotationMb) {
    this.fileRotationMb = fileRotationMb;
    return this;
  }

  /**
   * File format that data should be written to. Deprecated field (b/169501737) - use file_format
   * instead.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getGcsFileFormat() {
    return gcsFileFormat;
  }

  /**
   * File format that data should be written to. Deprecated field (b/169501737) - use file_format
   * instead.
   *
   * @param gcsFileFormat gcsFileFormat or {@code null} for none
   */
  public GcsDestinationConfig setGcsFileFormat(java.lang.String gcsFileFormat) {
    this.gcsFileFormat = gcsFileFormat;
    return this;
  }

  /**
   * JSON file format configuration.
   *
   * @return value or {@code null} for none
   */
  public JsonFileFormat getJsonFileFormat() {
    return jsonFileFormat;
  }

  /**
   * JSON file format configuration.
   *
   * @param jsonFileFormat jsonFileFormat or {@code null} for none
   */
  public GcsDestinationConfig setJsonFileFormat(JsonFileFormat jsonFileFormat) {
    this.jsonFileFormat = jsonFileFormat;
    return this;
  }

  /**
   * Path inside the Cloud Storage bucket to write data to.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getPath() {
    return path;
  }

  /**
   * Path inside the Cloud Storage bucket to write data to.
   *
   * @param path path or {@code null} for none
   */
  public GcsDestinationConfig setPath(java.lang.String path) {
    this.path = path;
    return this;
  }

  @Override
  public GcsDestinationConfig set(String fieldName, Object value) {
    return (GcsDestinationConfig) super.set(fieldName, value);
  }

  @Override
  public GcsDestinationConfig clone() {
    return (GcsDestinationConfig) super.clone();
  }
}
