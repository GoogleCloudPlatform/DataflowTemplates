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
 * JSON file format configuration.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class JsonFileFormat extends com.google.api.client.json.GenericJson {

  /** Compression of the loaded JSON file. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String compression;

  /** The schema file format along JSON data files. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String schemaFileFormat;

  /**
   * Compression of the loaded JSON file.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getCompression() {
    return compression;
  }

  /**
   * Compression of the loaded JSON file.
   *
   * @param compression compression or {@code null} for none
   */
  public JsonFileFormat setCompression(java.lang.String compression) {
    this.compression = compression;
    return this;
  }

  /**
   * The schema file format along JSON data files.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getSchemaFileFormat() {
    return schemaFileFormat;
  }

  /**
   * The schema file format along JSON data files.
   *
   * @param schemaFileFormat schemaFileFormat or {@code null} for none
   */
  public JsonFileFormat setSchemaFileFormat(java.lang.String schemaFileFormat) {
    this.schemaFileFormat = schemaFileFormat;
    return this;
  }

  @Override
  public JsonFileFormat set(String fieldName, Object value) {
    return (JsonFileFormat) super.set(fieldName, value);
  }

  @Override
  public JsonFileFormat clone() {
    return (JsonFileFormat) super.clone();
  }
}
