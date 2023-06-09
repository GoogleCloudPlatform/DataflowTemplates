/*
 * Copyright (C) 2022 Google LLC
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
package com.google.api.services.datastream.v1.model;

/**
 * MySQL source configuration
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Datastream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class MysqlSourceConfig extends com.google.api.client.json.GenericJson {

  /** MySQL objects to exclude from the stream. The value may be {@code null}. */
  @com.google.api.client.util.Key private MysqlRdbms excludeObjects;

  /** MySQL objects to retrieve from the source. The value may be {@code null}. */
  @com.google.api.client.util.Key private MysqlRdbms includeObjects;

  /**
   * MySQL objects to exclude from the stream.
   *
   * @return value or {@code null} for none
   */
  public MysqlRdbms getExcludeObjects() {
    return excludeObjects;
  }

  /**
   * MySQL objects to exclude from the stream.
   *
   * @param excludeObjects excludeObjects or {@code null} for none
   */
  public MysqlSourceConfig setExcludeObjects(MysqlRdbms excludeObjects) {
    this.excludeObjects = excludeObjects;
    return this;
  }

  /**
   * MySQL objects to retrieve from the source.
   *
   * @return value or {@code null} for none
   */
  public MysqlRdbms getIncludeObjects() {
    return includeObjects;
  }

  /**
   * MySQL objects to retrieve from the source.
   *
   * @param includeObjects includeObjects or {@code null} for none
   */
  public MysqlSourceConfig setIncludeObjects(MysqlRdbms includeObjects) {
    this.includeObjects = includeObjects;
    return this;
  }

  @Override
  public MysqlSourceConfig set(String fieldName, Object value) {
    return (MysqlSourceConfig) super.set(fieldName, value);
  }

  @Override
  public MysqlSourceConfig clone() {
    return (MysqlSourceConfig) super.clone();
  }
}
