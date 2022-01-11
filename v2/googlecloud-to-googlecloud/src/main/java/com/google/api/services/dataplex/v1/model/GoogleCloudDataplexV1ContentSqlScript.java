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
 * Configuration for the Sql Script content.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1ContentSqlScript
    extends com.google.api.client.json.GenericJson {

  /** Required. Query Engine to be used for the Sql Query. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String engine;

  /**
   * Required. Query Engine to be used for the Sql Query.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getEngine() {
    return engine;
  }

  /**
   * Required. Query Engine to be used for the Sql Query.
   *
   * @param engine engine or {@code null} for none
   */
  public GoogleCloudDataplexV1ContentSqlScript setEngine(java.lang.String engine) {
    this.engine = engine;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1ContentSqlScript set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1ContentSqlScript) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1ContentSqlScript clone() {
    return (GoogleCloudDataplexV1ContentSqlScript) super.clone();
  }
}
