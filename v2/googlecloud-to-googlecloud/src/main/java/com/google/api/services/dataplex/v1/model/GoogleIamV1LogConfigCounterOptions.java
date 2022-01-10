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
 * Increment a streamz counter with the specified metric and field names.Metric names should start
 * with a '/', generally be lowercase-only, and end in "_count". Field names should not contain an
 * initial slash. The actual exported metric names will have "/iam/policy" prepended.Field names
 * correspond to IAM request parameters and field values are their respective values.Supported field
 * names: - "authority", which is "token" if IAMContext.token is present, otherwise the value of
 * IAMContext.authority_selector if present, and otherwise a representation of IAMContext.principal;
 * or - "iam_principal", a representation of IAMContext.principal even if a token or authority
 * selector is present; or - "" (empty string), resulting in a counter with no fields.Examples:
 * counter { metric: "/debug_access_count" field: "iam_principal" } ==> increment counter
 * /iam/policy/debug_access_count {iam_principal=value of IAMContext.principal}
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleIamV1LogConfigCounterOptions
    extends com.google.api.client.json.GenericJson {

  /** Custom fields. The value may be {@code null}. */
  @com.google.api.client.util.Key
  private java.util.List<GoogleIamV1LogConfigCounterOptionsCustomField> customFields;

  /** The field value to attribute. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String field;

  /** The metric to update. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String metric;

  /**
   * Custom fields.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<GoogleIamV1LogConfigCounterOptionsCustomField> getCustomFields() {
    return customFields;
  }

  /**
   * Custom fields.
   *
   * @param customFields customFields or {@code null} for none
   */
  public GoogleIamV1LogConfigCounterOptions setCustomFields(
      java.util.List<GoogleIamV1LogConfigCounterOptionsCustomField> customFields) {
    this.customFields = customFields;
    return this;
  }

  /**
   * The field value to attribute.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getField() {
    return field;
  }

  /**
   * The field value to attribute.
   *
   * @param field field or {@code null} for none
   */
  public GoogleIamV1LogConfigCounterOptions setField(java.lang.String field) {
    this.field = field;
    return this;
  }

  /**
   * The metric to update.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getMetric() {
    return metric;
  }

  /**
   * The metric to update.
   *
   * @param metric metric or {@code null} for none
   */
  public GoogleIamV1LogConfigCounterOptions setMetric(java.lang.String metric) {
    this.metric = metric;
    return this;
  }

  @Override
  public GoogleIamV1LogConfigCounterOptions set(String fieldName, Object value) {
    return (GoogleIamV1LogConfigCounterOptions) super.set(fieldName, value);
  }

  @Override
  public GoogleIamV1LogConfigCounterOptions clone() {
    return (GoogleIamV1LogConfigCounterOptions) super.clone();
  }
}
