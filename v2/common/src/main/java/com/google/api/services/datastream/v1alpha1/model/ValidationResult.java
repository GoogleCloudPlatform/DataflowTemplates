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
 * Contains the current validation results.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class ValidationResult extends com.google.api.client.json.GenericJson {

  /** Returns executed as well as not executed validations. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.List<Validation> validations;

  /**
   * Returns executed as well as not executed validations.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<Validation> getValidations() {
    return validations;
  }

  /**
   * Returns executed as well as not executed validations.
   *
   * @param validations validations or {@code null} for none
   */
  public ValidationResult setValidations(java.util.List<Validation> validations) {
    this.validations = validations;
    return this;
  }

  @Override
  public ValidationResult set(String fieldName, Object value) {
    return (ValidationResult) super.set(fieldName, value);
  }

  @Override
  public ValidationResult clone() {
    return (ValidationResult) super.clone();
  }
}
