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
 * A condition to be met.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleIamV1Condition extends com.google.api.client.json.GenericJson {

  /** Trusted attributes supplied by the IAM system. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String iam;

  /** An operator to apply the subject with. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String op;

  /** Trusted attributes discharged by the service. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String svc;

  /**
   * Trusted attributes supplied by any service that owns resources and uses the IAM system for
   * access control. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String sys;

  /** The objects of the condition. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> values;

  /**
   * Trusted attributes supplied by the IAM system.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getIam() {
    return iam;
  }

  /**
   * Trusted attributes supplied by the IAM system.
   *
   * @param iam iam or {@code null} for none
   */
  public GoogleIamV1Condition setIam(java.lang.String iam) {
    this.iam = iam;
    return this;
  }

  /**
   * An operator to apply the subject with.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getOp() {
    return op;
  }

  /**
   * An operator to apply the subject with.
   *
   * @param op op or {@code null} for none
   */
  public GoogleIamV1Condition setOp(java.lang.String op) {
    this.op = op;
    return this;
  }

  /**
   * Trusted attributes discharged by the service.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getSvc() {
    return svc;
  }

  /**
   * Trusted attributes discharged by the service.
   *
   * @param svc svc or {@code null} for none
   */
  public GoogleIamV1Condition setSvc(java.lang.String svc) {
    this.svc = svc;
    return this;
  }

  /**
   * Trusted attributes supplied by any service that owns resources and uses the IAM system for
   * access control.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getSys() {
    return sys;
  }

  /**
   * Trusted attributes supplied by any service that owns resources and uses the IAM system for
   * access control.
   *
   * @param sys sys or {@code null} for none
   */
  public GoogleIamV1Condition setSys(java.lang.String sys) {
    this.sys = sys;
    return this;
  }

  /**
   * The objects of the condition.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getValues() {
    return values;
  }

  /**
   * The objects of the condition.
   *
   * @param values values or {@code null} for none
   */
  public GoogleIamV1Condition setValues(java.util.List<java.lang.String> values) {
    this.values = values;
    return this;
  }

  @Override
  public GoogleIamV1Condition set(String fieldName, Object value) {
    return (GoogleIamV1Condition) super.set(fieldName, value);
  }

  @Override
  public GoogleIamV1Condition clone() {
    return (GoogleIamV1Condition) super.clone();
  }
}
