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
 * Request message for SetIamPolicy method.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleIamV1SetIamPolicyRequest extends com.google.api.client.json.GenericJson {

  /**
   * REQUIRED: The complete policy to be applied to the resource. The size of the policy is limited
   * to a few 10s of KB. An empty policy is a valid policy but certain Cloud Platform services (such
   * as Projects) might reject them. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private GoogleIamV1Policy policy;

  /**
   * OPTIONAL: A FieldMask specifying which fields of the policy to modify. Only the fields in the
   * mask will be modified. If no mask is provided, the following default mask is used:paths:
   * "bindings, etag" The value may be {@code null}.
   */
  @com.google.api.client.util.Key private String updateMask;

  /**
   * REQUIRED: The complete policy to be applied to the resource. The size of the policy is limited
   * to a few 10s of KB. An empty policy is a valid policy but certain Cloud Platform services (such
   * as Projects) might reject them.
   *
   * @return value or {@code null} for none
   */
  public GoogleIamV1Policy getPolicy() {
    return policy;
  }

  /**
   * REQUIRED: The complete policy to be applied to the resource. The size of the policy is limited
   * to a few 10s of KB. An empty policy is a valid policy but certain Cloud Platform services (such
   * as Projects) might reject them.
   *
   * @param policy policy or {@code null} for none
   */
  public GoogleIamV1SetIamPolicyRequest setPolicy(GoogleIamV1Policy policy) {
    this.policy = policy;
    return this;
  }

  /**
   * OPTIONAL: A FieldMask specifying which fields of the policy to modify. Only the fields in the
   * mask will be modified. If no mask is provided, the following default mask is used:paths:
   * "bindings, etag"
   *
   * @return value or {@code null} for none
   */
  public String getUpdateMask() {
    return updateMask;
  }

  /**
   * OPTIONAL: A FieldMask specifying which fields of the policy to modify. Only the fields in the
   * mask will be modified. If no mask is provided, the following default mask is used:paths:
   * "bindings, etag"
   *
   * @param updateMask updateMask or {@code null} for none
   */
  public GoogleIamV1SetIamPolicyRequest setUpdateMask(String updateMask) {
    this.updateMask = updateMask;
    return this;
  }

  @Override
  public GoogleIamV1SetIamPolicyRequest set(String fieldName, Object value) {
    return (GoogleIamV1SetIamPolicyRequest) super.set(fieldName, value);
  }

  @Override
  public GoogleIamV1SetIamPolicyRequest clone() {
    return (GoogleIamV1SetIamPolicyRequest) super.clone();
  }
}
