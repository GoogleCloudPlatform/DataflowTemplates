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
 * Failed to apply security policy to the managed resource(s) under a lake, zone or an asset. For a
 * lake or zone resource, one or more underlying assets has a failure applying security policy to
 * the associated managed resource.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1ActionFailedSecurityPolicyApply
    extends com.google.api.client.json.GenericJson {

  /**
   * Resource name of one of the assets with failing security policy application. Populated for a
   * lake or zone resource only. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String asset;

  /**
   * Resource name of one of the assets with failing security policy application. Populated for a
   * lake or zone resource only.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getAsset() {
    return asset;
  }

  /**
   * Resource name of one of the assets with failing security policy application. Populated for a
   * lake or zone resource only.
   *
   * @param asset asset or {@code null} for none
   */
  public GoogleCloudDataplexV1ActionFailedSecurityPolicyApply setAsset(java.lang.String asset) {
    this.asset = asset;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1ActionFailedSecurityPolicyApply set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1ActionFailedSecurityPolicyApply) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1ActionFailedSecurityPolicyApply clone() {
    return (GoogleCloudDataplexV1ActionFailedSecurityPolicyApply) super.clone();
  }
}
