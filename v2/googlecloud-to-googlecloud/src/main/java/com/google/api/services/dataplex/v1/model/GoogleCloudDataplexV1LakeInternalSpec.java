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
 * Settings for development/test use by the Dataplex team. This is not exposed to external users of
 * the API.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1LakeInternalSpec
    extends com.google.api.client.json.GenericJson {

  /**
   * Immutable. Settings for the SLM instance associated with the lake. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private GoogleCloudDataplexV1LakeInternalSpecSlmTemplate slm;

  /**
   * Immutable. Settings for the SLM instance associated with the lake.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1LakeInternalSpecSlmTemplate getSlm() {
    return slm;
  }

  /**
   * Immutable. Settings for the SLM instance associated with the lake.
   *
   * @param slm slm or {@code null} for none
   */
  public GoogleCloudDataplexV1LakeInternalSpec setSlm(
      GoogleCloudDataplexV1LakeInternalSpecSlmTemplate slm) {
    this.slm = slm;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1LakeInternalSpec set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1LakeInternalSpec) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1LakeInternalSpec clone() {
    return (GoogleCloudDataplexV1LakeInternalSpec) super.clone();
  }
}
