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
 * Config for running ad-hoc discovery tasks.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1TaskDiscoveryTaskConfig
    extends com.google.api.client.json.GenericJson {

  /**
   * Required. The asset to run discovery on, of the form: projects/{project_number}/locations/{loca
   * tion_id}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id} The asset must be in the same lake
   * as the task. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String assetName;

  /** Optional. INCREMENTAL discovery is the default strategy. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String scan;

  /**
   * Required. The asset to run discovery on, of the form: projects/{project_number}/locations/{loca
   * tion_id}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id} The asset must be in the same lake
   * as the task.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getAssetName() {
    return assetName;
  }

  /**
   * Required. The asset to run discovery on, of the form: projects/{project_number}/locations/{loca
   * tion_id}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id} The asset must be in the same lake
   * as the task.
   *
   * @param assetName assetName or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskDiscoveryTaskConfig setAssetName(java.lang.String assetName) {
    this.assetName = assetName;
    return this;
  }

  /**
   * Optional. INCREMENTAL discovery is the default strategy.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getScan() {
    return scan;
  }

  /**
   * Optional. INCREMENTAL discovery is the default strategy.
   *
   * @param scan scan or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskDiscoveryTaskConfig setScan(java.lang.String scan) {
    this.scan = scan;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1TaskDiscoveryTaskConfig set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1TaskDiscoveryTaskConfig) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1TaskDiscoveryTaskConfig clone() {
    return (GoogleCloudDataplexV1TaskDiscoveryTaskConfig) super.clone();
  }
}
