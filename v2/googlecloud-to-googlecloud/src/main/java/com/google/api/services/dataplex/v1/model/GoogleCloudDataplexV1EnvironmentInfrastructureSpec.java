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
 * Configuration for the underlying infrastructure used to run workloads.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1EnvironmentInfrastructureSpec
    extends com.google.api.client.json.GenericJson {

  /**
   * Optional. Compute resources needed for analyze interactive workloads. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1EnvironmentInfrastructureSpecComputeResources compute;

  /**
   * Required. Software Runtime Configuration for analyze interactive workloads. The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1EnvironmentInfrastructureSpecOsImageRuntime osImage;

  /**
   * Optional. Compute resources needed for analyze interactive workloads.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1EnvironmentInfrastructureSpecComputeResources getCompute() {
    return compute;
  }

  /**
   * Optional. Compute resources needed for analyze interactive workloads.
   *
   * @param compute compute or {@code null} for none
   */
  public GoogleCloudDataplexV1EnvironmentInfrastructureSpec setCompute(
      GoogleCloudDataplexV1EnvironmentInfrastructureSpecComputeResources compute) {
    this.compute = compute;
    return this;
  }

  /**
   * Required. Software Runtime Configuration for analyze interactive workloads.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1EnvironmentInfrastructureSpecOsImageRuntime getOsImage() {
    return osImage;
  }

  /**
   * Required. Software Runtime Configuration for analyze interactive workloads.
   *
   * @param osImage osImage or {@code null} for none
   */
  public GoogleCloudDataplexV1EnvironmentInfrastructureSpec setOsImage(
      GoogleCloudDataplexV1EnvironmentInfrastructureSpecOsImageRuntime osImage) {
    this.osImage = osImage;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1EnvironmentInfrastructureSpec set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1EnvironmentInfrastructureSpec) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1EnvironmentInfrastructureSpec clone() {
    return (GoogleCloudDataplexV1EnvironmentInfrastructureSpec) super.clone();
  }
}
