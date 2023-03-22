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
public final class GoogleCloudDataplexV1TaskInfrastructureSpec
    extends com.google.api.client.json.GenericJson {

  /**
   * Compute resources needed for a Task when using Dataproc Serverless. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1TaskInfrastructureSpecBatchComputeResources batch;

  /**
   * Compute resources needed for a Task when using Dataproc on GCE. The value may be {@code null}.
   */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1TaskInfrastructureSpecClusterComputeResources cluster;

  /** Container Image Runtime Configuration. The value may be {@code null}. */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1TaskInfrastructureSpecContainerImageRuntime containerImage;

  /** OS Image Runtime Configuration. The value may be {@code null}. */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1TaskInfrastructureSpecOsImageRuntime osImage;

  /** The value may be {@code null}. */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1TaskInfrastructureSpecVpcNetwork vpcNetwork;

  /**
   * Compute resources needed for a Task when using Dataproc Serverless.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpecBatchComputeResources getBatch() {
    return batch;
  }

  /**
   * Compute resources needed for a Task when using Dataproc Serverless.
   *
   * @param batch batch or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpec setBatch(
      GoogleCloudDataplexV1TaskInfrastructureSpecBatchComputeResources batch) {
    this.batch = batch;
    return this;
  }

  /**
   * Compute resources needed for a Task when using Dataproc on GCE.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpecClusterComputeResources getCluster() {
    return cluster;
  }

  /**
   * Compute resources needed for a Task when using Dataproc on GCE.
   *
   * @param cluster cluster or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpec setCluster(
      GoogleCloudDataplexV1TaskInfrastructureSpecClusterComputeResources cluster) {
    this.cluster = cluster;
    return this;
  }

  /**
   * Container Image Runtime Configuration.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpecContainerImageRuntime getContainerImage() {
    return containerImage;
  }

  /**
   * Container Image Runtime Configuration.
   *
   * @param containerImage containerImage or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpec setContainerImage(
      GoogleCloudDataplexV1TaskInfrastructureSpecContainerImageRuntime containerImage) {
    this.containerImage = containerImage;
    return this;
  }

  /**
   * OS Image Runtime Configuration.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpecOsImageRuntime getOsImage() {
    return osImage;
  }

  /**
   * OS Image Runtime Configuration.
   *
   * @param osImage osImage or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpec setOsImage(
      GoogleCloudDataplexV1TaskInfrastructureSpecOsImageRuntime osImage) {
    this.osImage = osImage;
    return this;
  }

  /**
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpecVpcNetwork getVpcNetwork() {
    return vpcNetwork;
  }

  /**
   * @param vpcNetwork vpcNetwork or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpec setVpcNetwork(
      GoogleCloudDataplexV1TaskInfrastructureSpecVpcNetwork vpcNetwork) {
    this.vpcNetwork = vpcNetwork;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1TaskInfrastructureSpec set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1TaskInfrastructureSpec) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1TaskInfrastructureSpec clone() {
    return (GoogleCloudDataplexV1TaskInfrastructureSpec) super.clone();
  }
}
