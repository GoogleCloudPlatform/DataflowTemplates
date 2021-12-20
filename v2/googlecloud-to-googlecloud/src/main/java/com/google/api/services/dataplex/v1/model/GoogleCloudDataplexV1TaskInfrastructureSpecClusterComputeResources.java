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
 * Cluster Compute resources associated with the task.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1TaskInfrastructureSpecClusterComputeResources
    extends com.google.api.client.json.GenericJson {

  /** Optional. Size in GB of the disk. Default is 100 GB. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Integer diskSizeGb;

  /**
   * Optional. Max configurable nodes. If max_node_count > node_count, then auto-scaling is enabled.
   * The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.Integer maxNodeCount;

  /**
   * Optional. Total number of worker nodes in the cluster. 1 master + N workers. The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.Integer nodeCount;

  /**
   * Optional. Size in GB of the disk. Default is 100 GB.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Integer getDiskSizeGb() {
    return diskSizeGb;
  }

  /**
   * Optional. Size in GB of the disk. Default is 100 GB.
   *
   * @param diskSizeGb diskSizeGb or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpecClusterComputeResources setDiskSizeGb(
      java.lang.Integer diskSizeGb) {
    this.diskSizeGb = diskSizeGb;
    return this;
  }

  /**
   * Optional. Max configurable nodes. If max_node_count > node_count, then auto-scaling is enabled.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Integer getMaxNodeCount() {
    return maxNodeCount;
  }

  /**
   * Optional. Max configurable nodes. If max_node_count > node_count, then auto-scaling is enabled.
   *
   * @param maxNodeCount maxNodeCount or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpecClusterComputeResources setMaxNodeCount(
      java.lang.Integer maxNodeCount) {
    this.maxNodeCount = maxNodeCount;
    return this;
  }

  /**
   * Optional. Total number of worker nodes in the cluster. 1 master + N workers.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Integer getNodeCount() {
    return nodeCount;
  }

  /**
   * Optional. Total number of worker nodes in the cluster. 1 master + N workers.
   *
   * @param nodeCount nodeCount or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpecClusterComputeResources setNodeCount(
      java.lang.Integer nodeCount) {
    this.nodeCount = nodeCount;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1TaskInfrastructureSpecClusterComputeResources set(
      String fieldName, Object value) {
    return (GoogleCloudDataplexV1TaskInfrastructureSpecClusterComputeResources)
        super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1TaskInfrastructureSpecClusterComputeResources clone() {
    return (GoogleCloudDataplexV1TaskInfrastructureSpecClusterComputeResources) super.clone();
  }
}
