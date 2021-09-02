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
 * Cloud VPC Network used to run the infrastructure.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1TaskInfrastructureSpecVpcNetwork
    extends com.google.api.client.json.GenericJson {

  /**
   * Optional. The Cloud VPC network in which the job is run. By default, the Cloud VPC network
   * named Default within the project is used. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String network;

  /** Optional. List of network tags to apply to the job. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> networkTags;

  /**
   * Optional. Whether the compute resources allocated to run the job have internal/ private IPs. By
   * default, the VMs have a public IP. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.Boolean privateIpOnly;

  /** Optional. The Cloud VPC sub-network in which the job is run. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String subNetwork;

  /**
   * Optional. The Cloud VPC network in which the job is run. By default, the Cloud VPC network
   * named Default within the project is used.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getNetwork() {
    return network;
  }

  /**
   * Optional. The Cloud VPC network in which the job is run. By default, the Cloud VPC network
   * named Default within the project is used.
   *
   * @param network network or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpecVpcNetwork setNetwork(
      java.lang.String network) {
    this.network = network;
    return this;
  }

  /**
   * Optional. List of network tags to apply to the job.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getNetworkTags() {
    return networkTags;
  }

  /**
   * Optional. List of network tags to apply to the job.
   *
   * @param networkTags networkTags or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpecVpcNetwork setNetworkTags(
      java.util.List<java.lang.String> networkTags) {
    this.networkTags = networkTags;
    return this;
  }

  /**
   * Optional. Whether the compute resources allocated to run the job have internal/ private IPs. By
   * default, the VMs have a public IP.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getPrivateIpOnly() {
    return privateIpOnly;
  }

  /**
   * Optional. Whether the compute resources allocated to run the job have internal/ private IPs. By
   * default, the VMs have a public IP.
   *
   * @param privateIpOnly privateIpOnly or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpecVpcNetwork setPrivateIpOnly(
      java.lang.Boolean privateIpOnly) {
    this.privateIpOnly = privateIpOnly;
    return this;
  }

  /**
   * Optional. The Cloud VPC sub-network in which the job is run.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getSubNetwork() {
    return subNetwork;
  }

  /**
   * Optional. The Cloud VPC sub-network in which the job is run.
   *
   * @param subNetwork subNetwork or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpecVpcNetwork setSubNetwork(
      java.lang.String subNetwork) {
    this.subNetwork = subNetwork;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1TaskInfrastructureSpecVpcNetwork set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1TaskInfrastructureSpecVpcNetwork) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1TaskInfrastructureSpecVpcNetwork clone() {
    return (GoogleCloudDataplexV1TaskInfrastructureSpecVpcNetwork) super.clone();
  }
}
