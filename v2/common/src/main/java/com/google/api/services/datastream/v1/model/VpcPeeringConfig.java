/*
 * Copyright (C) 2022 Google LLC
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
package com.google.api.services.datastream.v1.model;

/**
 * The VPC Peering configuration is used to create VPC peering between Datastream and the consumer's
 * VPC.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Datastream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class VpcPeeringConfig extends com.google.api.client.json.GenericJson {

  /**
   * Required. A free subnet for peering. (CIDR of /29) TODO(b/172995841) add validators. The value
   * may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String subnet;

  /**
   * Required. Fully qualified name of the VPC that Datastream will peer to. Format:
   * `projects/{project}/global/{networks}/{name}` The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String vpc;

  /**
   * Required. A free subnet for peering. (CIDR of /29) TODO(b/172995841) add validators.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getSubnet() {
    return subnet;
  }

  /**
   * Required. A free subnet for peering. (CIDR of /29) TODO(b/172995841) add validators.
   *
   * @param subnet subnet or {@code null} for none
   */
  public VpcPeeringConfig setSubnet(java.lang.String subnet) {
    this.subnet = subnet;
    return this;
  }

  /**
   * Required. Fully qualified name of the VPC that Datastream will peer to. Format:
   * `projects/{project}/global/{networks}/{name}`
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getVpc() {
    return vpc;
  }

  /**
   * Required. Fully qualified name of the VPC that Datastream will peer to. Format:
   * `projects/{project}/global/{networks}/{name}`
   *
   * @param vpc vpc or {@code null} for none
   */
  public VpcPeeringConfig setVpc(java.lang.String vpc) {
    this.vpc = vpc;
    return this;
  }

  @Override
  public VpcPeeringConfig set(String fieldName, Object value) {
    return (VpcPeeringConfig) super.set(fieldName, value);
  }

  @Override
  public VpcPeeringConfig clone() {
    return (VpcPeeringConfig) super.clone();
  }
}
