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
 * Settings to manage the security policy applied to data referenced by an asset.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1AssetSecuritySpec
    extends com.google.api.client.json.GenericJson {

  /**
   * Optional. Indicates whether the policy overrides the list of groups specified in the policy
   * associated with the parent lake and zone. By default, the policy is additive. If this flag is
   * true, the list of groups must each include at least one entry. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.Boolean overrideParentSpec;

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataOwner IAM role, that
   * allows group members to modify the resources (including their IAM policies and child resources)
   * referenced by an asset. For example, yourgroup@yourdomain.com. It cannot be a service account,
   * user or another type of identity. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> ownerGroups;

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataReader IAM role, that
   * allows group members to read data referenced by an asset. For example,
   * yourgroup@yourdomain.com. It cannot be a service account, user or another type of identity. The
   * value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> readerGroups;

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataWriter IAM role, that
   * allows group members to update data referenced by an asset. For example,
   * yourgroup@yourdomain.com. It cannot be a service account, user or another type of identity. The
   * value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> writerGroups;

  /**
   * Optional. Indicates whether the policy overrides the list of groups specified in the policy
   * associated with the parent lake and zone. By default, the policy is additive. If this flag is
   * true, the list of groups must each include at least one entry.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getOverrideParentSpec() {
    return overrideParentSpec;
  }

  /**
   * Optional. Indicates whether the policy overrides the list of groups specified in the policy
   * associated with the parent lake and zone. By default, the policy is additive. If this flag is
   * true, the list of groups must each include at least one entry.
   *
   * @param overrideParentSpec overrideParentSpec or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetSecuritySpec setOverrideParentSpec(
      java.lang.Boolean overrideParentSpec) {
    this.overrideParentSpec = overrideParentSpec;
    return this;
  }

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataOwner IAM role, that
   * allows group members to modify the resources (including their IAM policies and child resources)
   * referenced by an asset. For example, yourgroup@yourdomain.com. It cannot be a service account,
   * user or another type of identity.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getOwnerGroups() {
    return ownerGroups;
  }

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataOwner IAM role, that
   * allows group members to modify the resources (including their IAM policies and child resources)
   * referenced by an asset. For example, yourgroup@yourdomain.com. It cannot be a service account,
   * user or another type of identity.
   *
   * @param ownerGroups ownerGroups or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetSecuritySpec setOwnerGroups(
      java.util.List<java.lang.String> ownerGroups) {
    this.ownerGroups = ownerGroups;
    return this;
  }

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataReader IAM role, that
   * allows group members to read data referenced by an asset. For example,
   * yourgroup@yourdomain.com. It cannot be a service account, user or another type of identity.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getReaderGroups() {
    return readerGroups;
  }

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataReader IAM role, that
   * allows group members to read data referenced by an asset. For example,
   * yourgroup@yourdomain.com. It cannot be a service account, user or another type of identity.
   *
   * @param readerGroups readerGroups or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetSecuritySpec setReaderGroups(
      java.util.List<java.lang.String> readerGroups) {
    this.readerGroups = readerGroups;
    return this;
  }

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataWriter IAM role, that
   * allows group members to update data referenced by an asset. For example,
   * yourgroup@yourdomain.com. It cannot be a service account, user or another type of identity.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getWriterGroups() {
    return writerGroups;
  }

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataWriter IAM role, that
   * allows group members to update data referenced by an asset. For example,
   * yourgroup@yourdomain.com. It cannot be a service account, user or another type of identity.
   *
   * @param writerGroups writerGroups or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetSecuritySpec setWriterGroups(
      java.util.List<java.lang.String> writerGroups) {
    this.writerGroups = writerGroups;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1AssetSecuritySpec set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1AssetSecuritySpec) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1AssetSecuritySpec clone() {
    return (GoogleCloudDataplexV1AssetSecuritySpec) super.clone();
  }
}
