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
 * Settings to manage the security policy applied to data in a lake.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1LakeSecuritySpec
    extends com.google.api.client.json.GenericJson {

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataOwner IAM role, that
   * allows group members to modify the resources (including their IAM policies and child resources)
   * managed within a lake. By default this is inherited by all assets within the lake. For example,
   * yourgroup@yourdomain.com. It cannot be a service account, user or another type of identity. The
   * value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> ownerGroups;

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataReader IAM role, that
   * allows group members to read data being managed within a lake. By default this is inherited by
   * all assets within a lake. For example, yourgroup@yourdomain.com. It cannot be a service
   * account, user or another type of identity. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> readerGroups;

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataWriter IAM role, that
   * allows group members to update data being managed within a lake. By default this is inherited
   * by all assets within a lake. For example, yourgroup@yourdomain.com. It cannot be a service
   * account, user or another type of identity. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> writerGroups;

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataOwner IAM role, that
   * allows group members to modify the resources (including their IAM policies and child resources)
   * managed within a lake. By default this is inherited by all assets within the lake. For example,
   * yourgroup@yourdomain.com. It cannot be a service account, user or another type of identity.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getOwnerGroups() {
    return ownerGroups;
  }

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataOwner IAM role, that
   * allows group members to modify the resources (including their IAM policies and child resources)
   * managed within a lake. By default this is inherited by all assets within the lake. For example,
   * yourgroup@yourdomain.com. It cannot be a service account, user or another type of identity.
   *
   * @param ownerGroups ownerGroups or {@code null} for none
   */
  public GoogleCloudDataplexV1LakeSecuritySpec setOwnerGroups(
      java.util.List<java.lang.String> ownerGroups) {
    this.ownerGroups = ownerGroups;
    return this;
  }

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataReader IAM role, that
   * allows group members to read data being managed within a lake. By default this is inherited by
   * all assets within a lake. For example, yourgroup@yourdomain.com. It cannot be a service
   * account, user or another type of identity.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getReaderGroups() {
    return readerGroups;
  }

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataReader IAM role, that
   * allows group members to read data being managed within a lake. By default this is inherited by
   * all assets within a lake. For example, yourgroup@yourdomain.com. It cannot be a service
   * account, user or another type of identity.
   *
   * @param readerGroups readerGroups or {@code null} for none
   */
  public GoogleCloudDataplexV1LakeSecuritySpec setReaderGroups(
      java.util.List<java.lang.String> readerGroups) {
    this.readerGroups = readerGroups;
    return this;
  }

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataWriter IAM role, that
   * allows group members to update data being managed within a lake. By default this is inherited
   * by all assets within a lake. For example, yourgroup@yourdomain.com. It cannot be a service
   * account, user or another type of identity.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getWriterGroups() {
    return writerGroups;
  }

  /**
   * Optional. A list of groups that should be granted the roles/dataplex.dataWriter IAM role, that
   * allows group members to update data being managed within a lake. By default this is inherited
   * by all assets within a lake. For example, yourgroup@yourdomain.com. It cannot be a service
   * account, user or another type of identity.
   *
   * @param writerGroups writerGroups or {@code null} for none
   */
  public GoogleCloudDataplexV1LakeSecuritySpec setWriterGroups(
      java.util.List<java.lang.String> writerGroups) {
    this.writerGroups = writerGroups;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1LakeSecuritySpec set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1LakeSecuritySpec) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1LakeSecuritySpec clone() {
    return (GoogleCloudDataplexV1LakeSecuritySpec) super.clone();
  }
}
