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
 * Identifies the cloud resource that is referenced by this asset.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1AssetResourceSpec
    extends com.google.api.client.json.GenericJson {

  /** Immutable. Creation policy for the referenced resource. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String creationPolicy;

  /** Optional. Deletion policy for the referenced resource. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String deletionPolicy;

  /**
   * Immutable. Relative name of the cloud resource that contains the data that is being managed
   * within a lake. For example: projects/{project_number}/buckets/{bucket_id}
   * projects/{project_number}/datasets/{dataset_id} If the creation policy indicates ATTACH
   * behavior, then an existing resource must be provided. If the policy indicates CREATE behavior,
   * new resource will be created with the given name.However if it is empty, nthen the resource
   * will be created using {asset_id}-{UUID} template for name. The location of the referenced
   * resource must always match that of the asset. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String name;

  /** Required. Immutable. Type of resource. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String type;

  /**
   * Immutable. Creation policy for the referenced resource.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getCreationPolicy() {
    return creationPolicy;
  }

  /**
   * Immutable. Creation policy for the referenced resource.
   *
   * @param creationPolicy creationPolicy or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetResourceSpec setCreationPolicy(java.lang.String creationPolicy) {
    this.creationPolicy = creationPolicy;
    return this;
  }

  /**
   * Optional. Deletion policy for the referenced resource.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDeletionPolicy() {
    return deletionPolicy;
  }

  /**
   * Optional. Deletion policy for the referenced resource.
   *
   * @param deletionPolicy deletionPolicy or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetResourceSpec setDeletionPolicy(java.lang.String deletionPolicy) {
    this.deletionPolicy = deletionPolicy;
    return this;
  }

  /**
   * Immutable. Relative name of the cloud resource that contains the data that is being managed
   * within a lake. For example: projects/{project_number}/buckets/{bucket_id}
   * projects/{project_number}/datasets/{dataset_id} If the creation policy indicates ATTACH
   * behavior, then an existing resource must be provided. If the policy indicates CREATE behavior,
   * new resource will be created with the given name.However if it is empty, nthen the resource
   * will be created using {asset_id}-{UUID} template for name. The location of the referenced
   * resource must always match that of the asset.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getName() {
    return name;
  }

  /**
   * Immutable. Relative name of the cloud resource that contains the data that is being managed
   * within a lake. For example: projects/{project_number}/buckets/{bucket_id}
   * projects/{project_number}/datasets/{dataset_id} If the creation policy indicates ATTACH
   * behavior, then an existing resource must be provided. If the policy indicates CREATE behavior,
   * new resource will be created with the given name.However if it is empty, nthen the resource
   * will be created using {asset_id}-{UUID} template for name. The location of the referenced
   * resource must always match that of the asset.
   *
   * @param name name or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetResourceSpec setName(java.lang.String name) {
    this.name = name;
    return this;
  }

  /**
   * Required. Immutable. Type of resource.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getType() {
    return type;
  }

  /**
   * Required. Immutable. Type of resource.
   *
   * @param type type or {@code null} for none
   */
  public GoogleCloudDataplexV1AssetResourceSpec setType(java.lang.String type) {
    this.type = type;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1AssetResourceSpec set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1AssetResourceSpec) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1AssetResourceSpec clone() {
    return (GoogleCloudDataplexV1AssetResourceSpec) super.clone();
  }
}
