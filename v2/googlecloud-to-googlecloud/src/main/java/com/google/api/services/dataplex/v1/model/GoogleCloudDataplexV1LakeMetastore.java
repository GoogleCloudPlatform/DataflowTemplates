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
 * Settings to manage association of Dataproc Metastore with a lake.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1LakeMetastore
    extends com.google.api.client.json.GenericJson {

  /**
   * Optional. A relative reference to the Dataproc Metastore (https://cloud.google.com/dataproc-
   * metastore/docs) service associated with the lake:
   * projects/{project_id}/locations/{location_id}/services/{service_id} The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.lang.String service;

  /**
   * Optional. A relative reference to the Dataproc Metastore (https://cloud.google.com/dataproc-
   * metastore/docs) service associated with the lake:
   * projects/{project_id}/locations/{location_id}/services/{service_id}
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getService() {
    return service;
  }

  /**
   * Optional. A relative reference to the Dataproc Metastore (https://cloud.google.com/dataproc-
   * metastore/docs) service associated with the lake:
   * projects/{project_id}/locations/{location_id}/services/{service_id}
   *
   * @param service service or {@code null} for none
   */
  public GoogleCloudDataplexV1LakeMetastore setService(java.lang.String service) {
    this.service = service;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1LakeMetastore set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1LakeMetastore) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1LakeMetastore clone() {
    return (GoogleCloudDataplexV1LakeMetastore) super.clone();
  }
}
