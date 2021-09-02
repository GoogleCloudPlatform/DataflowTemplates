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
 * List lakes response.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1ListLakesResponse
    extends com.google.api.client.json.GenericJson {

  /** Lakes under the given parent location. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.List<GoogleCloudDataplexV1Lake> lakes;

  static {
    // hack to force ProGuard to consider GoogleCloudDataplexV1Lake used, since otherwise it would
    // be stripped out
    // see https://github.com/google/google-api-java-client/issues/543
    com.google.api.client.util.Data.nullOf(GoogleCloudDataplexV1Lake.class);
  }

  /**
   * Token to retrieve the next page of results, or empty if there are no more results in the list.
   * The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String nextPageToken;

  /** Locations that could not be reached. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> unreachableLocations;

  /**
   * Lakes under the given parent location.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<GoogleCloudDataplexV1Lake> getLakes() {
    return lakes;
  }

  /**
   * Lakes under the given parent location.
   *
   * @param lakes lakes or {@code null} for none
   */
  public GoogleCloudDataplexV1ListLakesResponse setLakes(
      java.util.List<GoogleCloudDataplexV1Lake> lakes) {
    this.lakes = lakes;
    return this;
  }

  /**
   * Token to retrieve the next page of results, or empty if there are no more results in the list.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getNextPageToken() {
    return nextPageToken;
  }

  /**
   * Token to retrieve the next page of results, or empty if there are no more results in the list.
   *
   * @param nextPageToken nextPageToken or {@code null} for none
   */
  public GoogleCloudDataplexV1ListLakesResponse setNextPageToken(java.lang.String nextPageToken) {
    this.nextPageToken = nextPageToken;
    return this;
  }

  /**
   * Locations that could not be reached.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getUnreachableLocations() {
    return unreachableLocations;
  }

  /**
   * Locations that could not be reached.
   *
   * @param unreachableLocations unreachableLocations or {@code null} for none
   */
  public GoogleCloudDataplexV1ListLakesResponse setUnreachableLocations(
      java.util.List<java.lang.String> unreachableLocations) {
    this.unreachableLocations = unreachableLocations;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1ListLakesResponse set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1ListLakesResponse) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1ListLakesResponse clone() {
    return (GoogleCloudDataplexV1ListLakesResponse) super.clone();
  }
}
