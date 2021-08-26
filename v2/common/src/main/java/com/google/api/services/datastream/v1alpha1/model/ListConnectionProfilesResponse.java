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
package com.google.api.services.datastream.v1alpha1.model;

/**
 * Model definition for ListConnectionProfilesResponse.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class ListConnectionProfilesResponse extends com.google.api.client.json.GenericJson {

  /** List of Connection Profiles. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.List<ConnectionProfile> connectionProfiles;

  /**
   * A token, which can be sent as `page_token` to retrieve the next page. If this field is omitted,
   * there are no subsequent pages. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String nextPageToken;

  /** Locations that could not be reached. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> unreachable;

  /**
   * List of Connection Profiles.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<ConnectionProfile> getConnectionProfiles() {
    return connectionProfiles;
  }

  /**
   * List of Connection Profiles.
   *
   * @param connectionProfiles connectionProfiles or {@code null} for none
   */
  public ListConnectionProfilesResponse setConnectionProfiles(
      java.util.List<ConnectionProfile> connectionProfiles) {
    this.connectionProfiles = connectionProfiles;
    return this;
  }

  /**
   * A token, which can be sent as `page_token` to retrieve the next page. If this field is omitted,
   * there are no subsequent pages.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getNextPageToken() {
    return nextPageToken;
  }

  /**
   * A token, which can be sent as `page_token` to retrieve the next page. If this field is omitted,
   * there are no subsequent pages.
   *
   * @param nextPageToken nextPageToken or {@code null} for none
   */
  public ListConnectionProfilesResponse setNextPageToken(java.lang.String nextPageToken) {
    this.nextPageToken = nextPageToken;
    return this;
  }

  /**
   * Locations that could not be reached.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getUnreachable() {
    return unreachable;
  }

  /**
   * Locations that could not be reached.
   *
   * @param unreachable unreachable or {@code null} for none
   */
  public ListConnectionProfilesResponse setUnreachable(
      java.util.List<java.lang.String> unreachable) {
    this.unreachable = unreachable;
    return this;
  }

  @Override
  public ListConnectionProfilesResponse set(String fieldName, Object value) {
    return (ListConnectionProfilesResponse) super.set(fieldName, value);
  }

  @Override
  public ListConnectionProfilesResponse clone() {
    return (ListConnectionProfilesResponse) super.clone();
  }
}
