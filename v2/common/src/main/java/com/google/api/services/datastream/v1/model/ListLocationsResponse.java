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
 * The response message for Locations.ListLocations.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Datastream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class ListLocationsResponse extends com.google.api.client.json.GenericJson {

  /**
   * A list of locations that matches the specified filter in the request. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.util.List<Location> locations;

  /** The standard List next-page token. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String nextPageToken;

  /**
   * A list of locations that matches the specified filter in the request.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<Location> getLocations() {
    return locations;
  }

  /**
   * A list of locations that matches the specified filter in the request.
   *
   * @param locations locations or {@code null} for none
   */
  public ListLocationsResponse setLocations(java.util.List<Location> locations) {
    this.locations = locations;
    return this;
  }

  /**
   * The standard List next-page token.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getNextPageToken() {
    return nextPageToken;
  }

  /**
   * The standard List next-page token.
   *
   * @param nextPageToken nextPageToken or {@code null} for none
   */
  public ListLocationsResponse setNextPageToken(java.lang.String nextPageToken) {
    this.nextPageToken = nextPageToken;
    return this;
  }

  @Override
  public ListLocationsResponse set(String fieldName, Object value) {
    return (ListLocationsResponse) super.set(fieldName, value);
  }

  @Override
  public ListLocationsResponse clone() {
    return (ListLocationsResponse) super.clone();
  }
}
