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
 * route list response
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class ListRoutesResponse extends com.google.api.client.json.GenericJson {

  /**
   * A token, which can be sent as `page_token` to retrieve the next page. If this field is omitted,
   * there are no subsequent pages. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String nextPageToken;

  /** List of Routes. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.List<Route> routes;

  static {
    // hack to force ProGuard to consider Route used, since otherwise it would be stripped out
    // see https://github.com/google/google-api-java-client/issues/543
    com.google.api.client.util.Data.nullOf(Route.class);
  }

  /** Locations that could not be reached. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> unreachable;

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
  public ListRoutesResponse setNextPageToken(java.lang.String nextPageToken) {
    this.nextPageToken = nextPageToken;
    return this;
  }

  /**
   * List of Routes.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<Route> getRoutes() {
    return routes;
  }

  /**
   * List of Routes.
   *
   * @param routes routes or {@code null} for none
   */
  public ListRoutesResponse setRoutes(java.util.List<Route> routes) {
    this.routes = routes;
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
  public ListRoutesResponse setUnreachable(java.util.List<java.lang.String> unreachable) {
    this.unreachable = unreachable;
    return this;
  }

  @Override
  public ListRoutesResponse set(String fieldName, Object value) {
    return (ListRoutesResponse) super.set(fieldName, value);
  }

  @Override
  public ListRoutesResponse clone() {
    return (ListRoutesResponse) super.clone();
  }
}
