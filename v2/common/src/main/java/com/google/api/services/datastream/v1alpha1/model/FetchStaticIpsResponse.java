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
 * Response message for a 'FetchStaticIps' response.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class FetchStaticIpsResponse extends com.google.api.client.json.GenericJson {

  /**
   * A token that can be sent as `page_token` to retrieve the next page. If this field is omitted,
   * there are no subsequent pages. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String nextPageToken;

  /** list of static ips by account The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> staticIps;

  /**
   * A token that can be sent as `page_token` to retrieve the next page. If this field is omitted,
   * there are no subsequent pages.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getNextPageToken() {
    return nextPageToken;
  }

  /**
   * A token that can be sent as `page_token` to retrieve the next page. If this field is omitted,
   * there are no subsequent pages.
   *
   * @param nextPageToken nextPageToken or {@code null} for none
   */
  public FetchStaticIpsResponse setNextPageToken(java.lang.String nextPageToken) {
    this.nextPageToken = nextPageToken;
    return this;
  }

  /**
   * list of static ips by account
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getStaticIps() {
    return staticIps;
  }

  /**
   * list of static ips by account
   *
   * @param staticIps staticIps or {@code null} for none
   */
  public FetchStaticIpsResponse setStaticIps(java.util.List<java.lang.String> staticIps) {
    this.staticIps = staticIps;
    return this;
  }

  @Override
  public FetchStaticIpsResponse set(String fieldName, Object value) {
    return (FetchStaticIpsResponse) super.set(fieldName, value);
  }

  @Override
  public FetchStaticIpsResponse clone() {
    return (FetchStaticIpsResponse) super.clone();
  }
}
