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
 * Response containing the objects for a stream.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Datastream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class ListStreamObjectsResponse extends com.google.api.client.json.GenericJson {

  /**
   * A token, which can be sent as `page_token` to retrieve the next page. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.lang.String nextPageToken;

  /** List of stream objects. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.util.List<StreamObject> streamObjects;

  static {
    // hack to force ProGuard to consider StreamObject used, since otherwise it would be stripped
    // out
    // see https://github.com/google/google-api-java-client/issues/543
    com.google.api.client.util.Data.nullOf(StreamObject.class);
  }

  /**
   * A token, which can be sent as `page_token` to retrieve the next page.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getNextPageToken() {
    return nextPageToken;
  }

  /**
   * A token, which can be sent as `page_token` to retrieve the next page.
   *
   * @param nextPageToken nextPageToken or {@code null} for none
   */
  public ListStreamObjectsResponse setNextPageToken(java.lang.String nextPageToken) {
    this.nextPageToken = nextPageToken;
    return this;
  }

  /**
   * List of stream objects.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<StreamObject> getStreamObjects() {
    return streamObjects;
  }

  /**
   * List of stream objects.
   *
   * @param streamObjects streamObjects or {@code null} for none
   */
  public ListStreamObjectsResponse setStreamObjects(java.util.List<StreamObject> streamObjects) {
    this.streamObjects = streamObjects;
    return this;
  }

  @Override
  public ListStreamObjectsResponse set(String fieldName, Object value) {
    return (ListStreamObjectsResponse) super.set(fieldName, value);
  }

  @Override
  public ListStreamObjectsResponse clone() {
    return (ListStreamObjectsResponse) super.clone();
  }
}
