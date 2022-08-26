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
 * Response for manually initiating a backfill job for a specific stream object.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Datastream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class StartBackfillJobResponse extends com.google.api.client.json.GenericJson {

  /** The stream object resource a backfill job was started for. The value may be {@code null}. */
  @com.google.api.client.util.Key("object")
  private StreamObject object__;

  /**
   * The stream object resource a backfill job was started for.
   *
   * @return value or {@code null} for none
   */
  public StreamObject getObject() {
    return object__;
  }

  /**
   * The stream object resource a backfill job was started for.
   *
   * @param object__ object__ or {@code null} for none
   */
  public StartBackfillJobResponse setObject(StreamObject object__) {
    this.object__ = object__;
    return this;
  }

  @Override
  public StartBackfillJobResponse set(String fieldName, Object value) {
    return (StartBackfillJobResponse) super.set(fieldName, value);
  }

  @Override
  public StartBackfillJobResponse clone() {
    return (StartBackfillJobResponse) super.clone();
  }
}
