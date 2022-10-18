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
 * Request for looking up a specific stream object by its source object identifier.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Datastream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class LookupStreamObjectRequest extends com.google.api.client.json.GenericJson {

  /**
   * Required. The source object identifier which maps to the stream object. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private SourceObjectIdentifier sourceObjectIdentifier;

  /**
   * Required. The source object identifier which maps to the stream object.
   *
   * @return value or {@code null} for none
   */
  public SourceObjectIdentifier getSourceObjectIdentifier() {
    return sourceObjectIdentifier;
  }

  /**
   * Required. The source object identifier which maps to the stream object.
   *
   * @param sourceObjectIdentifier sourceObjectIdentifier or {@code null} for none
   */
  public LookupStreamObjectRequest setSourceObjectIdentifier(
      SourceObjectIdentifier sourceObjectIdentifier) {
    this.sourceObjectIdentifier = sourceObjectIdentifier;
    return this;
  }

  @Override
  public LookupStreamObjectRequest set(String fieldName, Object value) {
    return (LookupStreamObjectRequest) super.set(fieldName, value);
  }

  @Override
  public LookupStreamObjectRequest clone() {
    return (LookupStreamObjectRequest) super.clone();
  }
}
