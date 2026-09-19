/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils;

import javax.annotation.Nullable;

/** Outcome of applying a {@link ValueTransformer} with a decoded-size bound. */
public final class TransformResult {
  /** Status of a bounded transform attempt. */
  public enum Status {
    /** The transformer produced a value that fits within the decoded-size bound. */
    SUCCESS,
    /** The transformer failed to decode the raw bytes (e.g. malformed proto). */
    DECODE_ERROR,
    /** The decoded output exceeded the configured byte budget and was abandoned. */
    OVERSIZED,
    /** No transformer is registered for the column; raw bytes should be used as-is. */
    NO_TRANSFORMER
  }

  private final Status status;
  @Nullable private final String value;
  private final long rawBytes;
  // Exact for SUCCESS/OVERSIZED, 0 otherwise.
  private final long decodedBytes;

  private TransformResult(Status status, @Nullable String value, long rawBytes, long decodedBytes) {
    this.status = status;
    this.value = value;
    this.rawBytes = rawBytes;
    this.decodedBytes = decodedBytes;
  }

  public static TransformResult success(String value, long rawBytes, long decodedBytes) {
    return new TransformResult(Status.SUCCESS, value, rawBytes, decodedBytes);
  }

  public static TransformResult decodeError(long rawBytes) {
    return new TransformResult(Status.DECODE_ERROR, null, rawBytes, 0);
  }

  public static TransformResult oversized(long rawBytes, long estimatedDecodedBytes) {
    return new TransformResult(Status.OVERSIZED, null, rawBytes, estimatedDecodedBytes);
  }

  public static TransformResult noTransformer() {
    return new TransformResult(Status.NO_TRANSFORMER, null, 0, 0);
  }

  public Status status() {
    return status;
  }

  @Nullable
  public String value() {
    return value;
  }

  public long rawBytes() {
    return rawBytes;
  }

  public long decodedBytes() {
    return decodedBytes;
  }
}
