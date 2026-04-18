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

/**
 * Thrown by {@link Utf8BoundedAppendable} when the running UTF-8 byte count crosses the configured
 * upper bound during {@code JsonFormat.Printer#appendTo}. Unchecked so it can unwind through Guava
 * / protobuf appendable callers cleanly.
 */
final class OversizedJsonException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final long bytesSoFar;

  OversizedJsonException(long bytesSoFar) {
    super(
        "JSON output exceeded the configured maximum byte size (" + bytesSoFar + " bytes so far)");
    this.bytesSoFar = bytesSoFar;
  }

  long bytesSoFar() {
    return bytesSoFar;
  }
}
