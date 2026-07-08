/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.spanner.spannerio;

/**
 * Exception to signal that Spanner schema retrieval failed.
 *
 * <p>WARNING: This file is forked from Apache Beam. Ensure corresponding changes are made in Apache
 * Beam to prevent code divergence. TODO: (b/402322178) Remove this local copy.
 */
public class SpannerSchemaRetrievalException extends RuntimeException {
  SpannerSchemaRetrievalException(String message, Throwable cause) {
    super(message, cause);
  }

  SpannerSchemaRetrievalException(String message) {
    super(message);
  }
}
