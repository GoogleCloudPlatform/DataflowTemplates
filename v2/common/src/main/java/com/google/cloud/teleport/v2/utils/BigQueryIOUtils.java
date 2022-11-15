/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;

/** Various utility methods for working with BigQuery. */
public final class BigQueryIOUtils {

  private BigQueryIOUtils() {}

  public static void validateBQStorageApiOptionsBatch(BigQueryOptions options) {
    if (options.getUseStorageWriteApiAtLeastOnce() && !options.getUseStorageWriteApi()) {
      // Technically this is a no-op, since useStorageWriteApiAtLeastOnce is only checked by
      // BigQueryIO when useStorageWriteApi is true, but it might be confusing to a user why
      // useStorageWriteApiAtLeastOnce doesn't take effect.
      throw new IllegalArgumentException(
          "When at-least-once semantics (useStorageWriteApiAtLeastOnce) are enabled Storage Write"
              + " API (useStorageWriteApi) must also be enabled.");
    }
  }
}
