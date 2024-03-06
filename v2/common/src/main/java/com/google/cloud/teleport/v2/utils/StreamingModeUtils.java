/*
 * Copyright (C) 2024 Google LLC
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

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.ExperimentalOptions;

public class StreamingModeUtils {
  private StreamingModeUtils() {}

  public static void validate(DataflowPipelineOptions options) {
    if (isAtLeastOnceEnabled(options)) {
      options.setEnableStreamingEngine(true);
    }
  }

  public static void validateBQOptions(DataflowPipelineOptions options) {
    validate(options);
    if (isAtLeastOnceEnabled(options) && options.getUseStorageWriteApi()) {
      options.setUseStorageWriteApiAtLeastOnce(true);
    }
  }

  public static void enableAtLeastOnce(DataflowPipelineOptions options) {
    // Enable "At-Least Once" mode if "Exactly Once" mode is not specified.
    if (!isExactlyOnceEnabled(options)) {
      ExperimentalOptions.addExperiment(options, "streaming_mode_at_least_once");
    }
  }

  private static boolean isAtLeastOnceEnabled(DataflowPipelineOptions options) {
    return (ExperimentalOptions.hasExperiment(options, "streaming_mode_at_least_once")
        || ((options.getDataflowServiceOptions() != null)
            && options.getDataflowServiceOptions().contains("streaming_mode_at_least_once")));
  }

  private static boolean isExactlyOnceEnabled(DataflowPipelineOptions options) {
    return (ExperimentalOptions.hasExperiment(options, "streaming_mode_exactly_once")
        || ((options.getDataflowServiceOptions() != null)
            && options.getDataflowServiceOptions().contains("streaming_mode_exactly_once")));
  }
}
