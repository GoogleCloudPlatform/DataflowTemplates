/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.source;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;

/** Interface for source database connectors. Encapsulates source-specific migration execution. */
public interface ISrcToSpSourceConnector {

  /**
   * Return source type for the connector.
   *
   * @return
   */
  String getSourceType();

  /**
   * Return source type to be used in DLQ. //TODO check how this can be removed.
   *
   * @return
   */
  default String getDlqSourceType() {
    return getSourceType();
  }

  /**
   * Executes the migration pipeline for the source database.
   *
   * @param options Pipeline options.
   * @param pipeline The Beam pipeline.
   * @param spannerConfig Spanner configuration.
   * @return The pipeline result.
   */
  PipelineResult executeMigration(
      SourceDbToSpannerOptions options, Pipeline pipeline, SpannerConfig spannerConfig);
}
