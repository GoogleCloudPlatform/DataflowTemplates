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
package com.google.cloud.teleport.v2.source.cassandra;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.ISrcToSpSourceConnector;
import com.google.cloud.teleport.v2.source.cassandra.reader.io.cassandra.iowrapper.CassandraIOWrapperFactory;
import com.google.cloud.teleport.v2.templates.DbConfigContainerDefaultImpl;
import com.google.cloud.teleport.v2.templates.PipelineController;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;

/** Cassandra implementation of {@link ISrcToSpSourceConnector}. */
public class CassandraSrcToSpSourceConnector implements ISrcToSpSourceConnector {

  @Override
  public PipelineResult executeMigration(
      SourceDbToSpannerOptions options, Pipeline pipeline, SpannerConfig spannerConfig) {
    return PipelineController.executeMigrationForDbConfigContainer(
        options,
        pipeline,
        spannerConfig,
        new DbConfigContainerDefaultImpl(CassandraIOWrapperFactory.fromPipelineOptions(options)));
  }
}
