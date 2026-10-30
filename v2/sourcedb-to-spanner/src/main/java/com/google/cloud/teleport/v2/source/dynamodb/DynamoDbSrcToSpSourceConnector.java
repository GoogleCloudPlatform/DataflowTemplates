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
package com.google.cloud.teleport.v2.source.dynamodb;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.UnifiedTypeMapping;
import com.google.cloud.teleport.v2.source.ISrcToSpSourceConnector;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConnectionConfig;
import com.google.cloud.teleport.v2.templates.DbConfigContainerDefaultImpl;
import com.google.cloud.teleport.v2.templates.PipelineController;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;

/** Amazon DynamoDB implementation of {@link ISrcToSpSourceConnector}. */
public class DynamoDbSrcToSpSourceConnector implements ISrcToSpSourceConnector {

  @Override
  public String getSourceType() {
    return Constants.DYNAMODB_SOURCE_TYPE;
  }

  /**
   * Returns the type mapping from DynamoDB source data types to {@link UnifiedTypeMapping}.
   *
   * @return ImmutableMap of DynamoDB type names to UnifiedTypeMapping.
   */
  public ImmutableMap<String, UnifiedTypeMapping> getTypeMapping() {
    return DynamoDbMappingsProvider.getMapping();
  }

  @Override
  public PipelineResult executeMigration(
      SourceDbToSpannerOptions options,
      SourceConnectionConfig sourceConnectionConfig,
      Pipeline pipeline,
      SpannerConfig spannerConfig) {
    return PipelineController.executeMigrationForDbConfigContainer(
        options,
        pipeline,
        spannerConfig,
        new DbConfigContainerDefaultImpl(
            DynamoDbIoWrapperFactory.fromConfig(
                sourceConnectionConfig, options.getNumPartitions())));
  }
}
