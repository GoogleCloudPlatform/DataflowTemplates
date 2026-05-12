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
package com.google.cloud.teleport.v2.neo4j.providers;

import com.google.cloud.teleport.v2.neo4j.model.helpers.StepSequence;
import com.google.cloud.teleport.v2.neo4j.model.sources.BigQuerySource;
import com.google.cloud.teleport.v2.neo4j.model.sources.TextSource;
import com.google.cloud.teleport.v2.neo4j.providers.bigquery.BigQuerySourceProvider;
import com.google.cloud.teleport.v2.neo4j.providers.text.TextSourceProvider;
import org.neo4j.importer.v1.sources.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for binding implementation adapters into framework. Currently, supports two providers:
 * bigquery and text
 */
public class SourceProviderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SourceProviderFactory.class);

  public static SourceProvider of(Source source, StepSequence targetSequence) {
    var sourceType = source.getType();
    return switch (sourceType) {
      case "bigquery" -> new BigQuerySourceProvider((BigQuerySource) source, targetSequence);
      case "text" -> new TextSourceProvider((TextSource) source, targetSequence);
      default -> {
        LOG.error("Unsupported source type: {}", sourceType);
        throw new RuntimeException("Unsupported source type: " + sourceType);
      }
    };
  }
}
