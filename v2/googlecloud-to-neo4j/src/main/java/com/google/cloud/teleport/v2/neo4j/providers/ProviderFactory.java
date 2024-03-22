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

import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetSequence;
import com.google.cloud.teleport.v2.neo4j.providers.bigquery.BigQueryImpl;
import com.google.cloud.teleport.v2.neo4j.providers.text.TextImpl;
import org.neo4j.importer.v1.sources.BigQuerySource;
import org.neo4j.importer.v1.sources.Source;
import org.neo4j.importer.v1.sources.TextSource;

/**
 * Factory for binding implementation adapters into framework. Currently, supports two providers:
 * bigquery and text
 */
public class ProviderFactory {

  public static Provider of(Source source, TargetSequence targetSequence) {
    switch (source.getType()) {
      case BIGQUERY:
        return new BigQueryImpl((BigQuerySource) source, targetSequence);
      case TEXT:
        return new TextImpl((TextSource) source, targetSequence);
      case JDBC:
      default:
        throw new RuntimeException("Unsupported source type: " + source);
    }
  }
}
