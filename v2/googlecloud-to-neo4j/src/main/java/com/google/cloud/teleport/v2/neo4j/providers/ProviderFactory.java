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
import com.google.cloud.teleport.v2.neo4j.model.sources.BigQuerySource;
import com.google.cloud.teleport.v2.neo4j.model.sources.TextSource;
import com.google.cloud.teleport.v2.neo4j.providers.bigquery.BigQueryImpl;
import com.google.cloud.teleport.v2.neo4j.providers.text.TextImpl;
import org.neo4j.importer.v1.sources.Source;

/**
 * Factory for binding implementation adapters into framework. Currently, supports two providers:
 * bigquery and text
 */
public class ProviderFactory {

  public static Provider of(Source source, TargetSequence targetSequence) {
    switch (source.getType()) {
      case "bigquery":
        return new BigQueryImpl((BigQuerySource) source, targetSequence);
      case "text":
        return new TextImpl((TextSource) source, targetSequence);
      default:
        throw new RuntimeException("Unsupported source type: " + source);
    }
  }
}
