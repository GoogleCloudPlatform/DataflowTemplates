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

import com.google.cloud.teleport.v2.neo4j.model.enums.SourceType;
import com.google.cloud.teleport.v2.neo4j.providers.bigquery.BigQueryImpl;
import com.google.cloud.teleport.v2.neo4j.providers.text.TextImpl;

/**
 * Factory for binding implementation adapters into framework. Currently, supports two providers:
 * bigquery and text
 */
public class ProviderFactory {

  public static Provider of(SourceType sourceType) {
    if (sourceType == SourceType.bigquery) {
      return new BigQueryImpl();
    } else if (sourceType == SourceType.text) {
      return new TextImpl();
    } else {
      // TODO: support spanner sql, postgres, parquet, avro
      throw new RuntimeException("Unhandled source type: " + sourceType);
    }
  }
}
