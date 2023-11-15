/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.sqllauncher;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogTableProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

/**
 * Abstract base class for defining SQL pipeline sinks.
 *
 * <p>To deserialize a JSON sink definition use {@code new ObjectMapper().readValue(jsonString,
 * SinkDefinition.class)}
 *
 * <p>The resulting sink can then be applied to a PCollection with {@link
 * #applyTransform(PCollection, DataflowSqlLauncherOptions)}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = BigQuerySinkDefinition.class, name = "bigquery"),
  @JsonSubTypes.Type(value = PubSubSinkDefinition.class, name = "pubsub")
})
abstract class SinkDefinition {

  /**
   * Apply the configured sink to {@code queryResult}, possibly changing behavior based on {@code
   * options}.
   */
  final void applyTransform(
      PCollection<Row> queryResult,
      DataflowSqlLauncherOptions options,
      DataCatalogTableProvider tableProvider) {
    queryResult.apply(getTransformName(), createTransform(queryResult, options, tableProvider));
  }

  /** A table name that can be used to reference this resource. */
  abstract String getTableName();

  /** The name to be used when applying the created transform. */
  final String getTransformName() {
    return String.format("Write to %s", getTableName());
  }

  /** Create the appropriate transform. */
  protected abstract PTransform<PCollection<Row>, ? extends POutput> createTransform(
      PCollection<Row> queryResult,
      DataflowSqlLauncherOptions options,
      DataCatalogTableProvider tableProvider)
      throws UnsupportedOperationException;
}
