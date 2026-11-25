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
package com.google.cloud.teleport.v2.neo4j.providers.bigquery;

import com.google.cloud.teleport.v2.neo4j.model.helpers.BigQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.helpers.BigQuerySpec.BigQuerySpecBuilder;
import com.google.cloud.teleport.v2.neo4j.model.helpers.StepSequence;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.job.OverlayTokens;
import com.google.cloud.teleport.v2.neo4j.model.sources.BigQuerySource;
import com.google.cloud.teleport.v2.neo4j.providers.SourceProvider;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.importer.v1.pipeline.TargetStep;
import org.neo4j.importer.v1.targets.PropertyMapping;

/** Provider implementation for reading and writing BigQuery. */
public class BigQuerySourceProvider implements SourceProvider {
  private final BigQuerySource source;
  private final StepSequence targetSequence;

  public BigQuerySourceProvider(BigQuerySource source, StepSequence targetSequence) {
    this.source = source;
    this.targetSequence = targetSequence;
  }

  @Override
  public void configure(OverlayTokens overlayTokens) {}

  @Override
  public boolean supportsSqlPushDown() {
    return true;
  }

  @Override
  public PTransform<PBegin, PCollection<Row>> querySourceRows(Schema schema) {
    return new BqQueryToRow(getSourceQueryBeamSpec());
  }

  @Override
  public PTransform<PBegin, PCollection<Row>> querySourceRowsForTarget(
      TargetQuerySpec targetQuerySpec) {
    return new BqQueryToRow(getTargetQueryBeamSpec(targetQuerySpec));
  }

  @Override
  public PTransform<PBegin, PCollection<Row>> queryMetadata() {
    // The BigQuery schema is resolved by the read transform on a worker, so no metadata query is
    // needed. This zero-row collection only acts as the source step dependency.
    return Create.empty(Schema.of());
  }

  /**
   * Returns base source query from source helper object.
   *
   * @return helper object includes metadata and SQL
   */
  private BigQuerySpec getSourceQueryBeamSpec() {
    return new BigQuerySpecBuilder()
        .castDescription("Cast to BeamRow " + source.getName())
        .readDescription("Read from BQ " + source.getName())
        .sql(source.getQuery())
        .queryTempProject(source.getQueryTempProject())
        .queryTempDataset(source.getQueryTempDataset())
        .build();
  }

  /**
   * Returns target query from helper object which includes source and target.
   *
   * @return helper object includes metadata and SQL
   */
  private BigQuerySpec getTargetQueryBeamSpec(TargetQuerySpec spec) {
    var step = spec.getTargetStep();
    var sql = ModelUtils.getTargetSql(step, targetSourceFields(step), true, source.getQuery());
    return new BigQuerySpecBuilder()
        .readDescription(targetSequence.getSequenceNumber(step) + ": Read from BQ " + step.name())
        .castDescription(
            targetSequence.getSequenceNumber(step) + ": Cast to BeamRow " + step.name())
        .sql(sql)
        .queryTempProject(source.getQueryTempProject())
        .queryTempDataset(source.getQueryTempDataset())
        .build();
  }

  private static Set<String> targetSourceFields(TargetStep step) {
    return ModelUtils.getAllPropertyMappings(step).stream()
        .map(PropertyMapping::getSourceField)
        .filter(StringUtils::isNotEmpty)
        .collect(Collectors.toSet());
  }
}
