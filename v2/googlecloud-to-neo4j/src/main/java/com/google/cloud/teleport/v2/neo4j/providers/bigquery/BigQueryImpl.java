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
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetSequence;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.model.sources.BigQuerySource;
import com.google.cloud.teleport.v2.neo4j.providers.Provider;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provider implementation for reading and writing BigQuery. */
public class BigQueryImpl implements Provider {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryImpl.class);
  private final BigQuerySource source;
  private final TargetSequence targetSequence;
  private OptionsParams optionsParams;

  public BigQueryImpl(BigQuerySource source, TargetSequence targetSequence) {
    this.source = source;
    this.targetSequence = targetSequence;
  }

  @Override
  public void configure(OptionsParams optionsParams) {
    this.optionsParams = optionsParams;
  }

  @Override
  public boolean supportsSqlPushDown() {
    return true;
  }

  @Override
  public PTransform<PBegin, PCollection<Row>> querySourceBeamRows(Schema schema) {
    return new BqQueryToRow(getSourceQueryBeamSpec());
  }

  @Override
  public PTransform<PBegin, PCollection<Row>> queryTargetBeamRows(TargetQuerySpec targetQuerySpec) {
    return new BqQueryToRow(getTargetQueryBeamSpec(targetQuerySpec));
  }

  @Override
  public PTransform<PBegin, PCollection<Row>> queryMetadata() {
    return new BqQueryToRow(getMetadataQueryBeamSpec());
  }

  /**
   * Returns zero rows metadata query based on original query.
   *
   * @return helper object includes metadata and SQL
   */
  public BigQuerySpec getMetadataQueryBeamSpec() {

    String baseQuery = source.getQuery();

    ////////////////////////////
    // Dry run won't return schema so use regular query
    // We need fieldSet for SQL generation later
    String zeroRowSql = "SELECT * FROM (" + baseQuery + ") LIMIT 0";
    LOG.info("Reading BQ metadata with query: {}", zeroRowSql);

    return new BigQuerySpecBuilder()
        .readDescription("Read from BQ " + source.getName())
        .castDescription("Cast to BeamRow " + source.getName())
        .sql(zeroRowSql)
        .queryTempProject(source.getQueryTempProject())
        .queryTempDataset(source.getQueryTempDataset())
        .build();
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
    var sourceFields = ModelUtils.getBeamFieldSet(spec.getSourceBeamSchema());
    var target = spec.getTarget();
    var startNodeTarget = spec.getStartNodeTarget();
    var endNodeTarget = spec.getEndNodeTarget();
    String sql =
        ModelUtils.getTargetSql(
            target, startNodeTarget, endNodeTarget, sourceFields, true, source.getQuery());
    return new BigQuerySpecBuilder()
        .readDescription(
            targetSequence.getSequenceNumber(target) + ": Read from BQ " + target.getName())
        .castDescription(
            targetSequence.getSequenceNumber(target) + ": Cast to BeamRow " + target.getName())
        .sql(sql)
        .queryTempProject(source.getQueryTempProject())
        .queryTempDataset(source.getQueryTempDataset())
        .build();
  }
}
