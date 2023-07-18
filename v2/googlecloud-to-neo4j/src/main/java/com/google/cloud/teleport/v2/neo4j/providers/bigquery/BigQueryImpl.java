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

import com.google.cloud.teleport.v2.neo4j.model.helpers.SourceQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.helpers.SqlQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.helpers.SqlQuerySpec.SqlQuerySpecBuilder;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.providers.Provider;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provider implementation for reading and writing BigQuery. */
public class BigQueryImpl implements Provider {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryImpl.class);

  private OptionsParams optionsParams;

  public BigQueryImpl() {}

  @Override
  public void configure(OptionsParams optionsParams, JobSpec jobSpecRequest) {
    this.optionsParams = optionsParams;
  }

  @Override
  public boolean supportsSqlPushDown() {
    return true;
  }

  @Override
  public List<String> validateJobSpec() {
    // no specific validations currently

    return new ArrayList<>();
  }

  @Override
  public PTransform<PBegin, PCollection<Row>> querySourceBeamRows(SourceQuerySpec sourceQuerySpec) {
    return new BqQueryToRow(getSourceQueryBeamSpec(sourceQuerySpec));
  }

  @Override
  public PTransform<PBegin, PCollection<Row>> queryTargetBeamRows(TargetQuerySpec targetQuerySpec) {
    return new BqQueryToRow(getTargetQueryBeamSpec(targetQuerySpec));
  }

  @Override
  public PTransform<PBegin, PCollection<Row>> queryMetadata(Source source) {
    return new BqQueryToRow(getMetadataQueryBeamSpec(source));
  }

  /**
   * Returns zero rows metadata query based on original query.
   *
   * @return helper object includes metadata and SQL
   */
  public SqlQuerySpec getMetadataQueryBeamSpec(Source source) {

    String baseQuery = getBaseQuery(source);

    ////////////////////////////
    // Dry run won't return schema so use regular query
    // We need fieldSet for SQL generation later
    String zeroRowSql = "SELECT * FROM (" + baseQuery + ") LIMIT 0";
    LOG.info("Reading BQ metadata with query: {}", zeroRowSql);

    return new SqlQuerySpecBuilder()
        .readDescription("Read from BQ " + source.getName())
        .castDescription("Cast to BeamRow " + source.getName())
        .sql(zeroRowSql)
        .build();
  }

  /**
   * Returns base source query from source helper object.
   *
   * @return helper object includes metadata and SQL
   */
  public SqlQuerySpec getSourceQueryBeamSpec(SourceQuerySpec sourceQuerySpec) {
    return new SqlQuerySpecBuilder()
        .castDescription("Cast to BeamRow " + sourceQuerySpec.getSource().getName())
        .readDescription("Read from BQ " + sourceQuerySpec.getSource().getName())
        .sql(getBaseQuery(sourceQuerySpec.getSource()))
        .build();
  }

  /**
   * Returns target query from helper object which includes source and target.
   *
   * @return helper object includes metadata and SQL
   */
  public SqlQuerySpec getTargetQueryBeamSpec(TargetQuerySpec targetQuerySpec) {
    Set<String> sourceFieldSet = ModelUtils.getBeamFieldSet(targetQuerySpec.getSourceBeamSchema());
    String baseSql = getBaseQuery(targetQuerySpec.getSource());
    String targetSpecificSql =
        ModelUtils.getTargetSql(sourceFieldSet, targetQuerySpec, true, baseSql);
    return new SqlQuerySpecBuilder()
        .readDescription(
            targetQuerySpec.getTarget().getSequence()
                + ": Read from BQ "
                + targetQuerySpec.getTarget().getName())
        .castDescription(
            targetQuerySpec.getTarget().getSequence()
                + ": Cast to BeamRow "
                + targetQuerySpec.getTarget().getName())
        .sql(targetSpecificSql)
        .build();
  }

  private String getBaseQuery(Source source) {
    String baseSql = source.getQuery();
    if (StringUtils.isNotEmpty(optionsParams.getReadQuery())) {
      LOG.info("Overriding source query with run-time option");
      baseSql = optionsParams.getReadQuery();
    }
    return baseSql;
  }
}
