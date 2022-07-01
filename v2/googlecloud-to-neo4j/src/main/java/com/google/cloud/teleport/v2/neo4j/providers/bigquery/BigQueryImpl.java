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
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.job.JobSpec;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import com.google.cloud.teleport.v2.neo4j.providers.Provider;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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

  private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

  private JobSpec jobSpec;
  private OptionsParams optionsParams;

  public BigQueryImpl() {}

  @Override
  public void configure(OptionsParams optionsParams, JobSpec jobSpecRequest) {
    this.jobSpec = jobSpecRequest;
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
    return new BqQueryToRow(optionsParams, getSourceQueryBeamSpec(sourceQuerySpec));
  }

  @Override
  public PTransform<PBegin, PCollection<Row>> queryTargetBeamRows(TargetQuerySpec targetQuerySpec) {
    return new BqQueryToRow(optionsParams, getTargetQueryBeamSpec(targetQuerySpec));
  }

  @Override
  public PTransform<PBegin, PCollection<Row>> queryMetadata(final Source source) {
    return new BqQueryToRow(optionsParams, getMetadataQueryBeamSpec(source));
  }

  /**
   * Returns zero rows metadata query based on original query.
   *
   * @param source
   * @return helper object includes metadata and SQL
   */
  public SqlQuerySpec getMetadataQueryBeamSpec(final Source source) {

    final String baseQuery = getBaseQuery(source);

    ////////////////////////////
    // Dry run won't return schema so use regular query
    // We need fieldSet for SQL generation later
    final String zeroRowSql = "SELECT * FROM (" + baseQuery + ") LIMIT 0";
    LOG.info("Reading BQ metadata with query: " + zeroRowSql);

    return SqlQuerySpec.builder()
        .readDescription("Read from BQ " + source.name)
        .castDescription("Cast to BeamRow " + source.name)
        .sql(zeroRowSql)
        .build();
  }

  /**
   * Returns base source query from source helper object.
   *
   * @param sourceQuerySpec
   * @return helper object includes metadata and SQL
   */
  public SqlQuerySpec getSourceQueryBeamSpec(SourceQuerySpec sourceQuerySpec) {
    return SqlQuerySpec.builder()
        .castDescription("Cast to BeamRow " + sourceQuerySpec.source.name)
        .readDescription("Read from BQ " + sourceQuerySpec.source.name)
        .sql(getBaseQuery(sourceQuerySpec.source))
        .build();
  }

  /**
   * Returns target query from helper object which includes source and target.
   *
   * @param targetQuerySpec
   * @return helper object includes metadata and SQL
   */
  public SqlQuerySpec getTargetQueryBeamSpec(TargetQuerySpec targetQuerySpec) {
    Set<String> sourceFieldSet = ModelUtils.getBeamFieldSet(targetQuerySpec.sourceBeamSchema);
    final String baseSql = getBaseQuery(targetQuerySpec.source);
    final String targetSpecificSql =
        ModelUtils.getTargetSql(sourceFieldSet, targetQuerySpec.target, true, baseSql);
    return SqlQuerySpec.builder()
        .readDescription(
            targetQuerySpec.target.sequence + ": Read from BQ " + targetQuerySpec.target.name)
        .castDescription(
            targetQuerySpec.target.sequence + ": Cast to BeamRow " + targetQuerySpec.target.name)
        .sql(targetSpecificSql)
        .build();
  }

  private String getBaseQuery(Source source) {
    String baseSql = source.query;
    if (StringUtils.isNotEmpty(optionsParams.readQuery)) {
      LOG.info("Overriding source query with run-time option");
      baseSql = optionsParams.readQuery;
    }
    return baseSql;
  }
}
