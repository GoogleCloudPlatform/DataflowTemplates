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
import com.google.cloud.teleport.v2.neo4j.utils.RuntimeSchemaRowCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Transform to query BigQuery and output PCollection of Row. */
public class BqQueryToRow extends PTransform<PBegin, PCollection<Row>> {

  private static final Logger LOG = LoggerFactory.getLogger(BqQueryToRow.class);
  private final BigQuerySpec bqQuerySpec;

  public BqQueryToRow(BigQuerySpec bqQuerySpec) {
    this.bqQuerySpec = bqQuerySpec;
  }

  @Override
  public PCollection<Row> expand(PBegin input) {
    String rewrittenSql = this.bqQuerySpec.getSql();
    LOG.info("Reading BQ with query: {}", rewrittenSql);

    // The schema is resolved by the reader on a worker and carried by each Row (see
    // BigQueryRowParser and RuntimeSchemaRowCoder). Validation is disabled because it would
    // perform a BigQuery dry-run in the launcher, before any worker starts.
    var read =
        BigQueryIO.read(new BigQueryRowParser())
            .fromQuery(rewrittenSql)
            .usingStandardSql()
            .withoutValidation()
            .withTemplateCompatibility()
            .withCoder(RuntimeSchemaRowCoder.INSTANCE);

    var queryTempProject = this.bqQuerySpec.getQueryTempProject();
    var queryTempDataset = this.bqQuerySpec.getQueryTempDataset();

    if (queryTempProject != null && queryTempDataset != null) {
      read = read.withQueryTempProjectAndDataset(queryTempProject, queryTempDataset);
    } else if (queryTempDataset != null) {
      read = read.withQueryTempDataset(queryTempDataset);
    }

    return input.apply(bqQuerySpec.getReadDescription(), read);
  }
}
