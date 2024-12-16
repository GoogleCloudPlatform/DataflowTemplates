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

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.neo4j.model.helpers.BigQuerySpec;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
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

    var read =
        BigQueryIO.readTableRowsWithSchema()
            .fromQuery(rewrittenSql)
            .usingStandardSql()
            .withTemplateCompatibility();

    var queryTempProject = this.bqQuerySpec.getQueryTempProject();
    var queryTempDataset = this.bqQuerySpec.getQueryTempDataset();

    if (queryTempProject != null && queryTempDataset != null) {
      read = read.withQueryTempProjectAndDataset(queryTempProject, queryTempDataset);
    } else if (queryTempDataset != null) {
      read = read.withQueryTempDataset(queryTempDataset);
    }

    PCollection<TableRow> sourceRows = input.apply(bqQuerySpec.getReadDescription(), read);

    Schema beamSchema = sourceRows.getSchema();
    Coder<Row> rowCoder = SchemaCoder.of(beamSchema);
    LOG.info("Beam schema: {}", beamSchema);
    return sourceRows
        .apply(
            bqQuerySpec.getCastDescription(),
            MapElements.into(TypeDescriptor.of(Row.class)).via(sourceRows.getToRowFunction()))
        .setCoder(rowCoder);
  }
}
