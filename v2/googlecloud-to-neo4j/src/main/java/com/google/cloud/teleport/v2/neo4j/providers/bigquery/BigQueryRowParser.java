/*
 * Copyright (C) 2026 Google LLC
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

import com.google.api.services.bigquery.model.TableSchema;
import java.io.Serial;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;

/**
 * Parses BigQuery records into Beam {@link Row rows} using the BigQuery schema supplied by the
 * reader at execution time.
 *
 * <p>Resolving the schema on the worker (rather than when the pipeline graph is built) keeps all
 * BigQuery connectivity out of the launcher: no dry-run job is needed before the pipeline starts,
 * and no launch identity requires BigQuery permissions.
 */
public class BigQueryRowParser implements SerializableFunction<SchemaAndRecord, Row> {

  @Serial private static final long serialVersionUID = 1L;

  private static final BigQueryUtils.ConversionOptions CONVERSION_OPTIONS =
      BigQueryUtils.ConversionOptions.builder()
          // Beam's DATETIME only holds millisecond precision, like the TableRow based conversion
          // that this parser replaces. Truncating matches that behaviour instead of failing on
          // BigQuery's microsecond timestamps.
          .setTruncateTimestamps(BigQueryUtils.ConversionOptions.TruncateTimestamps.TRUNCATE)
          .build();

  // All records of a given read share the same TableSchema instance. Caching the last conversion
  // avoids rebuilding the Beam schema for every row.
  private transient volatile CachedSchema cachedSchema;

  @Override
  public Row apply(SchemaAndRecord schemaAndRecord) {
    Schema beamSchema = beamSchemaFor(schemaAndRecord.getTableSchema());
    return BigQueryUtils.toBeamRow(schemaAndRecord.getRecord(), beamSchema, CONVERSION_OPTIONS);
  }

  private Schema beamSchemaFor(TableSchema tableSchema) {
    CachedSchema cached = this.cachedSchema;
    if (cached != null && cached.tableSchema == tableSchema) {
      return cached.beamSchema;
    }
    var resolved = new CachedSchema(tableSchema, BigQueryUtils.fromTableSchema(tableSchema));
    this.cachedSchema = resolved;
    return resolved.beamSchema;
  }

  private record CachedSchema(TableSchema tableSchema, Schema beamSchema) {}
}
