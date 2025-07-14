/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.spanner.spannerio;

import static com.google.cloud.teleport.spanner.spannerio.StructUtils.structTypeToBeamRowSchema;

import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import org.apache.beam.sdk.schemas.Schema;

/**
 * WARNING: This file is forked from Apache Beam. Ensure corresponding changes are made in Apache
 * Beam to prevent code divergence. TODO: (b/402322178) Remove this local copy.
 */
class SpannerTableSourceDef
    implements com.google.cloud.teleport.spanner.spannerio.SpannerSourceDef {

  private final SpannerConfig config;
  private final String table;
  private final Iterable<String> columns;

  static SpannerTableSourceDef create(
      SpannerConfig config, String table, Iterable<String> columns) {
    return new SpannerTableSourceDef(config, table, columns);
  }

  private SpannerTableSourceDef(SpannerConfig config, String table, Iterable<String> columns) {
    this.table = table;
    this.config = config;
    this.columns = columns;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getBeamSchema() {
    Schema beamSchema;
    try (SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(config)) {
      try (ReadContext readContext = spannerAccessor.getDatabaseClient().singleUse()) {
        ResultSet result = readContext.read(table, KeySet.all(), columns, Options.limit(1));
        if (result.next()) {
          beamSchema = structTypeToBeamRowSchema(result.getMetadata().getRowType(), true);
        } else {
          throw new SpannerSchemaRetrievalException("Cannot find Spanner table.");
        }
      }
    } catch (Exception e) {
      throw new SpannerSchemaRetrievalException("Exception while trying to retrieve schema", e);
    }
    return beamSchema;
  }
}
