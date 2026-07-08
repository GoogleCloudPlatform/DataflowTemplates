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

import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import org.apache.beam.sdk.schemas.Schema;

/**
 * WARNING: This file is forked from Apache Beam. Ensure corresponding changes are made in Apache
 * Beam to prevent code divergence. TODO: (b/402322178) Remove this local copy.
 */
class SpannerQuerySourceDef
    implements com.google.cloud.teleport.spanner.spannerio.SpannerSourceDef {

  private final SpannerConfig config;
  private final Statement query;

  static SpannerQuerySourceDef create(SpannerConfig config, Statement query) {
    return new SpannerQuerySourceDef(config, query);
  }

  private SpannerQuerySourceDef(SpannerConfig config, Statement query) {
    this.config = config;
    this.query = query;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getBeamSchema() {
    Schema beamSchema;
    try (com.google.cloud.teleport.spanner.spannerio.SpannerAccessor spannerAccessor =
        SpannerAccessor.getOrCreate(config)) {
      try (ReadContext readContext = spannerAccessor.getDatabaseClient().singleUse()) {
        ResultSet result = readContext.analyzeQuery(query, ReadContext.QueryAnalyzeMode.PLAN);
        result.next();
        beamSchema = structTypeToBeamRowSchema(result.getMetadata().getRowType(), true);
      }
    } catch (Exception e) {
      throw new SpannerSchemaRetrievalException("Exception while trying to retrieve schema", e);
    }
    return beamSchema;
  }
}
