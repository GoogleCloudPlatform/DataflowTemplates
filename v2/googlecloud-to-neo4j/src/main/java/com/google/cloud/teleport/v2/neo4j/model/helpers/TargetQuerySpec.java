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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.neo4j.importer.v1.pipeline.TargetStep;

/**
 * Convenience object for passing Source metadata, Target metadata, PCollection schema, and nullable
 * source rows, together.
 */
public class TargetQuerySpec {

  private final Schema sourceBeamSchema;
  private final PCollection<Row> nullableSourceRows;
  private final TargetStep step;

  private TargetQuerySpec(
      Schema sourceBeamSchema, PCollection<Row> nullableSourceRows, TargetStep step) {
    this.sourceBeamSchema = sourceBeamSchema;
    this.nullableSourceRows = nullableSourceRows;
    this.step = step;
  }

  public Schema getSourceBeamSchema() {
    return sourceBeamSchema;
  }

  public PCollection<Row> getNullableSourceRows() {
    return nullableSourceRows;
  }

  public TargetStep getTargetStep() {
    return step;
  }

  public static class TargetQuerySpecBuilder {

    private Schema sourceBeamSchema;
    private PCollection<Row> nullableSourceRows;
    private TargetStep step;

    public TargetQuerySpecBuilder sourceBeamSchema(Schema sourceBeamSchema) {
      this.sourceBeamSchema = sourceBeamSchema;
      return this;
    }

    public TargetQuerySpecBuilder nullableSourceRows(PCollection<Row> nullableSourceRows) {
      this.nullableSourceRows = nullableSourceRows;
      return this;
    }

    public TargetQuerySpecBuilder targetStep(TargetStep step) {
      this.step = step;
      return this;
    }

    public TargetQuerySpec build() {
      return new TargetQuerySpec(sourceBeamSchema, nullableSourceRows, step);
    }
  }
}
