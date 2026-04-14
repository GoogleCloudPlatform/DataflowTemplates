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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.teleport.v2.templates.dofn.SelectTableFn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * A {@link PTransform} that selects a table for each tick based on weighted probability. The weight
 * for a table is (table insertQPS + insertQPS of its descendants). The denominator is the total
 * insertQPS across all tables.
 */
public class SelectTable extends PTransform<PCollection<Long>, PCollection<DataGeneratorTable>> {

  private final PCollectionView<DataGeneratorSchema> schemaView;

  public SelectTable(PCollectionView<DataGeneratorSchema> schemaView) {
    this.schemaView = schemaView;
  }

  @Override
  public PCollection<DataGeneratorTable> expand(PCollection<Long> input) {
    return input.apply(
        "SelectTableFn", ParDo.of(new SelectTableFn(schemaView)).withSideInputs(schemaView));
  }
}
