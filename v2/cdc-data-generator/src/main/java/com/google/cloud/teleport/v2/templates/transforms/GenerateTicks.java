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

import com.google.cloud.teleport.v2.templates.dofn.ScaleTicksFn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform} that takes a stream of periodic impulses (1 tick per second) and expands each
 * tick into the total number of root-table QPS declared by the schema side input.
 */
public class GenerateTicks
    extends PTransform<PCollection<org.joda.time.Instant>, PCollection<Long>> {

  private static final Logger LOG = LoggerFactory.getLogger(GenerateTicks.class);
  private final PCollectionView<DataGeneratorSchema> schemaView;

  public GenerateTicks(PCollectionView<DataGeneratorSchema> schemaView) {
    this.schemaView = schemaView;
  }

  @Override
  public PCollection<Long> expand(PCollection<org.joda.time.Instant> input) {
    return input.apply(
        "ScaleTicks", ParDo.of(new ScaleTicksFn(schemaView)).withSideInputs(schemaView));
  }
}
