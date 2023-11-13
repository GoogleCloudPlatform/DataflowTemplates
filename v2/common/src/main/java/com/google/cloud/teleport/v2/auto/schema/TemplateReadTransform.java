/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.auto.schema;

import com.google.cloud.teleport.v2.auto.blocks.BlockConstants;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

import java.util.Collections;
import java.util.List;

public abstract class TemplateReadTransform<X extends TemplateTransformClass.TemplateBlockOptions>
    extends TemplateTransformClass<X> {
  public abstract PCollectionRowTuple read(PBegin input, X config);

  public PCollectionRowTuple transform(PCollectionRowTuple input, X config) {
    return this.read(input.getPipeline().begin(), config);
  }

  @Override
  public SchemaTransform from(X configuration) {
    return new SchemaTransform() {

      @Override
      public PCollectionRowTuple expand(PCollectionRowTuple input) {
        PCollectionRowTuple output = read(input.getPipeline().begin(), configuration);
        PCollection<Row> rows = output.get(BlockConstants.OUTPUT_TAG);
        return PCollectionRowTuple.of(OUTPUT_ROW_TAG, rows.setCoder(RowCoder.of(rows.getSchema())));
      }
    };
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }
}
