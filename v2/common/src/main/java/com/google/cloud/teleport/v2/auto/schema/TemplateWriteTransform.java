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

import java.util.Collections;
import java.util.List;

import com.google.cloud.teleport.v2.auto.blocks.BlockConstants;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

public abstract class TemplateWriteTransform<X extends TemplateTransformClass.TemplateBlockOptions>
    extends TemplateTransformClass<X> {
  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  private static class NoOutputDoFn<T> extends DoFn<T, Row> {
    @ProcessElement
    public void process(ProcessContext c) {}
  }

  @Override
  public SchemaTransform from(X configuration) {
    return new TemplateSchemaTransform(configuration);
  }

  private class TemplateSchemaTransform extends SchemaTransform {
    private final X configuration;

    private TemplateSchemaTransform(X configuration) {
      //      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> messages = input.get(INPUT_ROW_TAG);

      PCollectionRowTuple output =
          transform(PCollectionRowTuple.of(BlockConstants.OUTPUT_TAG, messages), configuration);

      PCollection<Row> rows =
          output
              .get(BlockConstants.ERROR_TAG)
              .apply("post-write", ParDo.of(new NoOutputDoFn<>()))
              .setRowSchema(Schema.of());

      PCollectionRowTuple outputRowTuple = PCollectionRowTuple.of(OUTPUT_ROW_TAG, rows);
      //      if (configuration.getErrorHandling() != null) {
      //        PCollection<Row> errors = output.get(BlockConstants.ERROR_TAG);
      //        errors.setCoder(RowCoder.of(errors.getSchema()));
      //        outputRowTuple = outputRowTuple.and(configuration.getErrorHandling().getOutput(),
      // errors);
      //      }

      return outputRowTuple;
    }
  }
}
