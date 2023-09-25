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
package com.google.cloud.teleport.v2.auto.blocks;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.metadata.auto.TemplateTransform;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

abstract class TemplateTransformClass<X, Y extends TemplateTransformClass.Configuration>
    extends TypedSchemaTransformProvider<Y> implements TemplateTransform<X> {
  protected static final String INPUT_ROW_TAG = "input";
  protected static final String OUTPUT_ROW_TAG = "output";
  protected static final String ERROR_ROW_TAG = "errors";

  @Override
  public List<String> outputCollectionNames() {
    return List.of(OUTPUT_ROW_TAG, ERROR_ROW_TAG);
  }

  public abstract static class Configuration {
    abstract void validate();

    @SchemaFieldDescription("This option specifies whether and where to output unwritable rows.")
    @Nullable
    abstract TemplateTransformClass.Configuration.ErrorHandling getErrorHandling();

    @AutoValue
    public abstract static class ErrorHandling {
      @SchemaFieldDescription("The name of the output PCollection containing failed writes.")
      public abstract String getOutput();

      public static Configuration.ErrorHandling.Builder builder() {
        return new AutoValue_TemplateTransformClass_Configuration_ErrorHandling.Builder();
      }

      @AutoValue.Builder
      public abstract static class Builder {
        public abstract Configuration.ErrorHandling.Builder setOutput(String output);

        public abstract Configuration.ErrorHandling build();
      }
    }

    public abstract static class Builder<T extends Builder<?>> {
      public abstract T setErrorHandling(
          TemplateTransformClass.Configuration.ErrorHandling errorHandling);
    }
  }
  ;

  public abstract PCollectionRowTuple transform(PCollectionRowTuple input, Y config);

  @Override
  public SchemaTransform from(Y configuration) {
    return new TemplateSchemaTransform(configuration);
  }

  private class TemplateSchemaTransform extends SchemaTransform {
    private final Y configuration;

    private TemplateSchemaTransform(Y configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> messages = input.get(INPUT_ROW_TAG);

      PCollectionRowTuple output =
          transform(PCollectionRowTuple.of(BlockConstants.OUTPUT_TAG, messages), configuration);

      PCollection<Row> rows = output.get(BlockConstants.OUTPUT_TAG);

      PCollectionRowTuple outputRowTuple = PCollectionRowTuple.of(OUTPUT_ROW_TAG, rows);
      if (configuration.getErrorHandling() != null) {
        PCollection<Row> errors = output.get(BlockConstants.ERROR_TAG);
        errors.setCoder(RowCoder.of(errors.getSchema()));
        outputRowTuple = outputRowTuple.and(configuration.getErrorHandling().getOutput(), errors);
      }

      return outputRowTuple;
    }
  }

  @Override
  public List<String> inputCollectionNames() {
    return List.of(INPUT_ROW_TAG);
  }
}

abstract class TemplateReadTransform<X, Y extends TemplateTransformClass.Configuration>
    extends TemplateTransformClass<X, Y> {
  public abstract PCollectionRowTuple transform(PBegin input, Y config);

  public PCollectionRowTuple transform(PCollectionRowTuple input, Y config) {
    return this.transform(input.getPipeline().begin(), config);
  }

  @Override
  public SchemaTransform from(Y configuration) {
    return new SchemaTransform() {

      @Override
      public PCollectionRowTuple expand(PCollectionRowTuple input) {
        PCollectionRowTuple output = transform(input.getPipeline().begin(), configuration);
        PCollection<Row> rows = output.get(BlockConstants.OUTPUT_TAG);
        return PCollectionRowTuple.of(OUTPUT_ROW_TAG, rows.setCoder(RowCoder.of(rows.getSchema())));
      }
    };
  }
}

abstract class TemplateWriteTransform<X, Y extends TemplateTransformClass.Configuration>
    extends TemplateTransformClass<X, Y> {
  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  private static class NoOutputDoFn<T> extends DoFn<T, Row> {
    @ProcessElement
    public void process(ProcessContext c) {}
  }

  @Override
  public SchemaTransform from(Y configuration) {
    return new TemplateSchemaTransform(configuration);
  }

  private class TemplateSchemaTransform extends SchemaTransform {
    private final Y configuration;

    private TemplateSchemaTransform(Y configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> messages = input.get(INPUT_ROW_TAG);

      PCollectionRowTuple output =
          transform(PCollectionRowTuple.of(BlockConstants.OUTPUT_TAG, messages), configuration);

      PCollection<Row> rows =
          output
              .get(BlockConstants.OUTPUT_TAG)
              .apply("post-write", ParDo.of(new NoOutputDoFn<>()))
              .setRowSchema(Schema.of());

      PCollectionRowTuple outputRowTuple = PCollectionRowTuple.of(OUTPUT_ROW_TAG, rows);
      if (configuration.getErrorHandling() != null) {
        PCollection<Row> errors = output.get(BlockConstants.ERROR_TAG);
        errors.setCoder(RowCoder.of(errors.getSchema()));
        outputRowTuple = outputRowTuple.and(configuration.getErrorHandling().getOutput(), errors);
      }

      return outputRowTuple;
    }
  }
}
