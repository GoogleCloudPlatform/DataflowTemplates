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

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.auto.TemplateTransform;
import java.util.List;

import com.google.cloud.teleport.v2.auto.blocks.BlockConstants;
import com.google.common.base.Strings;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class TemplateTransformClass<X extends TemplateTransformClass.TemplateBlockOptions>
    extends TypedSchemaTransformProvider<X> implements TemplateTransform<X> {
  protected static final String INPUT_ROW_TAG = "input";
  protected static final String OUTPUT_ROW_TAG = "output";
  protected static final String ERROR_ROW_TAG = "errors";

  @Override
  public List<String> outputCollectionNames() {
    return List.of(OUTPUT_ROW_TAG, ERROR_ROW_TAG);
  }

  @Override
  public @NonNull Class<X> configurationClass() {
    return getOptionsClass();
  }

  void validate(X options) {
    String invalidConfigMessage = "Invalid PubSubMessageToTableRow configuration: ";
    if (options.getErrorHandling() != null) {
      checkArgument(
          !Strings.isNullOrEmpty(options.getErrorHandling().getOutput()),
          invalidConfigMessage + "Output must not be empty if error handling specified.");
    }
  }

  public interface TemplateBlockOptions extends PipelineOptions {

    @SchemaFieldDescription("This option specifies whether and where to output unwritable rows.")
    @Nullable
    ErrorHandling getErrorHandling();

    void setErrorHandling(ErrorHandling errorHandling);

//    interface ErrorHandling extends PipelineOptions {
//      @SchemaFieldDescription("The name of the output PCollection containing failed reads.")
//      String getOutput();
//
//      void setOutput(String output);
//    }
    @AutoValue
    abstract class ErrorHandling {
      @SchemaFieldDescription("The name of the output PCollection containing failed reads.")
      public abstract String getOutput();

      public static TemplateBlockOptions.ErrorHandling.Builder builder() {
        return new AutoValue_TemplateTransformClass_TemplateBlockOptions_ErrorHandling.Builder();
      }

      @AutoValue.Builder
      public abstract static class Builder {
        public abstract TemplateBlockOptions.ErrorHandling.Builder setOutput(
            String output);

        public abstract TemplateBlockOptions.ErrorHandling build();
      }
    }
  }

  public abstract PCollectionRowTuple transform(PCollectionRowTuple input, X options);

  @Override
  public SchemaTransform from(X options) {
    return new TemplateSchemaTransform(options);
  }

  private class TemplateSchemaTransform extends SchemaTransform {
    private final X options;

    private TemplateSchemaTransform(X options) {
      validate(options);
      this.options = options;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> messages = input.get(INPUT_ROW_TAG);

      PCollectionRowTuple output =
          transform(PCollectionRowTuple.of(BlockConstants.OUTPUT_TAG, messages), options);

      PCollection<Row> rows = output.get(BlockConstants.OUTPUT_TAG);

      PCollectionRowTuple outputRowTuple = PCollectionRowTuple.of(OUTPUT_ROW_TAG, rows);
      if (options.getErrorHandling() != null) {
        PCollection<Row> errors = output.get(BlockConstants.ERROR_TAG);
        errors.setCoder(RowCoder.of(errors.getSchema()));
        outputRowTuple = outputRowTuple.and(options.getErrorHandling().getOutput(), errors);
      }

      return outputRowTuple;
    }
  }

  @Override
  public List<String> inputCollectionNames() {
    return List.of(INPUT_ROW_TAG);
  }
}
