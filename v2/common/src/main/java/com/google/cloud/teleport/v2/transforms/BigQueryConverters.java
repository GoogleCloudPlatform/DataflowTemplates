/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.teleport.v2.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Throwables;

/**
 * Common transforms for Teleport BigQueryIO.
 */
public class BigQueryConverters {

  /**
   * The {@link FailsafeJsonToTableRow} transform converts JSON strings to {@link TableRow} objects.
   * The transform accepts a {@link FailsafeElement} object so the original payload of the incoming
   * record can be maintained across multiple series of transforms.
   */
  @AutoValue
  public abstract static class FailsafeJsonToTableRow<T>
      extends PTransform<PCollection<FailsafeElement<T, String>>, PCollectionTuple> {

    public abstract TupleTag<TableRow> successTag();

    public abstract TupleTag<FailsafeElement<T, String>> failureTag();

    public static <T> Builder<T> newBuilder() {
      return new AutoValue_BigQueryConverters_FailsafeJsonToTableRow.Builder<>();
    }

    /**
     * Builder for {@link FailsafeJsonToTableRow}.
     */
    @AutoValue.Builder
    public abstract static class Builder<T> {

      public abstract Builder<T> setSuccessTag(TupleTag<TableRow> successTag);

      public abstract Builder<T> setFailureTag(TupleTag<FailsafeElement<T, String>> failureTag);

      public abstract FailsafeJsonToTableRow<T> build();
    }

    @Override
    public PCollectionTuple expand(PCollection<FailsafeElement<T, String>> failsafeElements) {
      return failsafeElements.apply(
          "JsonToTableRow",
          ParDo.of(
              new DoFn<FailsafeElement<T, String>, TableRow>() {
                @ProcessElement
                public void processElement(ProcessContext context) {
                  FailsafeElement<T, String> element = context.element();
                  String json = element.getPayload();

                  try {
                    TableRow row = convertJsonToTableRow(json);
                    context.output(row);
                  } catch (Exception e) {
                    context.output(
                        failureTag(),
                        FailsafeElement.of(element)
                            .setErrorMessage(e.getMessage())
                            .setStacktrace(Throwables.getStackTraceAsString(e)));
                  }
                }
              })
              .withOutputTags(successTag(), TupleTagList.of(failureTag())));
    }
  }

  /**
   * Converts a JSON string to a {@link TableRow} object. If the data fails to convert, a {@link
   * RuntimeException} will be thrown.
   *
   * @param json The JSON string to parse.
   * @return The parsed {@link TableRow} object.
   */
  private static TableRow convertJsonToTableRow(String json) {
    TableRow row;
    // Parse the JSON into a {@link TableRow} object.
    try (InputStream inputStream =
        new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
      row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);

    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize json to table row: " + json, e);
    }

    return row;
  }
}
