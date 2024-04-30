/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.elasticsearch.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.elasticsearch.templates.PubSubToElasticsearch;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer;
import com.google.cloud.teleport.v2.transforms.PythonExternalTextTransformer;
import com.google.cloud.teleport.v2.transforms.PythonExternalTextTransformer.RowToPubSubFailsafeElementFn;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * The {@link PubSubMessageToJsonDocument} class is a {@link PTransform} which transforms incoming
 * {@link PubsubMessage} objects into JSON objects for insertion into Elasticsearch while applying
 * an optional UDF to the input. The executions of the UDF and transformation to Json objects is
 * done in a fail-safe way by wrapping the element with it's original payload inside the {@link
 * FailsafeElement} class. The {@link PubSubMessageToJsonDocument} transform will output a {@link
 * PCollectionTuple} which contains all output and dead-letter {@link PCollection}.
 *
 * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
 *
 * <ul>
 *   <li>{@link PubSubToElasticsearch#TRANSFORM_OUT} - Contains all records successfully converted
 *       to JSON objects.
 *   <li>{@link PubSubToElasticsearch#TRANSFORM_ERROR_OUTPUT_OUT} - Contains all {@link
 *       FailsafeElement} records which couldn't be converted to table rows.
 * </ul>
 */
@AutoValue
public abstract class PubSubMessageToJsonDocument
    extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

  public static Builder newBuilder() {
    return new AutoValue_PubSubMessageToJsonDocument.Builder();
  }

  @Nullable
  public abstract String javascriptTextTransformGcsPath();

  @Nullable
  public abstract String javascriptTextTransformFunctionName();

  @Nullable
  public abstract String pythonExternalTextTransformGcsPath();

  @Nullable
  public abstract String pythonExternalTextTransformFunctionName();

  @Nullable
  public abstract Integer javascriptTextTransformReloadIntervalMinutes();

  @Override
  public PCollectionTuple expand(PCollection<PubsubMessage> input) {

    boolean isPythonUdf = !Strings.isNullOrEmpty(pythonExternalTextTransformGcsPath());
    boolean isJavascriptUdf = !Strings.isNullOrEmpty(javascriptTextTransformGcsPath());

    if (isPythonUdf && isJavascriptUdf) {
      throw new IllegalArgumentException(
          "Either javascript or Python gcs path must be provided, but not both.");
    }

    if (isPythonUdf) {
      // Map the incoming messages into FailsafeElements so we can recover from failures
      // across multiple transforms.
      PCollection<Row> failsafeElements =
          input
              .apply(
                  "MapToRecord",
                  PythonExternalTextTransformer.FailsafeRowPythonExternalUdf
                      .pubSubMappingFunction())
              .setCoder(
                  RowCoder.of(
                      PythonExternalTextTransformer.FailsafeRowPythonExternalUdf.ROW_SCHEMA));

      return failsafeElements
          .apply(
              "InvokeUDF",
              PythonExternalTextTransformer.FailsafePythonExternalUdf.newBuilder()
                  .setFileSystemPath(pythonExternalTextTransformGcsPath())
                  .setFunctionName(pythonExternalTextTransformFunctionName())
                  .build())
          .setCoder(
              RowCoder.of(
                  PythonExternalTextTransformer.FailsafeRowPythonExternalUdf.FAILSAFE_SCHEMA))
          .apply(
              "MapRowsToFailsafeElements",
              ParDo.of(
                      new RowToPubSubFailsafeElementFn(
                          PubSubToElasticsearch.TRANSFORM_OUT,
                          PubSubToElasticsearch.TRANSFORM_ERROR_OUTPUT_OUT))
                  .withOutputTags(
                      PubSubToElasticsearch.TRANSFORM_OUT,
                      TupleTagList.of(PubSubToElasticsearch.TRANSFORM_ERROR_OUTPUT_OUT)));
    } else {
      // Map the incoming messages into FailsafeElements so we can recover from failures
      // across multiple transforms.
      PCollection<FailsafeElement<PubsubMessage, String>> failsafeElements =
          input.apply("MapToRecord", ParDo.of(new PubsubMessageToFailsafeElementFn()));

      // If a Udf is supplied then use it to parse the PubSubMessages.
      if (javascriptTextTransformGcsPath() != null) {
        return failsafeElements.apply(
            "InvokeUDF",
            JavascriptTextTransformer.FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
                .setFileSystemPath(javascriptTextTransformGcsPath())
                .setFunctionName(javascriptTextTransformFunctionName())
                .setReloadIntervalMinutes(javascriptTextTransformReloadIntervalMinutes())
                .setSuccessTag(PubSubToElasticsearch.TRANSFORM_OUT)
                .setFailureTag(PubSubToElasticsearch.TRANSFORM_ERROR_OUTPUT_OUT)
                .build());
      } else {
        return failsafeElements.apply(
            "ProcessPubSubMessages",
            ParDo.of(new ProcessFailsafePubSubFn())
                .withOutputTags(
                    PubSubToElasticsearch.TRANSFORM_OUT,
                    TupleTagList.of(PubSubToElasticsearch.TRANSFORM_ERROR_OUTPUT_OUT)));
      }
    }
  }

  /** Builder for {@link PubSubMessageToJsonDocument}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setJavascriptTextTransformGcsPath(
        String javascriptTextTransformGcsPath);

    public abstract Builder setJavascriptTextTransformFunctionName(
        String javascriptTextTransformFunctionName);

    public abstract Builder setJavascriptTextTransformReloadIntervalMinutes(
        Integer javascriptTextTransformReloadIntervalMinutes);

    public abstract Builder setPythonExternalTextTransformGcsPath(
        String pythonExternalTextTransformGcsPath);

    public abstract Builder setPythonExternalTextTransformFunctionName(
        String pythonExternalTextTransformFunctionName);

    public abstract PubSubMessageToJsonDocument build();
  }
}
