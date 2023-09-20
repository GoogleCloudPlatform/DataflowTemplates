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

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.auto.value.AutoValue.Builder;
import com.google.cloud.teleport.v2.auto.blocks.PubsubMessageToTableRowTransformProvider.PubsubMessageToTableRowTransformConfiguration;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

@AutoService(SchemaTransformProvider.class)
public class PubsubMessageToTableRowTransformProvider
    extends TypedSchemaTransformProvider<PubsubMessageToTableRowTransformConfiguration> {

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract class PubsubMessageToTableRowTransformConfiguration {

    String getJavascriptTextTransformGcsPath();

    String getJavascriptTextTransformFunctionName();

    public static Builder builder() {
      return new AutoValue_PubsubMessageToTableRowTransformConfiguration.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setJavascriptTextTransformGcsPath(String path);

      public abstract Builder setJavascriptTextTransformFunctionName(String name);

      public abstract PubsubMessageToTableRowTransformConfiguration build();
    }
  }

  /** The tag for the main output for the UDF. */
  public static final TupleTag<FailsafeElement<PubsubMessage, String>> UDF_OUT =
      new TupleTag<>() {};

  /** The tag for the main output of the json transformation. */
  public static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<>() {};

  /** The tag for the dead-letter output of the udf. */
  public static final TupleTag<FailsafeElement<PubsubMessage, String>> UDF_DEADLETTER_OUT =
      new TupleTag<>() {};

  /** The tag for the dead-letter output of the json to table row transform. */
  public static final TupleTag<FailsafeElement<PubsubMessage, String>> TRANSFORM_DEADLETTER_OUT =
      new TupleTag<>() {};

  private static final FailsafeElementCoder<PubsubMessage, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(PubsubMessageWithAttributesAndMessageIdCoder.of()),
          NullableCoder.of(StringUtf8Coder.of()));

  @Override
  public Class<PubsubMessageToTableRowTransformConfiguration> configurationClass() {
    return PubsubMessageToTableRowTransformConfiguration.class;
  }

  @Override
  public SchemaTransform from(PubsubMessageToTableRowTransformConfiguration configuration) {
    return new SchemaTransform() {
      @Override
      public PCollectionTuple expand(PCollectionTuple input) {
        PCollectionTuple udfOut =
            input
                .get("output")
                .apply("MapToRecord", ParDo.of(new PubsubMessageToFailsafeElementFn()))
                .setCoder(FAILSAFE_ELEMENT_CODER)
                .apply(
                    "InvokeUDF",
                    FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
                        .setFileSystemPath(configuration.getJavascriptTextTransformGcsPath())
                        .setFunctionName(configuration.getJavascriptTextTransformFunctionName())
                        .setSuccessTag(UDF_OUT)
                        .setFailureTag(UDF_DEADLETTER_OUT)
                        .build());

        PCollectionTuple jsonToTableRowOut =
            udfOut
                .get(UDF_OUT)
                .setCoder(FAILSAFE_ELEMENT_CODER)
                .apply(
                    "JsonToTableRow",
                    FailsafeJsonToTableRow.<PubsubMessage>newBuilder()
                        .setSuccessTag(TRANSFORM_OUT)
                        .setFailureTag(TRANSFORM_DEADLETTER_OUT)
                        .build());

        PCollectionList<FailsafeElement<PubsubMessage, String>> pcs =
            PCollectionList.of(udfOut.get(UDF_DEADLETTER_OUT).setCoder(FAILSAFE_ELEMENT_CODER))
                .and(
                    jsonToTableRowOut
                        .get(TRANSFORM_DEADLETTER_OUT)
                        .setCoder(FAILSAFE_ELEMENT_CODER));

        return PCollectionTuple.of("output", jsonToTableRowOut.get(TRANSFORM_OUT))
            .and("error", pcs.apply(Flatten.pCollections()));
      }
    };
  }

  private class PubsubMessageToFailsafeElementFn
      extends DoFn<PubsubMessage, FailsafeElement<PubsubMessage, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message = context.element();
      context.output(
          FailsafeElement.of(message, new String(message.getPayload(), StandardCharsets.UTF_8)));
    }
  }

  @Override
  public String identifier() {
    return "blocks:schematransform:org.apache.beam:pubsub_to_bigquery:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Arrays.asList("output");
  }
}
