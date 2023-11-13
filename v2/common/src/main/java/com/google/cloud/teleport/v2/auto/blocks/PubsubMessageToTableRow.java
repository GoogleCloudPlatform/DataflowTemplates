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
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.auto.Consumes;
import com.google.cloud.teleport.metadata.auto.DlqOutputs;
import com.google.cloud.teleport.metadata.auto.Outputs;
import com.google.cloud.teleport.v2.auto.schema.RowTypes;
import com.google.cloud.teleport.v2.auto.schema.TemplateOptionSchema;
import com.google.cloud.teleport.v2.auto.schema.TemplateTransformClass;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoService(SchemaTransformProvider.class)
public class PubsubMessageToTableRow
    extends TemplateTransformClass<PubsubMessageToTableRow.TransformOptions> {

  @DefaultSchema(TemplateOptionSchema.class)
  public interface TransformOptions extends TemplateBlockOptions {

    @TemplateParameter.GcsReadFile(
        order = 1,
        optional = true,
        description = "Cloud Storage path to Javascript UDF source.",
        helpText =
            "The Cloud Storage path pattern for the JavaScript code containing your user-defined "
                + "functions.",
        example = "gs://your-bucket/your-function.js")
    String getJavascriptTextTransformGcsPath();

    void setJavascriptTextTransformGcsPath(String value);

    @TemplateParameter.Text(
        order = 2,
        optional = true,
        regexes = {"[a-zA-Z0-9_]+"},
        description = "UDF Javascript Function Name",
        helpText =
            "The name of the function to call from your JavaScript file. Use only letters, digits, and underscores.",
        example = "'transform' or 'transform_udf1'")
    String getJavascriptTextTransformFunctionName();

    void setJavascriptTextTransformFunctionName(String value);

    @TemplateParameter.Integer(
        order = 3,
        optional = true,
        description = "JavaScript UDF auto-reload interval (minutes).",
        helpText =
            "Define the interval that workers may check for JavaScript UDF changes to reload the files.")
    @Default.Integer(0)
    @Nullable
    Integer getJavascriptTextTransformReloadIntervalMinutes();

    void setJavascriptTextTransformReloadIntervalMinutes(Integer value);
  }

  @Override
  public @NonNull String identifier() {
    return "blocks:external:org.apache.beam:pubsub_to_bigquery:v1";
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

  @Consumes(RowTypes.PubSubMessageRow.class)
  @Outputs(RowTypes.SchemaTableRow.class)
  @DlqOutputs(RowTypes.FailsafePubSubRow.class)
  public PCollectionRowTuple transform(PCollectionRowTuple input, TransformOptions options) {
    PCollectionTuple udfOut =
        input
            .get(BlockConstants.OUTPUT_TAG)
            // Map the incoming messages into FailsafeElements so we can recover from failures
            // across multiple transforms.
            .apply("MapToRecord", ParDo.of(new PubSubMessageToFailsafeElementFn()))
            .setCoder(FAILSAFE_ELEMENT_CODER)
            .apply(
                "InvokeUDF",
                JavascriptTextTransformer.FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
                    .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                    .setFunctionName(options.getJavascriptTextTransformFunctionName())
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
            .and(jsonToTableRowOut.get(TRANSFORM_DEADLETTER_OUT).setCoder(FAILSAFE_ELEMENT_CODER));

    PCollection<Row> outputRows =
        jsonToTableRowOut
            .get(TRANSFORM_OUT)
            .apply(
                MapElements.into(TypeDescriptor.of(Row.class))
                    .via(RowTypes.SchemaTableRow::TableRowToRow))
            .setCoder(RowCoder.of(RowTypes.SchemaTableRow.SCHEMA));

    PCollection<Row> errors =
        pcs.apply(Flatten.pCollections())
            .apply(RowTypes.FailsafePubSubRow.MapToRow.of());

    return PCollectionRowTuple.of(BlockConstants.OUTPUT_TAG, outputRows)
        .and(BlockConstants.ERROR_TAG, errors);
  }

  private static class PubSubMessageToFailsafeElementFn
      extends DoFn<Row, FailsafeElement<PubsubMessage, String>> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message =
          RowTypes.PubSubMessageRow.RowToPubSubMessage(Objects.requireNonNull(context.element()));
      context.output(
          FailsafeElement.of(message, new String(message.getPayload(), StandardCharsets.UTF_8)));
    }
  }

  @Override
  public Class<TransformOptions> getOptionsClass() {
    return TransformOptions.class;
  }
}
