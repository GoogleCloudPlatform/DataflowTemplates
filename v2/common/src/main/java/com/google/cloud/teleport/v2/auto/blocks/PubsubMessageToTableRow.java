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
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.auto.Consumes;
import com.google.cloud.teleport.metadata.auto.Outputs;
import com.google.cloud.teleport.metadata.auto.TemplateTransform;
import com.google.cloud.teleport.v2.auto.blocks.PubsubMessageToTableRow.TransformOptions;
import com.google.cloud.teleport.v2.auto.dlq.AutoDLQUtil;
import com.google.cloud.teleport.v2.auto.dlq.BigQueryDeadletterOptions;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.StringUtils;

public class PubsubMessageToTableRow implements TemplateTransform<TransformOptions> {

  public interface TransformOptions
      extends JavascriptTextTransformerOptions, BigQueryDeadletterOptions {

    @TemplateParameter.BigQueryTable(
        order = 1,
        description = "BigQuery output table",
        helpText =
            "BigQuery table location to write the output to. The table’s schema must match the "
                + "input JSON objects.")
    String getOutputTableSpec();
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

  @Consumes(PubsubMessage.class)
  @Outputs(TableRow.class)
  public PCollection<TableRow> transform(
      PCollection<PubsubMessage> input, TransformOptions options) {

    PCollectionTuple udfOut =
        input
            // Map the incoming messages into FailsafeElements so we can recover from failures
            // across multiple transforms.
            .apply("MapToRecord", ParDo.of(new PubsubMessageToFailsafeElementFn()))
            .setCoder(FAILSAFE_ELEMENT_CODER)
            .apply(
                "InvokeUDF",
                FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
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

    String dlqTable =
        StringUtils.isEmpty(options.getOutputDeadletterTable())
            ? options.getOutputTableSpec() + BlockConstants.DEFAULT_DEADLETTER_TABLE_SUFFIX
            : options.getOutputDeadletterTable();

    AutoDLQUtil.writeDLQToBigQueryForPubsubMessage(
        udfOut.get(UDF_DEADLETTER_OUT).setCoder(FAILSAFE_ELEMENT_CODER), dlqTable);
    AutoDLQUtil.writeDLQToBigQueryForPubsubMessage(
        jsonToTableRowOut.get(TRANSFORM_DEADLETTER_OUT).setCoder(FAILSAFE_ELEMENT_CODER), dlqTable);

    return jsonToTableRowOut.get(TRANSFORM_OUT);
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
  public Class<TransformOptions> getOptionsClass() {
    return TransformOptions.class;
  }
}
