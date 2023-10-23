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
import com.google.cloud.teleport.metadata.auto.TemplateTransform;
import com.google.cloud.teleport.v2.auto.blocks.PubsubMessageToTableRow.TransformOptions;
import com.google.cloud.teleport.v2.auto.dlq.BigQueryDeadletterOptions;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.NonNull;

import static com.google.cloud.teleport.v2.auto.blocks.StandardCoderConverters.RowToPubSubMessage;
import static com.google.cloud.teleport.v2.auto.blocks.StandardCoderConverters.failsafeElementToRow;
import static com.google.cloud.teleport.v2.auto.blocks.StandardCoderConverters.tableRowToRow;

@AutoService(ExternalTransformRegistrar.class)
public class PubsubMessageToTableRow
    implements TemplateTransform<TransformOptions>, ExternalTransformRegistrar {

  private static final String URN = "blocks:external:org.apache.beam:pubsub_to_bigquery:v1";

  private static class Builder
      implements ExternalTransformBuilder<
          Configuration, @NonNull PCollection<Row>, @NonNull PCollectionRowTuple> {
    @Override
    public @NonNull
        PTransform<@NonNull PCollection<Row>, @NonNull PCollectionRowTuple> buildExternal(
            Configuration config) {
      return new PTransform<>() {
        @Override
        public @NonNull PCollectionRowTuple expand(@NonNull PCollection<Row> input) {
          PCollectionTuple output = underlyingTransform(RowToPubSubMessage(input), config);

          PCollection<Row> tableRows = tableRowToRow(output.get(BlockConstants.OUTPUT_TAG));

          PCollection<FailsafeElement<PubsubMessage, String>> errors = output.get(BlockConstants.ERROR_TAG_PS);
          PCollection<Row> normalizedErrors = failsafeElementToRow(errors);

          return PCollectionRowTuple.of("output", tableRows)
              .and("errors", normalizedErrors);
//          return tableRows;
        }
      };
    }
  }

  @Override
  public Map<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> knownBuilders() {
    return ImmutableMap.of(URN, Builder.class);
  }

  // TODO(polber) - See if this Config class can be generated programmatically from TransformOptions
  // Interface
  public static class Configuration {
    String javascriptTextTransformGcsPath = "";
    String javascriptTextTransformFunctionName = "";

    public void setJavascriptTextTransformGcsPath(String path) {
      this.javascriptTextTransformGcsPath = path;
    }

    public String getJavascriptTextTransformGcsPath() {
      return this.javascriptTextTransformGcsPath;
    }

    public void setJavascriptTextTransformFunctionName(String name) {
      this.javascriptTextTransformFunctionName = name;
    }

    public String getJavascriptTextTransformFunctionName() {
      return this.javascriptTextTransformFunctionName;
    }

    public static Configuration fromOptions(TransformOptions options) {
      Configuration config = new Configuration();
      config.setJavascriptTextTransformGcsPath(options.getJavascriptTextTransformGcsPath());
      config.setJavascriptTextTransformFunctionName(
          options.getJavascriptTextTransformFunctionName());
      return config;
    }
  }

  public interface TransformOptions
      extends JavascriptTextTransformerOptions, BigQueryDeadletterOptions {

    @TemplateParameter.BigQueryTable(
        order = 1,
        description = "BigQuery output table",
        helpText =
            "BigQuery table location to write the output to. The table's schema must match the "
                + "input JSON objects.")
    String getOutputTableSpec();

    void setOutputTableSpec(String tableSpec);
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
  @DlqOutputs(
      value = FailsafeElement.class,
      types = {PubsubMessage.class, String.class})
  public PCollectionTuple transform(PCollection<PubsubMessage> input, TransformOptions options) {
    return underlyingTransform(input, Configuration.fromOptions(options));
  }

  static PCollectionTuple underlyingTransform(PCollection<PubsubMessage> input, Configuration config) {
    PCollectionTuple udfOut =
        input
            // Map the incoming messages into FailsafeElements so we can recover from failures
            // across multiple transforms.
            .apply("MapToRecord", ParDo.of(new PubsubMessageToFailsafeElementFn()))
            .setCoder(FAILSAFE_ELEMENT_CODER)
            .apply(
                "InvokeUDF",
                FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
                    .setFileSystemPath(config.getJavascriptTextTransformGcsPath())
                    .setFunctionName(config.getJavascriptTextTransformFunctionName())
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

    // String dlqTable =
    //     StringUtils.isEmpty(options.getOutputDeadletterTable())
    //         ? options.getOutputTableSpec() + BlockConstants.DEFAULT_DEADLETTER_TABLE_SUFFIX
    //         : options.getOutputDeadletterTable();

    // AutoDLQUtil.writeDLQToBigQueryForPubsubMessage(
    //     udfOut.get(UDF_DEADLETTER_OUT).setCoder(FAILSAFE_ELEMENT_CODER), dlqTable);
    // AutoDLQUtil.writeDLQToBigQueryForPubsubMessage(
    //     jsonToTableRowOut.get(TRANSFORM_DEADLETTER_OUT).setCoder(FAILSAFE_ELEMENT_CODER),
    // dlqTable);

    PCollectionList<FailsafeElement<PubsubMessage, String>> pcs =
        PCollectionList.of(udfOut.get(UDF_DEADLETTER_OUT).setCoder(FAILSAFE_ELEMENT_CODER))
            .and(jsonToTableRowOut.get(TRANSFORM_DEADLETTER_OUT).setCoder(FAILSAFE_ELEMENT_CODER));

    // return jsonToTableRowOut.get(TRANSFORM_OUT);

    return PCollectionTuple.of(BlockConstants.OUTPUT_TAG, jsonToTableRowOut.get(TRANSFORM_OUT))
        .and(BlockConstants.ERROR_TAG_PS, pcs.apply(Flatten.pCollections()));
  }

  private static class PubsubMessageToFailsafeElementFn
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
