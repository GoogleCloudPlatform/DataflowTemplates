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

import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.service.AutoService;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.auto.Consumes;
import com.google.cloud.teleport.metadata.auto.Outputs;
import com.google.cloud.teleport.v2.auto.schema.RowTypes;
import com.google.cloud.teleport.v2.auto.schema.TemplateOptionSchema;
import com.google.cloud.teleport.v2.auto.schema.TemplateWriteTransform;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;

import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.NonNull;

@AutoService(SchemaTransformProvider.class)
public class WriteToBigQuery extends TemplateWriteTransform<WriteToBigQuery.SinkOptions> {

  @DefaultSchema(TemplateOptionSchema.class)
  public interface SinkOptions extends TemplateBlockOptions {

    @TemplateParameter.BigQueryTable(
        order = 1,
        optional = true,
        description = "BigQuery output table",
        helpText =
            "BigQuery table location to write the output to. The table's schema must match the "
                + "input JSON objects.")
    String getOutputTableSpec();

    void setOutputTableSpec(String value);

    @TemplateParameter.Boolean(
        order = 1,
        optional = true,
        description = "Use BigQuery Storage Write API",
        helpText =
            "If enabled (set to true) the pipeline will use Storage Write API when writing the data"
                + " to BigQuery (see"
                + " https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api)."
                + " If this is enabled and at-least-once semantics (useStorageWriteApiAtLeastOnce)"
                + " option is off then \"Number of streams for BigQuery Storage Write API\" and"
                + " \"Triggering frequency in seconds for BigQuery Storage Write API\" must be"
                + " provided.")
    @Default.Boolean(false)
    Boolean getUseStorageWriteApi();

    void setUseStorageWriteApi(Boolean value);
  }

  public @NonNull String identifier() {
    return "blocks:external:org.apache.beam:write_to_bigquery:v1";
  }

  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));
  private static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();

  @Consumes(RowTypes.SchemaTableRow.class)
  @Outputs(RowTypes.FailsafeStringRow.class)
  public PCollectionRowTuple transform(PCollectionRowTuple input, SinkOptions options) {
    WriteResult writeResult =
        input
            .get(BlockConstants.OUTPUT_TAG)
            .apply(
                MapElements.into(TypeDescriptor.of(TableRow.class))
                    .via(RowTypes.SchemaTableRow::RowToTableRow))
            .apply(
                "WriteTableRows",
                BigQueryIO.writeTableRows()
                    .withoutValidation()
                    .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                    .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                    .withExtendedErrorInfo()
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                    .to(options.getOutputTableSpec()));

    BigQueryOptions bigQueryOptions = input.getPipeline().getOptions().as(BigQueryOptions.class);
    bigQueryOptions.setUseStorageWriteApi(bigQueryOptions.getUseStorageWriteApi());
    PCollection<FailsafeElement<String, String>> failedInserts =
        BigQueryIOUtils.writeResultToBigQueryInsertErrors(writeResult, bigQueryOptions)
            .apply(
                "WrapInsertionErrors",
                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                    .via((BigQueryInsertError e) -> wrapBigQueryInsertError(e)))
            .setCoder(FAILSAFE_ELEMENT_CODER);

    PCollection<Row> errors =
        failedInserts
            .apply(
                MapElements.into(TypeDescriptor.of(Row.class))
                    .via(RowTypes.FailsafeStringRow::FailsafeStringToRow))
            .setCoder(RowCoder.of(RowTypes.FailsafeStringRow.SCHEMA));

    PCollectionRowTuple output = PCollectionRowTuple.of(BlockConstants.ERROR_TAG, errors);

    return output;
  }

  //  @Consumes(GenericRecord.class)
  //  public void writeGenericRecords(
  //      PCollection<GenericRecord> input, WriteToBigQueryTransformConfiguration options) {
  //    WriteResult writeResult =
  //        input.apply(
  //            "WriteTableRows",
  //            BigQueryIO.writeGenericRecords()
  //                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
  //                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
  //                .withExtendedErrorInfo()
  //                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
  //                .to(options.getOutputTableSpec()));
  //
  //        handleFailures(writeResult, options);
  //  }

  //  @Consumes(String.class)
  //  @Outputs(
  //      value = FailsafeElement.class,
  //      types = {String.class, String.class})
  //  public void writeJson(PCollection<String> input, WriteToBigQueryTransformConfiguration
  // options) {
  //    WriteResult writeResult =
  //        input.apply(
  //            "WriteTableRows",
  //            BigQueryIO.<String>write()
  //                .to(options.getOutputTableSpec())
  //                .withWriteDisposition(WriteDisposition.valueOf(options.getWriteDisposition()))
  //
  // .withCreateDisposition(CreateDisposition.valueOf(options.getCreateDisposition()))
  //                .withExtendedErrorInfo()
  //                .withFormatFunction(BigQueryConverters::convertJsonToTableRow)
  //                .withJsonSchema(getGcsFileAsString(options.getBigQuerySchemaPath())));
  //
  //    // handleFailures(writeResult, options);
  //  }

  //  private void handleFailures(
  //      WriteResult writeResult, WriteToBigQueryTransformConfiguration options) {
  //    PCollection<FailsafeElement<String, String>> failedInserts =
  //        BigQueryIOUtils.writeResultToBigQueryInsertErrors(writeResult, options)
  //            .apply(
  //                "WrapInsertionErrors",
  //                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
  //                    .via((BigQueryInsertError e) -> wrapBigQueryInsertError(e)))
  //            .setCoder(FAILSAFE_ELEMENT_CODER);
  //
  //    String dlqTable =
  //        StringUtils.isEmpty(options.getOutputDeadletterTable())
  //            ? options.getOutputTableSpec() + BlockConstants.DEFAULT_DEADLETTER_TABLE_SUFFIX
  //            : options.getOutputDeadletterTable();
  //
  //    AutoDLQUtil.writeDLQToBigQueryForString(failedInserts, dlqTable);
  //  }

  static FailsafeElement<String, String> wrapBigQueryInsertError(BigQueryInsertError insertError) {

    FailsafeElement<String, String> failsafeElement;
    try {

      String rowPayload = JSON_FACTORY.toString(insertError.getRow());
      String errorMessage = JSON_FACTORY.toString(insertError.getError());

      failsafeElement = FailsafeElement.of(rowPayload, rowPayload);
      failsafeElement.setErrorMessage(errorMessage);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return failsafeElement;
  }

  @Override
  public Class<SinkOptions> getOptionsClass() {
    return SinkOptions.class;
  }
}
