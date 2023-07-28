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

import static com.google.cloud.teleport.v2.utils.GCSUtils.getGcsFileAsString;

import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.auto.Consumes;
import com.google.cloud.teleport.metadata.auto.Outputs;
import com.google.cloud.teleport.metadata.auto.TemplateTransform;
import com.google.cloud.teleport.v2.auto.blocks.WriteToBigQuery.SinkOptions;
import com.google.cloud.teleport.v2.auto.dlq.AutoDLQUtil;
import com.google.cloud.teleport.v2.auto.dlq.BigQueryDeadletterOptions;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.options.BigQueryCommonOptions.WriteOptions;
import com.google.cloud.teleport.v2.options.BigQueryStorageApiBatchOptions;
import com.google.cloud.teleport.v2.options.BigQueryStorageApiStreamingOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.commons.lang3.StringUtils;

public class WriteToBigQuery implements TemplateTransform<SinkOptions> {

  public interface SinkOptions
      extends PipelineOptions,
          WriteOptions,
          BigQueryDeadletterOptions,
          BigQueryStorageApiStreamingOptions,
          BigQueryStorageApiBatchOptions {

    @TemplateParameter.BigQueryTable(
        order = 1,
        optional = true,
        description = "BigQuery output table",
        helpText =
            "BigQuery table location to write the output to. The table's schema must match the "
                + "input JSON objects.")
    String getOutputTableSpec();

    void setOutputTableSpec(String input);

    @TemplateParameter.Text(
        order = 2,
        optional = true,
        description = "GCS Path to JSON file containing BigQuery table schema.",
        helpText = "sample text")
    String getBigQuerySchemaPath();

    void setBigQuerySchemaPath(String bigQuerySchemaPath);
  }

  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));
  private static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();

  @Consumes(TableRow.class)
  @Outputs(
      value = FailsafeElement.class,
      types = {String.class, String.class})
  public PCollectionTuple writeTableRows(PCollectionTuple input, SinkOptions options) {
    WriteResult writeResult =
        input
            .get(BlockConstants.OUTPUT_TAG)
            .apply(
                "WriteTableRows",
                BigQueryIO.writeTableRows()
                    .withoutValidation()
                    .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                    .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                    .withExtendedErrorInfo()
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                    .to(options.getOutputTableSpec()));

    PCollection<FailsafeElement<String, String>> failedInserts =
        BigQueryIOUtils.writeResultToBigQueryInsertErrors(writeResult, options)
            .apply(
                "WrapInsertionErrors",
                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                    .via((BigQueryInsertError e) -> wrapBigQueryInsertError(e)))
            .setCoder(FAILSAFE_ELEMENT_CODER);

    return PCollectionTuple.of(BlockConstants.ERROR_TAG_STR, failedInserts);
    // handleFailures(writeResult, options);
  }

  @Consumes(GenericRecord.class)
  public void writeGenericRecords(PCollection<GenericRecord> input, SinkOptions options) {
    WriteResult writeResult =
        input.apply(
            "WriteTableRows",
            BigQueryIO.writeGenericRecords()
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withExtendedErrorInfo()
                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                .to(options.getOutputTableSpec()));

    handleFailures(writeResult, options);
  }

  @Consumes(String.class)
  @Outputs(
      value = FailsafeElement.class,
      types = {String.class, String.class})
  public void writeJson(PCollection<String> input, SinkOptions options) {
    WriteResult writeResult =
        input.apply(
            "WriteTableRows",
            BigQueryIO.<String>write()
                .to(options.getOutputTableSpec())
                .withWriteDisposition(WriteDisposition.valueOf(options.getWriteDisposition()))
                .withCreateDisposition(CreateDisposition.valueOf(options.getCreateDisposition()))
                .withExtendedErrorInfo()
                .withFormatFunction(BigQueryConverters::convertJsonToTableRow)
                .withJsonSchema(getGcsFileAsString(options.getBigQuerySchemaPath())));

    handleFailures(writeResult, options);
  }

  private void handleFailures(WriteResult writeResult, SinkOptions options) {
    PCollection<FailsafeElement<String, String>> failedInserts =
        BigQueryIOUtils.writeResultToBigQueryInsertErrors(writeResult, options)
            .apply(
                "WrapInsertionErrors",
                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                    .via((BigQueryInsertError e) -> wrapBigQueryInsertError(e)))
            .setCoder(FAILSAFE_ELEMENT_CODER);

    String dlqTable =
        StringUtils.isEmpty(options.getOutputDeadletterTable())
            ? options.getOutputTableSpec() + BlockConstants.DEFAULT_DEADLETTER_TABLE_SUFFIX
            : options.getOutputDeadletterTable();

    AutoDLQUtil.writeDLQToBigQueryForString(failedInserts, dlqTable);
  }

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
