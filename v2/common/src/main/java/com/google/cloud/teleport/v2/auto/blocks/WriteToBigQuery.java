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
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.auto.Consumes;
import com.google.cloud.teleport.metadata.auto.TemplateSink;
import com.google.cloud.teleport.v2.auto.blocks.WriteToBigQuery.SinkOptions;
import com.google.cloud.teleport.v2.auto.dlq.AutoDLQUtil;
import com.google.cloud.teleport.v2.auto.dlq.BigQueryDeadletterOptions;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.options.BigQueryStorageApiStreamingOptions;
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
import org.apache.commons.lang3.StringUtils;

public class WriteToBigQuery implements TemplateSink<SinkOptions> {

  public interface SinkOptions
      extends PipelineOptions, BigQueryDeadletterOptions, BigQueryStorageApiStreamingOptions {

    @TemplateParameter.BigQueryTable(
        order = 1,
        description = "BigQuery output table",
        helpText =
            "BigQuery table location to write the output to. The table’s schema must match the "
                + "input JSON objects.")
    String getOutputTableSpec();

    void setOutputTableSpec(String input);
  }

  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));
  private static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();

  @Consumes(TableRow.class)
  public void writeTableRows(PCollection<TableRow> input, SinkOptions options) {
    WriteResult writeResult =
        input.apply(
            "WriteTableRows",
            BigQueryIO.writeTableRows()
                .withoutValidation()
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withExtendedErrorInfo()
                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                .to(options.getOutputTableSpec()));

    handleFailures(writeResult, options);
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
