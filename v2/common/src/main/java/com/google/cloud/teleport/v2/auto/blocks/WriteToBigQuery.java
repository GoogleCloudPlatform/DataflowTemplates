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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.auto.Consumes;
import com.google.cloud.teleport.metadata.auto.Outputs;
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
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;
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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;

@AutoService(SchemaTransformProvider.class)
public class WriteToBigQuery
    extends TemplateWriteTransform<
        SinkOptions, WriteToBigQuery.WriteToBigQueryTransformConfiguration> {

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class WriteToBigQueryTransformConfiguration extends Configuration {

    @SchemaFieldDescription(
        "BigQuery table location to write the output to. The table's schema must match the "
            + "input JSON objects.")
    @Nullable
    abstract String getOutputTableSpec();

    @SchemaFieldDescription("Enable BigQuery StorageWriteAPI.")
    abstract Boolean getUseStorageWriteApi();

    public void validate() {
      String invalidConfigMessage = "Invalid BigQuery Storage Write configuration: ";
      if (this.getErrorHandling() != null) {
        checkArgument(
            !Strings.isNullOrEmpty(this.getErrorHandling().getOutput()),
            invalidConfigMessage + "Output must not be empty if error handling specified.");
      }
    }

    public static WriteToBigQueryTransformConfiguration.Builder builder() {
      return new AutoValue_WriteToBigQuery_WriteToBigQueryTransformConfiguration.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder extends Configuration.Builder<Builder> {
      public abstract WriteToBigQueryTransformConfiguration.Builder setOutputTableSpec(
          String input);

      public abstract WriteToBigQueryTransformConfiguration.Builder setUseStorageWriteApi(
          Boolean input);

      public abstract WriteToBigQueryTransformConfiguration build();
    }

    public static WriteToBigQueryTransformConfiguration fromOptions(SinkOptions options) {
      return new AutoValue_WriteToBigQuery_WriteToBigQueryTransformConfiguration.Builder()
          .setOutputTableSpec(options.getOutputTableSpec())
          .setUseStorageWriteApi(options.getUseStorageWriteApi())
          .build();
    }
  }

  @Override
  public @NonNull Class<WriteToBigQueryTransformConfiguration> configurationClass() {
    return WriteToBigQueryTransformConfiguration.class;
  }

  public @NonNull String identifier() {
    return "blocks:external:org.apache.beam:write_to_bigquery:v1";
  }

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

  @Consumes(
      value = Row.class,
      types = {RowTypes.SchemaTableRow.class})
  @Outputs(
      value = Row.class,
      types = {RowTypes.FailsafeStringRow.class})
  public PCollectionRowTuple writeTableRows(PCollectionRowTuple input, SinkOptions options) {
    return transform(input, WriteToBigQueryTransformConfiguration.fromOptions(options));
  }

  public PCollectionRowTuple transform(
      PCollectionRowTuple input, WriteToBigQueryTransformConfiguration config) {
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
                    .to(config.getOutputTableSpec()));

    BigQueryOptions options = input.getPipeline().getOptions().as(BigQueryOptions.class);
    options.setUseStorageWriteApi(config.getUseStorageWriteApi());
    PCollection<FailsafeElement<String, String>> failedInserts =
        BigQueryIOUtils.writeResultToBigQueryInsertErrors(writeResult, options)
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
