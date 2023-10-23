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

import static com.google.cloud.teleport.v2.auto.blocks.StandardCoderConverters.rowToTableRow;
import static com.google.cloud.teleport.v2.utils.GCSUtils.getGcsFileAsString;

import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auto.service.AutoService;
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
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.LoggerFactory;

@AutoService(ExternalTransformRegistrar.class)
public class WriteToBigQuery implements TemplateTransform<SinkOptions>, ExternalTransformRegistrar {

  private static final String URN = "blocks:external:org.apache.beam:write_to_bigquery:v1";

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(WriteToBigQuery.class);

  public String identifier() {
    return URN;
  }

  private static class Builder
      implements ExternalTransformBuilder<
          Configuration, @NonNull PCollectionRowTuple, @NonNull PCollection<Row>> {
    @Override
    public @NonNull PTransform<@NonNull PCollectionRowTuple, @NonNull PCollection<Row>> buildExternal(
        Configuration config) {
      return new PTransform<>() {
        @Override
        public @NonNull PCollection<Row> expand(@NonNull PCollectionRowTuple input) {
          PCollection<TableRow> tableRowPCollection = rowToTableRow(input.get("output"));
          return underlyingTransform(PCollectionTuple.of(BlockConstants.OUTPUT_TAG, tableRowPCollection), config).get("post_write");
        }
      };
    }
  }

  public Map<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> knownBuilders() {
    return ImmutableMap.of(identifier(), Builder.class);
  }

  // TODO(polber) - See if this Config class can be generated programmatically from TransformOptions
  // Interface
  public static class Configuration {

    String outputTableSpec = "";
    Boolean useStorageWriteApi = false;

    public void setOutputTableSpec(String input) {
      this.outputTableSpec = input;
    }

    public String getOutputTableSpec() {
      return this.outputTableSpec;
    }

    public void setUseStorageWriteApi(Boolean input) {
      this.useStorageWriteApi = input;
    }

    public Boolean getUseStorageWriteApi() {
      return this.useStorageWriteApi;
    }

    public static Configuration fromOptions(SinkOptions options) {
      Configuration config = new Configuration();
      config.setOutputTableSpec(options.getOutputTableSpec());
      config.setUseStorageWriteApi(options.getUseStorageWriteApi());
      return config;
    }
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

  @Consumes(TableRow.class)
  @Outputs(
      value = FailsafeElement.class,
      types = {String.class, String.class})
  public PCollectionTuple writeTableRows(PCollectionTuple input, SinkOptions options) {
    return underlyingTransform(input, Configuration.fromOptions(options));
  }

  private static class NoOutputDoFn<T> extends DoFn<T, Row> {
    @ProcessElement
    public void process(ProcessContext c) {}
  }

  public static PCollectionTuple underlyingTransform(PCollectionTuple input, Configuration config) {
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

    PCollection<Row> postWrite =
        writeResult
            .getFailedInsertsWithErr()
            .apply("post-write", ParDo.of(new NoOutputDoFn<>()))
            .setRowSchema(Schema.of());

    return PCollectionTuple.of(BlockConstants.ERROR_TAG_STR, failedInserts)
        .and("post_write", postWrite);
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
