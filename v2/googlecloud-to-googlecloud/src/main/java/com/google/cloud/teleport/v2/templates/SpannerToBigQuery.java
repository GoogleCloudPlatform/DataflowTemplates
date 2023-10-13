/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.utils.GCSUtils.getGcsFileAsString;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_NEVER;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.SpannerToBigQueryOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.transforms.SpannerToBigQueryTransform.StructToJson;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/** Template to read data from a Spanner table and write into a BigQuery table. */
@Template(
    name = "Cloud_Spanner_to_BigQuery_Flex",
    category = TemplateCategory.BATCH,
    displayName = "Spanner to BigQuery",
    description =
        "The Spanner to BigQuery template is a batch pipeline that reads data from a Spanner table, and writes them to a BigQuery table.",
    optionsClass = SpannerToBigQueryOptions.class,
    flexContainerName = "spanner-to-bigquery",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/spanner-to-bigquery",
    contactInformation = "https://cloud.google.com/support")
public final class SpannerToBigQuery {

  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    PipelineOptionsFactory.register(SpannerToBigQueryOptions.class);
    SpannerToBigQueryOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SpannerToBigQueryOptions.class);

    BigQueryIOUtils.validateBQStorageApiOptionsBatch(options);

    Pipeline pipeline = Pipeline.create(options);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(
                options.getSpannerProjectId().isEmpty()
                    ? options.getProject()
                    : options.getSpannerProjectId())
            .withDatabaseId(options.getSpannerDatabaseId())
            .withInstanceId(options.getSpannerInstanceId())
            .withRpcPriority(options.getSpannerRpcPriority());

    pipeline
        .apply(
            SpannerIO.read()
                .withTable(options.getSpannerTableId())
                .withSpannerConfig(spannerConfig)
                .withQuery(options.getSqlQuery()))
        .apply(new StructToJson())
        .apply("Write To BigQuery", writeToBigQuery(options));

    pipeline.run();
  }

  private static Write<String> writeToBigQuery(SpannerToBigQueryOptions options) {
    if (CreateDisposition.valueOf(options.getCreateDisposition()) == CREATE_NEVER) {
      return BigQueryIO.<String>write()
          .to(options.getOutputTableSpec())
          .withWriteDisposition(WriteDisposition.valueOf(options.getWriteDisposition()))
          .withCreateDisposition(CreateDisposition.valueOf(options.getCreateDisposition()))
          .withExtendedErrorInfo()
          .withFormatFunction(BigQueryConverters::convertJsonToTableRow);
    }
    return BigQueryIO.<String>write()
        .to(options.getOutputTableSpec())
        .withWriteDisposition(WriteDisposition.valueOf(options.getWriteDisposition()))
        .withCreateDisposition(CreateDisposition.valueOf(options.getCreateDisposition()))
        .withExtendedErrorInfo()
        .withFormatFunction(BigQueryConverters::convertJsonToTableRow)
        .withJsonSchema(getGcsFileAsString(options.getBigQuerySchemaPath()));
  }
}
