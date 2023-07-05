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

/** Template to write data from Spanner table into a BigQuery table. */
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
    return BigQueryIO.<String>write()
        .to(options.getOutputTableSpec())
        .withWriteDisposition(WriteDisposition.valueOf(options.getWriteDisposition()))
        .withCreateDisposition(CreateDisposition.valueOf(options.getCreateDisposition()))
        .withExtendedErrorInfo()
        .withFormatFunction(BigQueryConverters::convertJsonToTableRow)
        .withJsonSchema(getGcsFileAsString(options.getBigQuerySchemaPath()));
  }
}
