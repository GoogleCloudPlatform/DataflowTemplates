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
package com.google.cloud.teleport.v2.elasticsearch.templates;


import com.google.cloud.teleport.v2.elasticsearch.options.SpannerToElasticsearchOptions;
import com.google.cloud.teleport.v2.elasticsearch.transforms.WriteToElasticsearch;
import com.google.cloud.teleport.v2.transforms.SpannerToJsonTransform.StructToJson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/** Template to write data from Spanner table into BigQuery table. */
public final class SpannerToElasticsearch {

  public static void main(String[] args) {
    PipelineOptionsFactory.register(SpannerToElasticsearchOptions.class);
    SpannerToElasticsearchOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SpannerToElasticsearchOptions.class);
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
        .apply(
            "WriteToElasticsearch",
            WriteToElasticsearch.newBuilder()
                .setOptions(options.as(BigQueryToElasticsearchOptions.class))
                .build());

    pipeline.run();
  }
}

