/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.templates.common.SpannerConverters;
import com.google.cloud.teleport.templates.common.SpannerConverters.CreateTransactionFnWithTimestamp;
import com.google.cloud.teleport.templates.common.SpannerConverters.SpannerReadOptions;
import com.google.cloud.teleport.templates.common.TextConverters.FilesystemWriteOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.LocalSpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow template which copies a Spanner table to a Text sink. It exports a Spanner table using
 * <a href="https://cloud.google.com/spanner/docs/reads#read_data_in_parallel">Batch API</a>, which
 * creates multiple workers in parallel for better performance. The result is written to a CSV file
 * in Google Cloud Storage. The table schema file is saved in json format along with the exported
 * table.
 *
 * <p>Schema file sample: { "id":"INT64", "name":"STRING(MAX)" }
 *
 * <p>A sample run:
 *
 * <pre>
 * mvn compile exec:java \
 *   -Dexec.mainClass=com.google.cloud.teleport.templates.SpannerToText \
 *   -Dexec.args="--runner=DataflowRunner \
 *                --spannerProjectId=projectId \
 *                --gcpTempLocation=gs://gsTmpLocation \
 *                --spannerInstanceId=instanceId \
 *                --spannerDatabaseId=databaseId \
 *                --spannerTable=table_name \
 *                --spannerSnapshotTime=snapshot_time \
 *                --textWritePrefix=gcsOutputPath"
 * </pre>
 */
public class SpannerToText {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToText.class);

  /** Custom PipelineOptions. */
  public interface SpannerToTextOptions
      extends PipelineOptions, SpannerReadOptions, FilesystemWriteOptions {}

  /**
   * Runs a pipeline which reads in Records from Spanner, and writes the CSV to TextIO sink.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    LOG.info("Starting pipeline setup");
    PipelineOptionsFactory.register(SpannerToTextOptions.class);
    SpannerToTextOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SpannerToTextOptions.class);

    FileSystems.setDefaultPipelineOptions(options);
    Pipeline pipeline = Pipeline.create(options);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withHost(options.getSpannerHost())
            .withProjectId(options.getSpannerProjectId())
            .withInstanceId(options.getSpannerInstanceId())
            .withDatabaseId(options.getSpannerDatabaseId());

    PTransform<PBegin, PCollection<ReadOperation>> spannerExport =
        SpannerConverters.ExportTransformFactory.create(
            options.getSpannerTable(),
            spannerConfig,
            options.getTextWritePrefix(),
            options.getSpannerSnapshotTime());

    /* CreateTransaction and CreateTransactionFn classes in SpannerIO
     * only take a timestamp object for exact staleness which works when
     * parameters are provided during template compile time. They do not work with
     * a Timestamp valueProvider which can take parameters at runtime. Hence a new
     * ParDo class CreateTransactionFnWithTimestamp had to be created for this
     * purpose.
     */
    PCollectionView<Transaction> tx =
        pipeline
            .apply("Setup for Transaction", Create.of(1))
            .apply(
                "Create transaction",
                ParDo.of(
                    new CreateTransactionFnWithTimestamp(
                        spannerConfig, options.getSpannerSnapshotTime())))
            .apply("As PCollectionView", View.asSingleton());

    PCollection<String> csv =
        pipeline
            .apply("Create export", spannerExport)
            // We need to use SpannerIO.readAll() instead of SpannerIO.read()
            // because ValueProvider parameters such as table name required for SpannerIO.read()
            // can be read only inside DoFn but SpannerIO.read() is of type
            // PTransform<PBegin, Struct>, which prevents prepending it with DoFn that reads these
            // parameters at the pipeline execution time.
            .apply(
                "Read all records",
                LocalSpannerIO.readAll().withTransaction(tx).withSpannerConfig(spannerConfig))
            .apply(
                "Struct To Csv",
                MapElements.into(TypeDescriptors.strings())
                    .via(struct -> (new SpannerConverters.StructCsvPrinter()).print(struct)));

    csv.apply(
        "Write to storage", TextIO.write().to(options.getTextWritePrefix()).withSuffix(".csv"));

    pipeline.run();
    LOG.info("Completed pipeline setup");
  }
}
