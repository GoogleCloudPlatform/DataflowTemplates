/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates;

import static com.google.cloud.teleport.templates.TextToBigQueryStreaming.wrapBigQueryInsertError;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.templates.common.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.templates.common.ErrorConverters;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.util.DualInputNestedValueProvider;
import com.google.cloud.teleport.util.DualInputNestedValueProvider.TranslatorInput;
import com.google.cloud.teleport.util.ResourceUtils;
import com.google.cloud.teleport.util.ValueProviderUtils;
import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import com.google.cloud.teleport.templates.common.KinesisConverters.KinesisStreamOptions;
import com.google.cloud.teleport.templates.common.KinesisConverters.KinesisPartitionKeyOptions;
import com.google.cloud.teleport.templates.common.KinesisConverters.KinesisAccessKeyOptions;
import com.google.cloud.teleport.templates.common.KinesisConverters.KinesisSecretKeyOptions;
import com.google.cloud.teleport.templates.common.KinesisConverters.KinesisRegionOptions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.apache.beam.sdk.io.kinesis.KinesisIO;
import org.apache.beam.sdk.io.kinesis.KinesisRecord;
import com.amazonaws.regions.Regions;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class KinesisToBigQuery {

  /** The log to output status messages to. */
  private static final Logger LOG = LoggerFactory.getLogger(PubSubToBigQuery.class);

  /**
   * The {@link Options} class provides the custom execution options passed by the executor at the
   * command-line.
   */
  public interface KinesisOptions extends KinesisStreamOptions, KinesisPartitionKeyOptions, KinesisAccessKeyOptions, KinesisSecretKeyOptions, KinesisRegionOptions {}

  public interface Options extends PipelineOptions, JavascriptTextTransformerOptions, KinesisOptions {
    @Description("Table spec to write the output to")
    ValueProvider<String> getOutputTableSpec();

    void setOutputTableSpec(ValueProvider<String> value);

    @Description(
        "The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
            + "format. If it doesn't exist, it will be created during pipeline execution.")
    ValueProvider<String> getOutputDeadletterTable();

    void setOutputDeadletterTable(ValueProvider<String> value);
  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * PubSubToBigQuery#run(Options)} method to start the pipeline and invoke {@code
   * result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options. This method does not wait until the
   * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
   * object to block until the pipeline is finished running if blocking programmatic execution is
   * required.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(Options options) {

    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply(
      "Read Kinesis Events",
      KinesisIO.read()
      .withStreamName(options.getAwsKinesisStream().toString())
      .withInitialPositionInStream(InitialPositionInStream.LATEST)
      .withAWSClientsProvider(
      options.getAwsAccessKey().toString(),
      options.getAwsSecretKey().toString(),
      Regions.fromName(options.getAwsKinesisRegion().toString())
    ))
    .apply(
      "ConvertKinesisRecordToTableRow",
      ParDo.of(new KinesisRecordToTableRow()))
    .apply(
      "WriteSuccessfulRecords",
      BigQueryIO.writeTableRows()
      .withoutValidation()
      .withCreateDisposition(CreateDisposition.CREATE_NEVER)
      .withWriteDisposition(WriteDisposition.WRITE_APPEND)
      .withExtendedErrorInfo()
      .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
      .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
      .to(options.getOutputTableSpec()));
    return pipeline.run();
  }

  static class KinesisRecordToTableRow extends DoFn<KinesisRecord, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      KinesisRecord message = context.element();
      String json = new String(message.getDataAsBytes(), StandardCharsets.UTF_8);
      TableRow row = convertJsonToTableRow(json);
      context.output(row);
    }
  }

  public static TableRow convertJsonToTableRow(String json) {
    TableRow row;
    try (InputStream inputStream =
        new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
      row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize json to table row: " + json, e);
    }
    return row;
  }
}
