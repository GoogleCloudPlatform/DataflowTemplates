/*
 * Copyright (C) 2020 Google LLC
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

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.BigQueryCommonOptions.WriteOptions;
import com.google.cloud.teleport.v2.options.BigQueryStorageApiStreamingOptions;
import com.google.cloud.teleport.v2.options.PubsubCommonOptions.ReadSubscriptionOptions;
import com.google.cloud.teleport.v2.options.PubsubCommonOptions.WriteTopicOptions;
import com.google.cloud.teleport.v2.templates.PubsubAvroToBigQuery.PubsubAvroToBigQueryOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.values.Row;

/**
 * A Dataflow pipeline to stream <a href="https://avro.apache.org/">Apache Avro</a> records from
 * Pub/Sub into a BigQuery table.
 *
 * <p>Any persistent failures while writing to BigQuery will be written to a Pub/Sub dead-letter
 * topic.
 */
@Template(
    name = "PubSub_Avro_to_BigQuery",
    category = TemplateCategory.STREAMING,
    displayName = "Pub/Sub Avro to BigQuery",
    description =
        "A streaming pipeline which inserts Avro records from a Pub/Sub subscription into a"
            + " BigQuery table.",
    optionsClass = PubsubAvroToBigQueryOptions.class,
    flexContainerName = "pubsub-avro-to-bigquery",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-avro-to-bigquery",
    contactInformation = "https://cloud.google.com/support")
public final class PubsubAvroToBigQuery {
  /**
   * Validates input flags and executes the Dataflow pipeline.
   *
   * @param args command line arguments to the pipeline
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    PubsubAvroToBigQueryOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(PubsubAvroToBigQueryOptions.class);

    run(options);
  }

  /**
   * Provides custom {@link org.apache.beam.sdk.options.PipelineOptions} required to execute the
   * {@link PubsubAvroToBigQuery} pipeline.
   */
  public interface PubsubAvroToBigQueryOptions
      extends ReadSubscriptionOptions,
          WriteOptions,
          WriteTopicOptions,
          BigQueryStorageApiStreamingOptions {

    @TemplateParameter.GcsReadFile(
        order = 1,
        description = "Cloud Storage path to the Avro schema file",
        helpText = "Cloud Storage path to Avro schema file. For example, gs://MyBucket/file.avsc.")
    @Required
    String getSchemaPath();

    void setSchemaPath(String schemaPath);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options execution parameters to the pipeline
   * @return result of the pipeline execution as a {@link PipelineResult}
   */
  private static PipelineResult run(PubsubAvroToBigQueryOptions options) {
    BigQueryIOUtils.validateBQStorageApiOptionsStreaming(options);

    // Create the pipeline.
    Pipeline pipeline = Pipeline.create(options);

    Schema schema = SchemaUtils.getAvroSchema(options.getSchemaPath());

    WriteResult writeResults =
        pipeline
            .apply(
                "Read Avro records",
                PubsubIO.readAvroGenericRecords(schema)
                    .fromSubscription(options.getInputSubscription())
                    .withDeadLetterTopic(options.getOutputTopic()))
            // Workaround for BEAM-12256. Eagerly convert to rows to avoid
            // the RowToGenericRecord function that doesn't handle all data
            // types.
            // TODO: Remove this workaround when a fix for BEAM-12256 is
            // released.
            .apply(Convert.toRows())
            .apply(
                "Write to BigQuery",
                BigQueryConverters.<Row>createWriteTransform(options).useBeamSchema());

    BigQueryIOUtils.writeResultToBigQueryInsertErrors(writeResults, options)
        .apply(
            "Create error payload",
            ErrorConverters.BigQueryInsertErrorToPubsubMessage.<GenericRecord>newBuilder()
                .setPayloadCoder(AvroCoder.of(schema))
                .setTranslateFunction(BigQueryConverters.TableRowToGenericRecordFn.of(schema))
                .build())
        .apply("Write failed records", PubsubIO.writeMessages().to(options.getOutputTopic()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}
