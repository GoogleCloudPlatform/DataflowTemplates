/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.v2.mongodb.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.BigQueryWriteOptions;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.JavascriptDocumentTransformerOptions;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.MongoDbOptions;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.PubSubOptions;
import com.google.cloud.teleport.v2.mongodb.templates.MongoDbToBigQueryCdc.Options;
import com.google.cloud.teleport.v2.options.BigQueryStorageApiStreamingOptions;
import com.google.cloud.teleport.v2.transforms.JavascriptDocumentTransformer.TransformDocumentViaJavascript;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import java.io.IOException;
import javax.script.ScriptException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link MongoDbToBigQueryCdc} pipeline is a streaming pipeline which reads data pushed to
 * PubSub from MongoDB Changestream and outputs the resulting records to BigQuery.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/mongodb-to-googlecloud/README_MongoDB_to_BigQuery_CDC.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "MongoDB_to_BigQuery_CDC",
    category = TemplateCategory.STREAMING,
    displayName = "MongoDB to BigQuery (CDC)",
    description =
        "A streaming pipeline which reads data pushed to Pub/Sub from MongoDB Changestream and"
            + " writes the resulting records to BigQuery.",
    optionsClass = Options.class,
    flexContainerName = "mongodb-to-bigquery-cdc",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/mongodb-change-stream-to-bigquery",
    contactInformation = "https://cloud.google.com/support")
public class MongoDbToBigQueryCdc {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbToBigQuery.class);

  /** Options interface. */
  public interface Options
      extends PipelineOptions,
          MongoDbOptions,
          PubSubOptions,
          BigQueryWriteOptions,
          JavascriptDocumentTransformerOptions,
          BigQueryStorageApiStreamingOptions {}

  /** class ParseAsDocumentsFn. */
  private static class ParseAsDocumentsFn extends DoFn<String, Document> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(Document.parse(context.element()));
    }
  }

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args)
      throws ScriptException, IOException, NoSuchMethodException {
    UncaughtExceptionLogger.register();

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    BigQueryIOUtils.validateBQStorageApiOptionsStreaming(options);
    run(options);
  }

  /** Pipeline to read data from PubSub and write to MongoDB. */
  public static boolean run(Options options)
      throws ScriptException, IOException, NoSuchMethodException {
    options.setStreaming(true);
    Pipeline pipeline = Pipeline.create(options);
    String userOption = options.getUserOption();
    String inputOption = options.getInputTopic();

    TableSchema bigquerySchema;

    if (options.getJavascriptDocumentTransformFunctionName() != null
        && options.getJavascriptDocumentTransformGcsPath() != null) {
      bigquerySchema =
          MongoDbUtils.getTableFieldSchemaForUDF(
              options.getMongoDbUri(),
              options.getDatabase(),
              options.getCollection(),
              options.getJavascriptDocumentTransformGcsPath(),
              options.getJavascriptDocumentTransformFunctionName(),
              options.getUserOption());
    } else {
      bigquerySchema =
          MongoDbUtils.getTableFieldSchema(
              options.getMongoDbUri(),
              options.getDatabase(),
              options.getCollection(),
              options.getUserOption());
    }

    pipeline
        .apply("Read PubSub Messages", PubsubIO.readStrings().fromTopic(inputOption))
        .apply(
            "Transform string to document",
            ParDo.of(
                new DoFn<String, Document>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    Document document = Document.parse(c.element());
                    c.output(document);
                  }
                }))
        .apply(
            "UDF",
            TransformDocumentViaJavascript.newBuilder()
                .setFileSystemPath(options.getJavascriptDocumentTransformGcsPath())
                .setFunctionName(options.getJavascriptDocumentTransformFunctionName())
                .build())
        .apply(
            "Read and transform data",
            ParDo.of(
                new DoFn<Document, TableRow>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    Document document = c.element();
                    TableRow row = MongoDbUtils.getTableSchema(document, userOption);
                    c.output(row);
                  }
                }))
        .apply(
            BigQueryIO.writeTableRows()
                .to(options.getOutputTableSpec())
                .withSchema(bigquerySchema)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    pipeline.run();
    return true;
  }
}
