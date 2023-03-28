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
import com.google.cloud.teleport.v2.mongodb.templates.MongoDbToBigQuery.Options;
import com.google.cloud.teleport.v2.options.BigQueryStorageApiBatchOptions;
import com.google.cloud.teleport.v2.transforms.JavascriptDocumentTransformer.TransformDocumentViaJavascript;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import java.io.IOException;
import javax.script.ScriptException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.bson.Document;

/**
 * The {@link MongoDbToBigQuery} pipeline is a batch pipeline which ingests data from MongoDB and
 * outputs the resulting records to BigQuery.
 */
@Template(
    name = "MongoDB_to_BigQuery",
    category = TemplateCategory.BATCH,
    displayName = "MongoDB to BigQuery",
    description =
        "A batch pipeline which reads data documents from MongoDB and writes them to BigQuery.",
    optionsClass = Options.class,
    flexContainerName = "mongodb-to-bigquery",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/mongodb-to-bigquery",
    contactInformation = "https://cloud.google.com/support")
public class MongoDbToBigQuery {
  /**
   * Options supported by {@link MongoDbToBigQuery}
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options
      extends PipelineOptions,
          MongoDbOptions,
          BigQueryWriteOptions,
          BigQueryStorageApiBatchOptions,
          JavascriptDocumentTransformerOptions {}

  private static class ParseAsDocumentsFn extends DoFn<String, Document> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(Document.parse(context.element()));
    }
  }

  public static void main(String[] args)
      throws ScriptException, IOException, NoSuchMethodException {
    UncaughtExceptionLogger.register();

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    BigQueryIOUtils.validateBQStorageApiOptionsBatch(options);

    run(options);
  }

  public static boolean run(Options options)
      throws ScriptException, IOException, NoSuchMethodException {
    Pipeline pipeline = Pipeline.create(options);
    String userOption = options.getUserOption();

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
        .apply(
            "Read Documents",
            MongoDbIO.read()
                .withUri(options.getMongoDbUri())
                .withDatabase(options.getDatabase())
                .withCollection(options.getCollection()))
        .apply(
            "UDF",
            TransformDocumentViaJavascript.newBuilder()
                .setFileSystemPath(options.getJavascriptDocumentTransformGcsPath())
                .setFunctionName(options.getJavascriptDocumentTransformFunctionName())
                .build())
        .apply(
            "Transform to TableRow",
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
            "Write to Bigquery",
            BigQueryIO.writeTableRows()
                .to(options.getOutputTableSpec())
                .withSchema(bigquerySchema)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    pipeline.run();
    return true;
  }
}
