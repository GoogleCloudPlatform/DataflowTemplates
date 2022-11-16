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
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.mongodb.options.BigQueryToMongoDbOptions.BigQueryReadOptions;
import com.google.cloud.teleport.v2.mongodb.options.BigQueryToMongoDbOptions.MongoDbOptions;
import com.google.cloud.teleport.v2.mongodb.templates.BigQueryToMongoDb.Options;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.bson.Document;

/**
 * The {@link BigQueryToMongoDb} pipeline is a batch pipeline which reads data from BigQuery and
 * outputs the resulting records to MongoDB.
 */
@Template(
    name = "BigQuery_to_MongoDB",
    category = TemplateCategory.BATCH,
    displayName = "BigQuery to MongoDB",
    description =
        "A batch pipeline which reads data rows from BigQuery and writes them to MongoDB as documents.",
    optionsClass = Options.class,
    flexContainerName = "bigquery-to-mongodb",
    contactInformation = "https://cloud.google.com/support")
public class BigQueryToMongoDb {
  /**
   * Options supported by {@link BigQueryToMongoDb}
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, MongoDbOptions, BigQueryReadOptions {}

  private static class ParseAsDocumentsFn extends DoFn<String, Document> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(Document.parse(context.element()));
    }
  }

  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    run(options);
  }

  public static boolean run(Options options) {
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(BigQueryIO.readTableRows().withoutValidation().from(options.getInputTableSpec()))
        .apply(
            "bigQueryDataset",
            ParDo.of(
                new DoFn<TableRow, Document>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    Document doc = new Document();
                    TableRow row = c.element();
                    row.forEach(
                        (key, value) -> {
                          if (key != "_id") {
                            doc.append(key, value);
                          }
                        });
                    c.output(doc);
                  }
                }))
        .apply(
            MongoDbIO.write()
                .withUri(options.getMongoDbUri())
                .withDatabase(options.getDatabase())
                .withCollection(options.getCollection()));
    pipeline.run();
    return true;
  }
}
