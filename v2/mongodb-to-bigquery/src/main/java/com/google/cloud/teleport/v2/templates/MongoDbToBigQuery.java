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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.templates.MongoDbUtils;
import com.google.cloud.teleport.v2.templates.MongoDbUtils.BigQueryWriteOptions;
import com.google.cloud.teleport.v2.templates.MongoDbUtils.MongoDbOptions;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.bson.Document;
import org.apache.beam.sdk.transforms.SimpleFunction;
import com.google.api.services.bigquery.model.TableFieldSchema;


/**
 * The {@link MongoDbToBigQuery} pipeline is a streaming pipeline which ingests data in JSON format
 * from PubSub, applies a Javascript UDF if provided and inserts resulting records as Bson Document
 * in MongoDB. If the element fails to be processed then it is written to a deadletter table in
 * BigQuery.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The PubSub topic and subscriptions exist
 *   <li>The MongoDB is up and running
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_NAME=my-project
 * BUCKET_NAME=my-bucket
 * INPUT_SUBSCRIPTION=my-subscription
 * MONGODB_DATABASE_NAME=testdb
 * MONGODB_HOSTNAME=my-host:port
 * MONGODB_COLLECTION_NAME=testCollection
 * DEADLETTERTABLE=project:dataset.deadletter_table_name
 *
 * mvn compile exec:java \
 *  -Dexec.mainClass=com.google.cloud.teleport.v2.templates.PubSubToMongoDB \
 *  -Dexec.cleanupDaemonThreads=false \
 *  -Dexec.args=" \
 *  --project=${PROJECT_NAME} \
 *  --stagingLocation=gs://${BUCKET_NAME}/staging \
 *  --tempLocation=gs://${BUCKET_NAME}/temp \
 *  --runner=DataflowRunner \
 *  --inputSubscription=${INPUT_SUBSCRIPTION} \
 *  --mongoDBUri=${MONGODB_HOSTNAME} \
 *  --database=${MONGODB_DATABASE_NAME} \
 *  --collection=${MONGODB_COLLECTION_NAME} \
 *  --deadletterTable=${DEADLETTERTABLE}"
 * </pre>
 */
public class MongoDbToBigQuery {
  /**
   * Options supported by {@link PubSubToMongoDB}
   *
   * <p>Inherits standard configuration options.
   */


  public interface Options
      extends PipelineOptions,MongoDbOptions, BigQueryWriteOptions  {
  }

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


    TableSchema bigquerySchema = MongoDbUtils.getTableFieldSchema(
            options.getMongoDbUri(),
            options.getDatabase(),
            options.getCollection(),
            options.getUserOption()
    );
    String userOption = options.getUserOption();

    pipeline
            .apply(
                "Read Documents",
                MongoDbIO.read().
                        withBucketAuto(true).
                        withUri(options.getMongoDbUri()).
                        withDatabase(options.getDatabase()).
                        withCollection(options.getCollection()))
            .apply(
                    "Transform to TableRow", MapElements.via(
                            new SimpleFunction<Document, TableRow>() {
                              @Override
                              public TableRow apply(Document document) {
                                return MongoDbUtils.generateTableRow(document, userOption);
                              }
                            }
                    )
            )
            .apply(
                    "Write to Bigquery",
                    BigQueryIO.writeTableRows()
                            .to(options.getOutputTableSpec())
                            .withSchema(bigquerySchema)
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            );
    pipeline.run();
    return true;
  }



}
