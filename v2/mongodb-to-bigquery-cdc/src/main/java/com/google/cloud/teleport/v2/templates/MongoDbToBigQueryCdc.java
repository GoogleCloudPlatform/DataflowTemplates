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
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.HashMap;


public class MongoDbToBigQueryCdc {


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
      options.setStreaming(true);

      TableSchema bigquerySchema = MongoDbUtils.getTableFieldSchema(
              options.getMongoDbUri(),
              options.getDatabase(),
              options.getCollection(),
              options.getUserOption()
      );

    String userOption = options.getUserOption();
    String inputOption = options.getInputTopic();

    pipeline
            .apply(
                    "Read PubSub Messages",
                    PubsubIO.
                            readStrings().
                            fromTopic(inputOption))
            .apply(
                    "Read and transform data",
                    MapElements.via(
                            new SimpleFunction<String, TableRow>() {
                                @Override
                                public TableRow apply(String document) {
                                    Gson gson = new GsonBuilder().create();
                                    HashMap<String, Object> parsedMap = gson.fromJson(document, HashMap.class);
                                    return MongoDbUtils.generateTableRow(parsedMap, userOption);
                                }
                            }
                    )
            )
            .apply(
                    BigQueryIO
                            .writeTableRows()
                            .to(options.getOutputTableSpec())
                            .withSchema(bigquerySchema)
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            );
    pipeline.run();
    return true;
  }
}
