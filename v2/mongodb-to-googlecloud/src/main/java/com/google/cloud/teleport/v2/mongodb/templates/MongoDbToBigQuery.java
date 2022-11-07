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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.BigQueryWriteOptions;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.JavascriptDocumentTransformerOptions;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.MongoDbOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.FailsafeJsonToTableRow;
//import com.google.cloud.teleport.v2.mongodb.templates.JavascriptDocumentTransformer.JavascriptDocumentTransformerOptions;
import com.google.cloud.teleport.v2.mongodb.templates.JavascriptDocumentTransformer.TransformDocumentViaJavascript;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import javax.script.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CharStreams;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * The {@link BigQueryToMongoDb} pipeline is a batch pipeline which ingests data from MongoDB and
 * outputs the resulting records to BigQuery.
 */
public class MongoDbToBigQuery {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbToBigQuery.class);
  /**
   * Options supported by {@link MongoDbToBigQuery}
   *
   * <p>Inherits standard configuration options.
   */
  /** The tag for the dead-letter output of the udf. */
  public static final TupleTag<FailsafeElement<Document, String>> UDF_DEADLETTER_OUT =
          new TupleTag<FailsafeElement<Document, String>>() {};

  /** The tag for the main output for the UDF. */
  public static final TupleTag<FailsafeElement<Document, String>> UDF_OUT =
          new TupleTag<FailsafeElement<Document, String>>() {};

  /** The tag for the dead-letter output of the json to table row transform. */
  public static final TupleTag<FailsafeElement<Document, String>> TRANSFORM_DEADLETTER_OUT =
          new TupleTag<FailsafeElement<Document, String>>() {};


  /** The tag for the main output of the json transformation. */
  public static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<TableRow>() {};


  public interface Options extends PipelineOptions, MongoDbOptions, BigQueryWriteOptions, JavascriptDocumentTransformerOptions {}

  private static class ParseAsDocumentsFn extends DoFn<String, Document> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(Document.parse(context.element()));
    }
  }



  public static void main(String[] args) throws IOException, ScriptException, NoSuchMethodException{
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    run(options);
  }



  public static boolean run(Options options) throws IOException, ScriptException, NoSuchMethodException{
    Pipeline pipeline = Pipeline.create(options);
    String userOption = options.getUserOption();

    TableSchema bigquerySchema;

    if(options.getUserOption().equals("UDF")){
      bigquerySchema =
        MongoDbUtils.getTableFieldSchemaForUDF(
          options.getMongoDbUri(),
          options.getDatabase(),
          options.getCollection(),
          options.getUserOption(),
          options.getJavascriptDocumentTransformGcsPath(),
          options.getJavascriptDocumentTransformFunctionName()
                );
    }
    else{
      bigquerySchema =
        MongoDbUtils.getTableFieldSchema(
          options.getMongoDbUri(),
          options.getDatabase(),
          options.getCollection(),
          options.getUserOption());
    }

    PCollection<Document> lines = pipeline.apply(
            "Read Documents",
            MongoDbIO.read()
                .withUri(options.getMongoDbUri())
                .withDatabase(options.getDatabase())
                .withCollection(options.getCollection())
            )
            .apply(
                    "UDF",
                    TransformDocumentViaJavascript.newBuilder()
                            .setFileSystemPath(options.getJavascriptDocumentTransformGcsPath())
                            .setFunctionName(options.getJavascriptDocumentTransformFunctionName())
                            .build()
            );

    lines.apply(
            "Transform to TableRow",
            ParDo.of(
                new DoFn<Document, TableRow>() {

                  @ProcessElement
                  public void process(ProcessContext c) {
                    Document document = c.element();
                    Gson gson = new GsonBuilder().create();
                    HashMap<String, Object> parsedMap =
                        gson.fromJson(document.toJson(), HashMap.class);
                    TableRow row = MongoDbUtils.getTableSchema(parsedMap, userOption);
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


