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
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.BigQueryWriteOptions;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.MongoDbOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.beam.sdk.Pipeline;
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
import org.bson.Document;



/**
 * The {@link BigQueryToMongoDb} pipeline is a batch pipeline which ingests data from MongoDB and
 * outputs the resulting records to BigQuery.
 */
public class MongoDbToBigQuery {
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


  public interface Options extends PipelineOptions, MongoDbOptions, BigQueryWriteOptions {}

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
    TableSchema bigquerySchema =
        MongoDbUtils.getTableFieldSchema(
            options.getMongoDbUri(),
            options.getDatabase(),
            options.getCollection(),
            options.getUserOption());
    String userOption = options.getUserOption();
    PCollection<Document> lines = pipeline.apply(
            "Read Documents",
            MongoDbIO.read()
                .withUri(options.getMongoDbUri())
                .withDatabase(options.getDatabase())
                .withCollection(options.getCollection()));


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

  static class DocumentToTableRow
          extends PTransform<PCollection<Document>, PCollectionTuple> {

    private final Options options;

    DocumentToTableRow(Options options) {
      this.options = options;
    }

    @Override
    public PCollectionTuple expand(PCollection<Document> input) {

      PCollectionTuple udfOut =
        input
          // Map the incoming messages into FailsafeElements so we can recover from failures
          // across multiple transforms.
          .apply("MapToRecord", ParDo.of(new DocumentToFailsafeElementFn()))
          .apply(
            "InvokeUDF",
            FailsafeJavascriptUdf.<Document>newBuilder()
                    .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                    .setFunctionName(options.getJavascriptTextTransformFunctionName())
                    .setSuccessTag(UDF_OUT)
                    .setFailureTag(UDF_DEADLETTER_OUT)
                    .build());

      // Convert the records which were successfully processed by the UDF into TableRow objects.
      PCollectionTuple jsonToTableRowOut =
              udfOut
                      .get(UDF_OUT)
                      .apply(
                              "JsonToTableRow",
                              FailsafeJsonToTableRow.<Document>newBuilder()
                                      .setSuccessTag(TRANSFORM_OUT)
                                      .setFailureTag(TRANSFORM_DEADLETTER_OUT)
                                      .build());

      // Re-wrap the PCollections so we can return a single PCollectionTuple
      return PCollectionTuple.of(UDF_OUT, udfOut.get(UDF_OUT))
              .and(UDF_DEADLETTER_OUT, udfOut.get(UDF_DEADLETTER_OUT))
              .and(TRANSFORM_OUT, jsonToTableRowOut.get(TRANSFORM_OUT))
              .and(TRANSFORM_DEADLETTER_OUT, jsonToTableRowOut.get(TRANSFORM_DEADLETTER_OUT));
    }
  }

  /**
   * The {@link DocumentToFailsafeElementFn} wraps an incoming {@link Document} with the
   * {@link FailsafeElement} class so errors can be recovered from and the original message can be
   * output to a error records table.
   */
  static class DocumentToFailsafeElementFn
          extends DoFn<Document, FailsafeElement<Document, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      Document message = context.element();
      System.out.println("Failed !!");
//      context.output(
//              FailsafeElement.of(message, new String(message, StandardCharsets.UTF_8)));
    }
  }
}
