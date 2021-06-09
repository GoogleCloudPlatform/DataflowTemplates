/*
 * Copyright (C) 2020 Google Inc.
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

package com.google.cloud.teleport.v2.templates;
import com.google.cloud.teleport.v2.cdc.sources.DataStreamIO;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This pipeline ingests DataStream data from GCS. The data is then transformed to BSON documents
 * they are automatically added to MongoDB.
 *
 * <p>Example Usage: TODO: FIX EXMAPLE USAGE
 *
 */
public class DataStreamToMongoDB {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToMongoDB.class);
  private static final String AVRO_SUFFIX = "avro";
  private static final String JSON_SUFFIX = "json";
  public static final Set<String> MAPPER_IGNORE_FIELDS = new HashSet<String>(
          Arrays.asList(
                  "_metadata_stream", "_metadata_schema", "_metadata_table", "_metadata_source",
                  "_metadata_ssn", "_metadata_rs_id", "_metadata_tx_id",
                  "_metadata_dlq_reconsumed", "_metadata_error", "_metadata_retry_count",
                  "_metadata_timestamp", "_metadata_read_timestamp", "_metadata_read_method",
                  "_metadata_source_type", "_metadata_deleted", "_metadata_change_type",
                  "_metadata_log_file", "_metadata_log_position"));

  /** The tag for the main output of the json transformation. */
  public static final TupleTag<FailsafeElement<String, String>> TRANSFORM_OUT =
          new TupleTag<FailsafeElement<String, String>>() {};


  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.</p>
   */
  public interface Options extends PipelineOptions, StreamingOptions {
    @Description("The GCS location of the avro files you'd like to process")
    String getInputFilePattern();
    void setInputFilePattern(String value);

    @Description("The GCS output format avro/json")
    @Default.String("json")
    String getInputFileFormat();
    void setInputFileFormat(String value);

    @Description(
        "The Pub/Sub subscription with DataStream file notifications."
            + "The name should be in the format of "
            + "projects/<project-id>/subscriptions/<subscription-name>.")
    String getInputSubscription();
    void setInputSubscription(String value);

    @Description("The DataStream Stream to Reference.")
    String getStreamName();
    void setStreamName(String value);

    @Description("The starting DateTime used to fetch from GCS (https://tools.ietf.org/html/rfc3339).")
    @Default.String("1970-01-01T00:00:00.00Z")
    String getRfcStartDateTime();
    void setRfcStartDateTime(String value);

    @Description("The number of concurrent DataStream files to read.")
    @Default.Integer(10)
    Integer getFileReadConcurrency();
    void setFileReadConcurrency(Integer value);

    // Destination MongoDB Connection String
    @Description("The MongoDB Atlas connection string")
    String getMongoDBUri();
    void setMongoDBUri(String value);

    @Description("The MongoDB Database name")
    String getDatabase();
    void setDatabase(String value);

    @Description("The MongoDB Collection")
    String getCollection();
    void setCollection(String value);


  }

  /**
   * Main entry point for executing the pipeline.
   * @param args  The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    LOG.info("Starting Input Files to BigQuery");

    Options options = PipelineOptionsFactory
        .fromArgs(args)
        .withValidation()
        .as(Options.class);

    options.setStreaming(true);

    validateOptions(options);
    run(options);
  }

  private static void validateOptions(Options options) {

    String inputFileFormat = options.getInputFileFormat();
    if (!(inputFileFormat.equals(AVRO_SUFFIX)
           || inputFileFormat.equals(JSON_SUFFIX))){
      throw new IllegalArgumentException(
          "Input file format must be one of: avro, json or left empty - found " + inputFileFormat);
    }
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return  The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {
    /*
     * Stages:
     *   1) Ingest and Normalize Data to FailsafeElement with JSON Strings
     *   2) Push the data to MongoDB
     */

    Pipeline pipeline = Pipeline.create(options);

    /*
     * Stage 1: Ingest and Normalize Data to FailsafeElement with JSON Strings
     *   a) Read DataStream data from GCS into JSON String FailsafeElements (datastreamJsonRecords)
     */
     PCollection<FailsafeElement<String, String>> datastreamJsonRecords = pipeline
         .apply(
             new DataStreamIO(
                    options.getStreamName(),
                    options.getInputFilePattern(),
                    options.getInputFileFormat(),
                    options.getInputSubscription(),
                    options.getRfcStartDateTime())
             .withFileReadConcurrency(options.getFileReadConcurrency()));


    PCollection<FailsafeElement<String, String>> jsonRecords =
        PCollectionList.of(datastreamJsonRecords)
            .apply(Flatten.pCollections());
    /**
     * Does below steps:
     * 1. Converts JSON to BSON documents.
     * 2. Removes the metadata fileds.
     * 3. Inserts the data into MongoDB collections.
     */
    jsonRecords
            .apply("jsonToDocuments", MapElements.via(
                    new SimpleFunction<FailsafeElement<String, String>, Document>() {
                      @Override
                      public Document apply(FailsafeElement<String, String> jsonString) {
                        String s = jsonString.getOriginalPayload();
                        Document doc = Document.parse(s);
                        return removeTableRowFields(doc, MAPPER_IGNORE_FIELDS);
                      }
                    }))
        .apply(
            "Write To MongoDB",
                MongoDbIO.write().withUri(options.getMongoDBUri())
                        .withDatabase(options.getDatabase())
                        .withCollection(options.getCollection())
                        //To update to an existing collection. Beam connector change is in
                        //progress after which this can be used.
                        //.withIsUpdate(true)
                        //.withUpdateKey("accId").withUpdateField("transactions")
                        //.withUpdateOperator("$push")
        );

    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  /** DoFn that will parse the given string elements as Bson Documents. */
  private static class ParseAsDocumentsFn extends DoFn<String, Document> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(new Document("value", context.element()));
    }
  }

  private static Document removeTableRowFields(Document doc, Set<String> ignoreFields) {

    for (String ignoreField : ignoreFields) {
      if (doc.containsKey(ignoreField)) {
        doc.remove(ignoreField);
      }
    }

    return doc;
  }
}
