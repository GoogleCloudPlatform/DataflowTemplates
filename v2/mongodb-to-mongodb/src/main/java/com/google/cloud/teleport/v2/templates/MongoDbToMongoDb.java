/*
 * Copyright (C) 2026 Google LLC
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.cdc.dlq.FileBasedDeadLetterQueueReconsumer;
import com.google.cloud.teleport.v2.transforms.DLQWriteTransform;
import com.google.cloud.teleport.v2.transforms.DocumentWithMetadata;
import com.google.cloud.teleport.v2.transforms.MongoDbTransforms;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.bson.Document;

/** Dataflow template which copies data from one MongoDB database to another. */
@Template(
    name = "Mongodb_To_Mongodb",
    category = TemplateCategory.BATCH,
    displayName = "MongoDB to MongoDB",
    description = "Copy data from one MongoDB database to another.",
    flexContainerName = "mongodb-to-mongodb",
    optionsClass = MongoDbToMongoDb.Options.class)
public class MongoDbToMongoDb {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(MongoDbToMongoDb.class);

  public interface Options extends PipelineOptions {
    @TemplateParameter.Text(
        order = 1,
        groupName = "Source",
        description = "Source MongoDB Connection URI",
        helpText = "URI to connect to the source MongoDB cluster.")
    @Validation.Required
    String getSourceUri();

    void setSourceUri(String value);

    @TemplateParameter.Text(
        order = 2,
        groupName = "Target",
        description = "Target MongoDB Connection URI",
        helpText = "URI to connect to the target MongoDB cluster.")
    @Validation.Required
    String getTargetUri();

    void setTargetUri(String value);

    @TemplateParameter.Text(
        order = 3,
        groupName = "Source",
        description = "Source MongoDB Database",
        helpText = "Database in the source MongoDB to read from.")
    @Validation.Required
    String getSourceDatabase();

    void setSourceDatabase(String value);

    @TemplateParameter.Text(
        order = 4,
        groupName = "Target",
        description = "Target MongoDB Database",
        helpText = "Database in the target MongoDB to write to.")
    @Validation.Required
    String getTargetDatabase();

    void setTargetDatabase(String value);

    @TemplateParameter.Text(
        order = 5,
        groupName = "Source",
        optional = true,
        description = "Source MongoDB Collection",
        helpText =
            "Collection in the source MongoDB to read from. If not provided, all collections in the"
                + " database will be migrated.")
    String getSourceCollection();

    void setSourceCollection(String value);

    @TemplateParameter.Text(
        order = 6,
        groupName = "Target",
        optional = true,
        description = "Target MongoDB Collection",
        helpText =
            "Collection in the target MongoDB to write to. If not provided, source collection names"
                + " will be used.")
    String getTargetCollection();

    void setTargetCollection(String value);

    @TemplateParameter.Boolean(
        order = 7,
        groupName = "Source",
        optional = true,
        description = "Use BucketAuto",
        helpText = "Enable withBucketAuto for Atlas compatibility.")
    @Default.Boolean(false)
    Boolean getUseBucketAuto();

    void setUseBucketAuto(Boolean value);

    @TemplateParameter.Integer(
        order = 8,
        groupName = "Source",
        optional = true,
        description = "Number of Splits",
        helpText = "Suggest a specific number of partitions for reading.")
    Integer getNumSplits();

    void setNumSplits(Integer value);

    @TemplateParameter.Integer(
        order = 9,
        groupName = "Target",
        optional = true,
        description = "Batch Size",
        helpText = "Number of documents in a bulk write.")
    @Default.Integer(5000)
    Integer getBatchSize();

    void setBatchSize(Integer value);

    @TemplateParameter.Text(
        order = 11,
        optional = true,
        description = "Process DLQ Path",
        helpText = "Path to store failed events during processing.")
    String getProcessDlqPath();

    void setProcessDlqPath(String value);

    @TemplateParameter.Text(
        order = 12,
        optional = true,
        description = "Write DLQ Path",
        helpText = "Path to store failed events during writing.")
    String getWriteDlqPath();

    void setWriteDlqPath(String value);

    @TemplateParameter.Integer(
        order = 13,
        optional = true,
        description = "Max Concurrent Async Writes",
        helpText = "Maximum number of concurrent asynchronous batch writes per worker.")
    @Default.Integer(10)
    Integer getMaxConcurrentAsyncWrites();

    void setMaxConcurrentAsyncWrites(Integer value);

    @TemplateParameter.Integer(
        order = 14,
        optional = true,
        description = "Max Write Retries",
        helpText = "Maximum number of retry attempts for transient failures during write.")
    @Default.Integer(3)
    Integer getMaxWriteRetries();

    void setMaxWriteRetries(Integer value);

    @TemplateParameter.Integer(
        order = 15,
        optional = true,
        description = "DLQ Max Retries",
        helpText = "Maximum number of times to retry events from DLQ.")
    @Default.Integer(3)
    Integer getDlqMaxRetries();

    void setDlqMaxRetries(Integer value);

    @TemplateParameter.Text(
        order = 16,
        optional = true,
        description = "Reconsume DLQ Path",
        helpText =
            "Path to read files from DLQ for reprocessing. If not provided, write DLQ path will be"
                + " used.")
    String getReconsumeDlqPath();

    void setReconsumeDlqPath(String value);

    @TemplateParameter.Boolean(
        order = 17,
        optional = true,
        description = "Read from DLQ",
        helpText = "If true, reads only from DLQ for retry. If false, reads from MongoDB.")
    @Default.Boolean(false)
    Boolean getReadFromDlq();

    void setReadFromDlq(Boolean value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    run(options);
  }

  public static void run(Options options) {
    Pipeline pipeline = Pipeline.create(options);

    String sourceUri = options.getSourceUri();
    String sourceDatabase = options.getSourceDatabase();
    String sourceCollection = options.getSourceCollection();

    List<String> collections = new ArrayList<>();
    if (sourceCollection != null && !sourceCollection.isEmpty()) {
      collections.add(sourceCollection);
    } else {
      // List collections from source
      try (MongoClient mongoClient = MongoClients.create(sourceUri)) {
        MongoDatabase db = mongoClient.getDatabase(sourceDatabase);
        for (String name : db.listCollectionNames()) {
          collections.add(name);
        }
      }
    }

    String writeDlqPath =
        getDefaultDlqPath(options.getWriteDlqPath(), options.getTempLocation(), "dlq");
    String processDlqPath =
        getDefaultDlqPath(options.getProcessDlqPath(), options.getTempLocation(), "process-dlq");

    for (String collection : collections) {
      String targetCollection = options.getTargetCollection();
      if (targetCollection == null || targetCollection.isEmpty()) {
        targetCollection = collection; // Use source collection name if target not provided
      }

      PCollection<DocumentWithMetadata> documents;

      if (options.getReadFromDlq() != null && options.getReadFromDlq()) {
        String reconsumePath = options.getReconsumeDlqPath();
        if (reconsumePath == null || reconsumePath.isEmpty()) {
          reconsumePath = writeDlqPath;
        }
        documents = readFromDlq(pipeline, collection, reconsumePath);
      } else {
        documents = readFromMongo(pipeline, options, collection);
      }

      // Validation Stage with DLQ
      TupleTag<DocumentWithMetadata> successTag = new TupleTag<DocumentWithMetadata>() {};
      TupleTag<String> failureTag = new TupleTag<String>() {};

      PCollectionTuple processed =
          documents.apply(
              "Validate_" + collection,
              ParDo.of(
                      new DoFn<DocumentWithMetadata, DocumentWithMetadata>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          DocumentWithMetadata item = c.element();
                          if (item == null || item.getDocument() == null) {
                            c.output(failureTag, "Null document");
                          } else {
                            c.output(successTag, item);
                          }
                        }
                      })
                  .withOutputTags(successTag, TupleTagList.of(failureTag)));

      // Write Process Failures to DLQ
      processed
          .get(failureTag)
          .apply(
              "WriteProcessDlq_" + collection,
              DLQWriteTransform.WriteDLQ.newBuilder()
                  .withDlqDirectory(processDlqPath + "/" + collection)
                  .withTmpDirectory(options.getTempLocation())
                  .build());

      // Write Stage with DLQ
      PCollection<DocumentWithMetadata> validDocs = processed.get(successTag);

      validDocs.apply(
          "Write_" + collection,
          MongoDbTransforms.writeWithDlq()
              .withUri(options.getTargetUri())
              .withDatabase(options.getTargetDatabase())
              .withCollection(targetCollection)
              .withBatchSize(options.getBatchSize())
              .withDlqPath(writeDlqPath)
              .withTmpDirectory(options.getTempLocation())
              .withMaxConcurrentAsyncWrites(options.getMaxConcurrentAsyncWrites())
              .withMaxWriteRetries(options.getMaxWriteRetries())
              .withDlqMaxRetries(options.getDlqMaxRetries()));
    }

    pipeline.run();
  }

  private static String getDefaultDlqPath(String dlqPath, String tempLocation, String suffix) {
    if (dlqPath != null && !dlqPath.isEmpty()) {
      return dlqPath;
    }
    if (tempLocation != null) {
      String path = tempLocation.endsWith("/") ? tempLocation : tempLocation + "/";
      return path + suffix;
    }
    return "/tmp/" + suffix;
  }

  private static PCollection<DocumentWithMetadata> readFromDlq(
      Pipeline pipeline, String collection, String reconsumePath) {
    PCollection<String> dlqStrings =
        pipeline.apply(
            "ReadDlq_" + collection, FileBasedDeadLetterQueueReconsumer.create(reconsumePath));

    return dlqStrings.apply(
        "ParseDlq_" + collection,
        ParDo.of(
            new DoFn<String, DocumentWithMetadata>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                String line = c.element();
                try {
                  JsonNode jsonNode = MAPPER.readTree(line);
                  JsonNode dataNode = jsonNode.get("data");
                  if (dataNode != null) {
                    Document doc = Document.parse(dataNode.toString());

                    Integer retryCount =
                        jsonNode.has("_metadata_retry_count")
                            ? jsonNode.get("_metadata_retry_count").asInt()
                            : 0;
                    String errorMsg =
                        jsonNode.has("_metadata_error")
                            ? jsonNode.get("_metadata_error").asText()
                            : null;
                    String errorType =
                        jsonNode.has("error_type") ? jsonNode.get("error_type").asText() : null;

                    c.output(DocumentWithMetadata.of(doc, retryCount, errorMsg, errorType));
                  }
                } catch (Exception e) {
                  LOG.error("Failed to parse DLQ event", e);
                }
              }
            }));
  }

  private static PCollection<DocumentWithMetadata> readFromMongo(
      Pipeline pipeline, Options options, String collection) {
    MongoDbIO.Read read =
        MongoDbIO.read()
            .withUri(options.getSourceUri())
            .withDatabase(options.getSourceDatabase())
            .withCollection(collection);

    if (options.getUseBucketAuto() != null && options.getUseBucketAuto()) {
      read = read.withBucketAuto(true);
    }

    if (options.getNumSplits() != null) {
      read = read.withNumSplits(options.getNumSplits());
    }

    return pipeline
        .apply("Read_" + collection, read)
        .apply(
            "MapToMetadata_" + collection,
            ParDo.of(
                new DoFn<Document, DocumentWithMetadata>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(DocumentWithMetadata.of(c.element()));
                  }
                }));
  }
}
