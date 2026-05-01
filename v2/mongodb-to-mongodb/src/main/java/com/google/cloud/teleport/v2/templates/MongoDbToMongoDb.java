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

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.transforms.MongoDbTransforms;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.mongodb.FindQuery;
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
import org.bson.BsonDocument;
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

    @TemplateParameter.Text(
        order = 7,
        groupName = "Source",
        optional = true,
        description = "BSON Filter",
        helpText = "BSON query for server-side filtering.")
    String getFilter();

    void setFilter(String value);

    @TemplateParameter.Text(
        order = 8,
        groupName = "Source",
        optional = true,
        description = "Projection",
        helpText = "Fields to include or exclude.")
    String getProjection();

    void setProjection(String value);

    @TemplateParameter.Boolean(
        order = 9,
        groupName = "Source",
        optional = true,
        description = "Use BucketAuto",
        helpText = "Enable withBucketAuto for Atlas compatibility.")
    @Default.Boolean(false)
    Boolean getUseBucketAuto();

    void setUseBucketAuto(Boolean value);

    @TemplateParameter.Integer(
        order = 10,
        groupName = "Source",
        optional = true,
        description = "Number of Splits",
        helpText = "Suggest a specific number of partitions for reading.")
    Integer getNumSplits();

    void setNumSplits(Integer value);

    @TemplateParameter.Integer(
        order = 11,
        groupName = "Target",
        optional = true,
        description = "Batch Size",
        helpText = "Number of documents in a bulk write.")
    @Default.Integer(5000)
    Integer getBatchSize();

    void setBatchSize(Integer value);

    @TemplateParameter.Boolean(
        order = 12,
        groupName = "Target",
        optional = true,
        description = "Ordered Bulk Write",
        helpText = "Allow parallel execution and prevent batch failure on single error.")
    @Default.Boolean(false)
    Boolean getOrdered();

    void setOrdered(Boolean value);

    @TemplateParameter.Text(
        order = 13,
        groupName = "Target",
        optional = true,
        description = "Write Concern",
        helpText = "Acknowledgment level (e.g., 'w: 1').")
    String getWriteConcern();

    void setWriteConcern(String value);

    @TemplateParameter.Boolean(
        order = 14,
        groupName = "Target",
        optional = true,
        description = "Journaling",
        helpText = "Enable/disable journaling.")
    Boolean getJournal();

    void setJournal(Boolean value);

    @TemplateParameter.Text(
        order = 15,
        optional = true,
        description = "Process DLQ Path",
        helpText = "Path to store failed events during processing.")
    String getProcessDlqPath();

    void setProcessDlqPath(String value);

    @TemplateParameter.Text(
        order = 16,
        optional = true,
        description = "Write DLQ Path",
        helpText = "Path to store failed events during writing.")
    String getWriteDlqPath();

    void setWriteDlqPath(String value);
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

    for (String collection : collections) {
      String targetCollection = options.getTargetCollection();
      if (targetCollection == null || targetCollection.isEmpty()) {
        targetCollection = collection; // Use source collection name if target not provided
      }

      MongoDbIO.Read read =
          MongoDbIO.read()
              .withUri(options.getSourceUri())
              .withDatabase(options.getSourceDatabase())
              .withCollection(collection);

      String filterJson = options.getFilter();
      if (filterJson != null && !filterJson.isEmpty()) {
        BsonDocument filter = BsonDocument.parse(filterJson);
        read = read.withQueryFn(FindQuery.create().withFilters(filter));
      }

      String projectionStr = options.getProjection();
      if (projectionStr != null && !projectionStr.isEmpty()) {
        List<String> projection = Arrays.asList(projectionStr.split(","));
        read = read.withQueryFn(FindQuery.create().withProjection(projection));
      }

      if (options.getUseBucketAuto() != null && options.getUseBucketAuto()) {
        read = read.withBucketAuto(true);
      }

      if (options.getNumSplits() != null) {
        read = read.withNumSplits(options.getNumSplits());
      }

      PCollection<Document> documents = pipeline.apply("Read_" + collection, read);

      // Validation Stage with DLQ
      TupleTag<Document> successTag = new TupleTag<Document>() {};
      TupleTag<String> failureTag = new TupleTag<String>() {};

      PCollectionTuple processed =
          documents.apply(
              "Validate_" + collection,
              ParDo.of(
                      new DoFn<Document, Document>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          Document doc = c.element();
                          if (doc == null) {
                            c.output(failureTag, "Null document");
                          } else {
                            c.output(successTag, doc);
                          }
                        }
                      })
                  .withOutputTags(successTag, TupleTagList.of(failureTag)));

      // Write Process Failures to DLQ
      if (options.getProcessDlqPath() != null && !options.getProcessDlqPath().isEmpty()) {
        processed
            .get(failureTag)
            .apply(
                "WriteProcessDlq_" + collection,
                TextIO.write().to(options.getProcessDlqPath() + "/" + collection));
      }

      // Write Stage with DLQ
      PCollection<Document> validDocs = processed.get(successTag);

      validDocs.apply(
          "Write_" + collection,
          MongoDbTransforms.writeWithDlq()
              .withUri(options.getTargetUri())
              .withDatabase(options.getTargetDatabase())
              .withCollection(targetCollection)
              .withBatchSize(options.getBatchSize())
              .withOrdered(options.getOrdered())
              .withWriteConcern(options.getWriteConcern())
              .withJournal(options.getJournal())
              .withDlqPath(options.getWriteDlqPath()));
    }

    pipeline.run();
  }
}
