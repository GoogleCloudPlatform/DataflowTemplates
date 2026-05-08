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
import com.google.cloud.teleport.v2.transforms.DocumentWithMetadata;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer;
import com.google.cloud.teleport.v2.transforms.MongoDbTransforms;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
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

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(MongoDbToMongoDb.class);

  public interface Options extends JavascriptTextTransformer.JavascriptTextTransformerOptions {
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
        description = "DLQ Directory",
        helpText =
            "Base path to store failed events. Events will be grouped by date and time, and"
                + " separated into 'retryable' and 'permanent' subdirectories.")
    String getDlqDirectory();

    void setDlqDirectory(String value);

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
        groupName = "Source",
        optional = true,
        description = "Reconsume DLQ Path",
        helpText =
            "Path to read files from DLQ for reprocessing. If not provided, write DLQ path will be"
                + " used.")
    String getReconsumeDlqPath();

    void setReconsumeDlqPath(String value);

    @TemplateParameter.Boolean(
        order = 17,
        groupName = "Source",
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

    List<String> sourceCollections = new ArrayList<>();
    if (sourceCollection != null && !sourceCollection.isEmpty()) {
      sourceCollections.add(sourceCollection);
    } else {
      // List collections from source
      try (MongoClient mongoClient = MongoClients.create(sourceUri)) {
        MongoDatabase db = mongoClient.getDatabase(sourceDatabase);
        for (String name : db.listCollectionNames()) {
          sourceCollections.add(name);
        }
      }
    }

    String stagingLocation = options.as(DataflowPipelineOptions.class).getStagingLocation();
    String tmpDirectory = options.getTempLocation();
    if (tmpDirectory == null || tmpDirectory.isEmpty()) {
      if (stagingLocation != null && !stagingLocation.isEmpty()) {
        tmpDirectory =
            stagingLocation.endsWith("/") ? stagingLocation + "tmp" : stagingLocation + "/tmp";
      } else {
        tmpDirectory = "/tmp";
      }
    }

    String dlqDirectory = options.getDlqDirectory();
    if (dlqDirectory == null || dlqDirectory.isEmpty()) {
      dlqDirectory = tmpDirectory;
    }
    String baseDlqPath = dlqDirectory.endsWith("/") ? dlqDirectory : dlqDirectory + "/";
    String timestampPath = new SimpleDateFormat("yyyy-MM-dd/HH-mm-ss").format(new Date());
    String retryableDlqPath = baseDlqPath + timestampPath + "/retryable";
    String permanentDlqPath = baseDlqPath + timestampPath + "/permanent";

    if (options.getReadFromDlq() != null && options.getReadFromDlq()) {
      String reconsumePath = options.getReconsumeDlqPath();
      if (reconsumePath == null || reconsumePath.isEmpty()) {
        throw new IllegalArgumentException(
            "Reconsume DLQ path must be specified when reading from DLQ.");
      }
      PCollection<DocumentWithMetadata> documents = readFromDlq(pipeline, reconsumePath);
      documents.apply(
          "ProcessDlq", new ProcessDocuments(options, retryableDlqPath, permanentDlqPath));
    } else {
      for (String inputCollection : sourceCollections) {
        String targetCollectionRaw = options.getTargetCollection();
        final String targetCollection =
            (targetCollectionRaw == null || targetCollectionRaw.isEmpty())
                ? inputCollection
                : targetCollectionRaw;

        PCollection<DocumentWithMetadata> documents =
            readFromMongo(pipeline, options, inputCollection, targetCollection);
        documents.apply(
            "Process_" + inputCollection,
            new ProcessDocuments(options, retryableDlqPath, permanentDlqPath));
      }
    }

    pipeline.run();
  }

  public static class ProcessDocuments
      extends PTransform<PCollection<DocumentWithMetadata>, PDone> {
    private final transient Options options;
    private final String retryableDlqPath;
    private final String permanentDlqPath;

    public ProcessDocuments(Options options, String retryableDlqPath, String permanentDlqPath) {
      this.options = options;
      this.retryableDlqPath = retryableDlqPath;
      this.permanentDlqPath = permanentDlqPath;
    }

    @Override
    public PDone expand(PCollection<DocumentWithMetadata> input) {
      PCollection<DocumentWithMetadata> documents =
          input.apply(
              "CountTotalProcessed",
              ParDo.of(
                  new DoFn<DocumentWithMetadata, DocumentWithMetadata>() {
                    private final Counter totalProcessedDocuments =
                        Metrics.counter(MongoDbToMongoDb.class, "totalProcessedDocuments");

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      totalProcessedDocuments.inc();
                      c.output(c.element());
                    }
                  }));

      // UDF Stage
      if (options.getJavascriptTextTransformGcsPath() != null
          && !options.getJavascriptTextTransformGcsPath().isEmpty()) {
        TupleTag<DocumentWithMetadata> udfSuccessTag = new TupleTag<DocumentWithMetadata>() {};
        TupleTag<DocumentWithMetadata> udfFailureTag = new TupleTag<DocumentWithMetadata>() {};

        PCollectionTuple udfProcessed =
            documents.apply(
                "ApplyUDF",
                ParDo.of(
                        new MongoDbTransforms.ApplyUdfFn(
                            options.getJavascriptTextTransformGcsPath(),
                            options.getJavascriptTextTransformFunctionName(),
                            options.getJavascriptTextTransformReloadIntervalMinutes(),
                            udfFailureTag))
                    .withOutputTags(udfSuccessTag, TupleTagList.of(udfFailureTag)));

        // Write UDF Failures to DLQ
        udfProcessed
            .get(udfFailureTag)
            .apply(
                "WriteToDlq_UDF",
                new MongoDbTransforms.WriteToDlq(
                    retryableDlqPath, permanentDlqPath, options.getTempLocation()));

        documents =
            udfProcessed
                .get(udfSuccessTag)
                .setCoder(SerializableCoder.of(DocumentWithMetadata.class));
      }

      // Validation Stage with DLQ
      TupleTag<DocumentWithMetadata> successTag = new TupleTag<DocumentWithMetadata>() {};
      TupleTag<DocumentWithMetadata> failureTag = new TupleTag<DocumentWithMetadata>() {};

      PCollectionTuple processed =
          documents.apply(
              "Validate",
              ParDo.of(
                      new DoFn<DocumentWithMetadata, DocumentWithMetadata>() {
                        private final Counter contextCreationFailures =
                            Metrics.counter(MongoDbToMongoDb.class, "contextCreationFailures");

                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          DocumentWithMetadata item = c.element();
                          if (item == null || item.getDocument() == null) {
                            contextCreationFailures.inc();
                            c.output(
                                failureTag,
                                DocumentWithMetadata.of(
                                    null,
                                    null,
                                    0,
                                    "Null document",
                                    DocumentWithMetadata.ErrorType.PERMANENT,
                                    null,
                                    null,
                                    DocumentWithMetadata.FailureStage.VALIDATE));
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
              "WriteToDlq_Validate",
              new MongoDbTransforms.WriteToDlq(
                  retryableDlqPath, permanentDlqPath, options.getTempLocation()));

      // Write Stage with DLQ
      PCollection<DocumentWithMetadata> validDocs = processed.get(successTag);

      PCollection<DocumentWithMetadata> writeFailures =
          validDocs.apply(
              "Write",
              MongoDbTransforms.writeWithDlq()
                  .withUri(options.getTargetUri())
                  .withDatabase(options.getTargetDatabase())
                  .withBatchSize(options.getBatchSize())
                  .withMaxConcurrentAsyncWrites(options.getMaxConcurrentAsyncWrites())
                  .withMaxWriteRetries(options.getMaxWriteRetries())
                  .withDlqMaxRetries(options.getDlqMaxRetries()));

      writeFailures.apply(
          "WriteToDlq_Write",
          new MongoDbTransforms.WriteToDlq(
              retryableDlqPath, permanentDlqPath, options.getTempLocation()));

      return PDone.in(input.getPipeline());
    }
  }

  private static PCollection<DocumentWithMetadata> readFromDlq(
      Pipeline pipeline, String reconsumePath) {
    PCollection<String> dlqStrings =
        pipeline.apply("ReadDlq", TextIO.read().from(reconsumePath + "/**"));

    return dlqStrings.apply(
        "ParseDlq",
        ParDo.of(
            new DoFn<String, DocumentWithMetadata>() {
              private final Counter retriedDocuments =
                  Metrics.counter(MongoDbToMongoDb.class, "retriedDocuments");
              private final Counter contextCreationFailures =
                  Metrics.counter(MongoDbToMongoDb.class, "contextCreationFailures");

              @ProcessElement
              public void processElement(ProcessContext c) {
                String line = c.element();
                try {
                  c.output(DocumentWithMetadata.fromDlqJson(line));
                  retriedDocuments.inc();
                } catch (Exception e) {
                  LOG.error("Failed to parse DLQ event", e);
                  contextCreationFailures.inc();
                }
              }
            }));
  }

  private static PCollection<DocumentWithMetadata> readFromMongo(
      Pipeline pipeline, Options options, String sourceCollection, String targetCollection) {
    MongoDbIO.Read read =
        MongoDbIO.read()
            .withUri(options.getSourceUri())
            .withDatabase(options.getSourceDatabase())
            .withCollection(sourceCollection);

    if (options.getUseBucketAuto() != null && options.getUseBucketAuto()) {
      read = read.withBucketAuto(true);
    }

    if (options.getNumSplits() != null) {
      read = read.withNumSplits(options.getNumSplits());
    }

    return pipeline
        .apply("Read_" + sourceCollection, read)
        .apply(
            "MapToMetadata_" + sourceCollection,
            ParDo.of(
                new DoFn<Document, DocumentWithMetadata>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    c.output(
                        DocumentWithMetadata.of(c.element(), sourceCollection, targetCollection));
                  }
                }));
  }
}
