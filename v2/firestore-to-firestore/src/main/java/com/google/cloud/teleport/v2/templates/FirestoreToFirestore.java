/*
 * Copyright (C) 2025 Google LLC
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

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.transforms.CreatePartitionQueryRequestFn;
import com.google.cloud.teleport.v2.transforms.PrepareWritesFn;
import com.google.cloud.teleport.v2.transforms.RunQueryResponseToDocumentFn;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.Write;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.WriteFailure;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Template(
    name = "Cloud_Firestore_to_Firestore",
    category = TemplateCategory.BATCH,
    displayName = "Firestore to Firestore",
    description = {
      "The Firestore to Firestore template is a batch pipeline that reads documents from one"
          + " <a href=\"https://cloud.google.com/firestore/docs\">Firestore</a> database and writes"
          + " them to another Firestore database. ",
      "It is not supported for Enterprise edition databases to be the source.",
      "Data consistency is guaranteed only at the end of the pipeline when all data has been"
          + " written to the destination database. Any errors that occur during operation are recorded in error queues. \n",
    },
    flexContainerName = "firestore-to-firestore",
    optionsClass = FirestoreToFirestore.Options.class)
public class FirestoreToFirestore {

  private static final Logger LOG = LoggerFactory.getLogger(FirestoreToFirestore.class);

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard Dataflow configuration options.
   */
  public interface Options extends DataflowPipelineWorkerPoolOptions {

    @TemplateParameter.Text(
        groupName = "Source",
        order = 1,
        description = "Source Project ID",
        helpText = "The source project to read from.",
        example = "my-project")
    String getSourceProjectId();

    void setSourceProjectId(String value);

    @TemplateParameter.Text(
        groupName = "Source",
        order = 2,
        description = "Source Database ID",
        helpText = "The source database to read from. Use '(default)' for the default database.",
        example = "my-database")
    String getSourceDatabaseId();

    void setSourceDatabaseId(String value);

    @TemplateParameter.Text(
        groupName = "Source",
        order = 3,
        description = "Database collections to copy",
        helpText =
            "If specified, only replicate these collections."
                + " If not specified, copy all collections.",
        example = "my-collection1,my-collection2",
        optional = true)
    @Default.String("")
    String getCollectionIds();

    void setCollectionIds(String value);

    @TemplateParameter.Text(
        groupName = "Destination",
        order = 4,
        description = "Destination Project ID",
        helpText =
            "The destination project to write to." + " Defaults to the source project if not set.",
        example = "my-project",
        optional = true)
    @Default.String("")
    String getDestinationProjectId();

    void setDestinationProjectId(String value);

    @TemplateParameter.Text(
        groupName = "Destination",
        order = 5,
        description = "Destination Database ID",
        helpText =
            "The destination database to write to. Use '(default)' for the default database.",
        example = "my-database")
    String getDestinationDatabaseId();

    void setDestinationDatabaseId(String value);

    @TemplateParameter.DateTime(
        order = 6,
        optional = true,
        description = "Read Time",
        helpText =
            "The read time of the Firestore read operations."
                + " Uses current timestamp if not set.",
        example = "2021-10-12T07:20:50.52Z")
    @Default.String("")
    String getReadTime();

    void setReadTime(String readTime);

    @TemplateParameter.GcsWriteFolder(
        order = 7,
        optional = true,
        description = "Error Write Path",
        helpText =
            "The Cloud Storage path to write error logs to. Path should be in the format gs://<bucket>/<folder>/.",
        example = "gs://your-bucket/errors/")
    String getErrorWritePath();

    void setErrorWritePath(String value);
  }

  private static final int DEFAULT_MAX_NUM_WORKERS = 500;

  public static void main(String[] args) {
    try {
      UncaughtExceptionLogger.register();

      LOG.info("Starting Firestore to Firestore pipeline...");

      Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
      validateOptions(options);
      LOG.info("Pipeline options parsed and validated.");

      Pipeline p = Pipeline.create(options);
      LOG.info("Pipeline created.");

      String sourceProjectId = options.getSourceProjectId();
      String sourceDatabaseId = options.getSourceDatabaseId();

      String destinationProjectId =
          options.getDestinationProjectId().isEmpty()
              ? sourceProjectId
              : options.getDestinationProjectId();
      String destinationDatabaseId = options.getDestinationDatabaseId();

      List<String> collectionIdsList;
      String collectionIds = options.getCollectionIds();
      if (collectionIds.isEmpty()) {
        try {
          collectionIdsList = getAllCollectionIds(sourceProjectId, sourceDatabaseId);
        } catch (Exception e) {
          throw new RuntimeException(
              "Failed to list collections for project: "
                  + sourceProjectId
                  + ", database: "
                  + sourceDatabaseId,
              e);
        }
      } else {
        collectionIdsList =
            Arrays.stream(collectionIds.split(",")).map(String::trim).collect(Collectors.toList());
      }

      int maxNumWorkers =
          options.getMaxNumWorkers() > 0 ? options.getMaxNumWorkers() : DEFAULT_MAX_NUM_WORKERS;
      RpcQosOptions rpcQosOptions =
          RpcQosOptions.newBuilder().withHintMaxNumWorkers(maxNumWorkers).build();

      Instant readTime =
          options.getReadTime().isEmpty() ? Instant.now() : Instant.parse(options.getReadTime());

      LOG.info(
          "Starting pipeline execution with options: sourceProjectId={}, sourceDatabaseId={}, "
              + "destinationProjectId={}, destinationDatabaseId={}, collectionIds={}, "
              + "maxNumWorkers={}, readTime={}",
          sourceProjectId,
          sourceDatabaseId,
          destinationProjectId,
          destinationDatabaseId,
          collectionIdsList,
          maxNumWorkers,
          readTime);

      // 1. Construct the PartitionQuery requests for the collections.
      PCollection<PartitionQueryRequest> partitionQueryRequests =
          p.apply(Create.of(collectionIdsList))
              .apply(
                  new CreatePartitionQueryRequestFn(
                      sourceProjectId, sourceDatabaseId, maxNumWorkers));

      // 2. Apply FirestoreIO to get partitions (as RunQueryRequests)
      PCollection<RunQueryRequest> partitionedQueries =
          partitionQueryRequests.apply(
              "GetPartitions",
              FirestoreIO.v1()
                  .read()
                  .partitionQuery()
                  .withProjectId(sourceProjectId)
                  .withDatabaseId(sourceDatabaseId)
                  .withReadTime(readTime)
                  .withRpcQosOptions(rpcQosOptions)
                  .build());

      // 3. Execute each partitioned query
      PCollection<RunQueryResponse> responses =
          partitionedQueries.apply(
              "QueryDocumentsInPartitions",
              FirestoreIO.v1()
                  .read()
                  .runQuery()
                  .withProjectId(sourceProjectId)
                  .withDatabaseId(sourceDatabaseId)
                  .withReadTime(readTime)
                  .withRpcQosOptions(rpcQosOptions)
                  .build());

      // 4. Process the documents from the responses
      PCollectionTuple extractResult =
          responses.apply(
              "ExtractDocumentsAndErrors",
              ParDo.of(new RunQueryResponseToDocumentFn())
                  .withOutputTags(
                      RunQueryResponseToDocumentFn.DOCUMENT_TAG,
                      TupleTagList.of(RunQueryResponseToDocumentFn.ERROR_TAG)));
      PCollection<Document> documents =
          extractResult.get(RunQueryResponseToDocumentFn.DOCUMENT_TAG);
      PCollection<String> runQueryErrors =
          extractResult.get(RunQueryResponseToDocumentFn.ERROR_TAG);

      // 5. Prepare documents for writing to the destination database
      PCollection<Write> writes =
          documents.apply(
              ParDo.of(new PrepareWritesFn(destinationProjectId, destinationDatabaseId)));

      // 6. Write documents to the destination Firestore database
      PCollection<WriteFailure> writeFailures =
          writes.apply(
              "WriteToDestinationWithDeadLetterQueue",
              FirestoreIO.v1()
                  .write()
                  .withProjectId(destinationProjectId)
                  .withDatabaseId(destinationDatabaseId)
                  .batchWrite()
                  .withDeadLetterQueue()
                  .withRpcQosOptions(rpcQosOptions)
                  .build());
      PCollection<String> writeErrors =
          writeFailures.apply(
              "MapFailedWritesToErrorMessages",
              MapElements.into(TypeDescriptors.strings())
                  .via(
                      failure -> {
                        return String.format(
                            "Write failed for document: %s, error: %s",
                            getDocumentName(failure.getWrite()), failure.getStatus());
                      }));

      // 7. Log errors to GCS if errorWritePath is provided
      String errorWritePath = options.getErrorWritePath();
      PCollection<String> allErrors =
          PCollectionList.of(runQueryErrors)
              .and(writeErrors)
              .apply("MergeErrorCollections", Flatten.<String>pCollections());
      if (!Strings.isNullOrEmpty(errorWritePath)) {
        LOG.info("Error logging to GCS is enabled: {}", errorWritePath);
        allErrors.apply(
            "WriteErrorLogsToGcs",
            TextIO.write()
                .to(options.getErrorWritePath())
                .withSuffix("firestore_to_firestore_error.txt")
                .withWindowedWrites());
      }

      p.run();
      LOG.info("Pipeline.run() called.");
    } catch (Exception e) {
      LOG.error("Failed to run pipeline: {}", e.getMessage(), e);
      throw e;
    }
  }

  private static String getDocumentName(Write write) {
    if (write.hasUpdate()) {
      return write.getUpdate().getName();
    }
    if (write.hasDelete()) {
      return write.getDelete();
    }
    return "unknown doc name";
  }

  private static void validateOptions(Options options) {
    String sourceProjectId = options.getSourceProjectId();
    if (sourceProjectId == null || sourceProjectId.isEmpty()) {
      throw new IllegalArgumentException("sourceProjectId must be provided");
    }
    String sourceDatabaseId = options.getSourceDatabaseId();
    if (sourceDatabaseId == null || sourceDatabaseId.isEmpty()) {
      throw new IllegalArgumentException("sourceDatabaseId must be provided");
    }
    String destinationDatabaseId = options.getDestinationDatabaseId();
    if (destinationDatabaseId == null || destinationDatabaseId.isEmpty()) {
      throw new IllegalArgumentException("destinationDatabaseId must be provided");
    }
  }

  private static List<String> getAllCollectionIds(String projectId, String databaseId)
      throws Exception {
    FirestoreOptions firestoreOptions =
        FirestoreOptions.newBuilder().setProjectId(projectId).setDatabaseId(databaseId).build();
    try (Firestore db = firestoreOptions.getService()) {
      List<String> collectionIds = new ArrayList<>();
      Iterable<CollectionReference> collections = db.listCollections();
      for (CollectionReference collRef : collections) {
        collectionIds.add(collRef.getId());
      }
      return collectionIds;
    }
  }
}
