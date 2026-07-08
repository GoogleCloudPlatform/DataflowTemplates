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
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.transforms.CreatePartitionQueryRequestFn;
import com.google.cloud.teleport.v2.transforms.PrepareWritesFn;
import com.google.cloud.teleport.v2.transforms.RunQueryResponseToDocumentFn;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.KindExpression;
import com.google.datastore.v1.Query;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.Write;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
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
      "It does not support using an Enterprise edition database as the source.",
      "Data consistency is guaranteed only at the end of the pipeline when all data has been"
          + " written to the destination database.\n",
    },
    flexContainerName = "firestore-to-firestore",
    optionsClass = FirestoreToFirestore.Options.class)
public class FirestoreToFirestore {

  private static final Logger LOG = LoggerFactory.getLogger(FirestoreToFirestore.class);
  private static final int DEFAULT_MAX_NUM_WORKERS = 500;
  private static final String FIRESTORE_DEFAULT_DATABASE = "(default)";
  private static final String DATASTORE_DEFAULT_DATABASE = "";

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
        helpText =
            "The source database to read from. Use '(default)' or an empty string for the default"
                + " database.",
        example = "my-database")
    String getSourceDatabaseId();

    void setSourceDatabaseId(String value);

    @TemplateParameter.Text(
        groupName = "Source",
        order = 3,
        optional = true,
        description = "Collection Groups to Copy from Source Database",
        helpText =
            "Specifies collection groups to copy. If not provided, all collection groups will be "
                + "copied. Note: does NOT include all subcollections of provided Collection Groups "
                + "recursively. e.g. with data /users/bob/messages/msg1 and "
                + "/users/alice/messages/msg2, both `users` and `messages` must be provided to "
                + "copy all data in `users` and `messages` collections.",
        example = "users,messages")
    @Default.String("")
    String getCollectionGroupIds();

    void setCollectionGroupIds(String value);

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
            "The destination database to write to. Use '(default)' or an empty string for the"
                + " default database.",
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
  }

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
      String sourceDatabaseIdForFirestore =
          normalizeDatabaseIdForFirestore(options.getSourceDatabaseId());

      String destinationProjectId =
          options.getDestinationProjectId().isEmpty()
              ? sourceProjectId
              : options.getDestinationProjectId();
      String destinationDatabaseIdForFirestore =
          normalizeDatabaseIdForFirestore(options.getDestinationDatabaseId());

      int maxNumWorkers =
          options.getMaxNumWorkers() > 0 ? options.getMaxNumWorkers() : DEFAULT_MAX_NUM_WORKERS;
      RpcQosOptions rpcQosOptions =
          RpcQosOptions.newBuilder().withHintMaxNumWorkers(maxNumWorkers).build();

      Instant readTime =
          options.getReadTime().isEmpty() ? Instant.now() : Instant.parse(options.getReadTime());

      PCollection<String> collectionGroupIds = getCollectionGroupIds(p, options);

      LOG.info(
          "Starting pipeline execution with options: sourceProjectId={}, sourceDatabaseId={}, "
              + "destinationProjectId={}, destinationDatabaseId={}, collectionGroupIds={}, "
              + "maxNumWorkers={}, readTime={}",
          sourceProjectId,
          sourceDatabaseIdForFirestore,
          destinationProjectId,
          destinationDatabaseIdForFirestore,
          options.getCollectionGroupIds().isEmpty() ? "ALL" : options.getCollectionGroupIds(),
          maxNumWorkers,
          readTime);

      // 1. Construct the PartitionQuery requests for the collections.
      PCollection<PartitionQueryRequest> partitionQueryRequests =
          collectionGroupIds.apply(
              new CreatePartitionQueryRequestFn(
                  sourceProjectId, sourceDatabaseIdForFirestore, maxNumWorkers, readTime));

      // 2. Apply FirestoreIO to get partitions (as RunQueryRequests)
      PCollection<RunQueryRequest> partitionedQueries =
          partitionQueryRequests.apply(
              "Get Partitions",
              FirestoreIO.v1()
                  .read()
                  .partitionQuery()
                  .withProjectId(sourceProjectId)
                  .withDatabaseId(sourceDatabaseIdForFirestore)
                  .withReadTime(readTime)
                  .withRpcQosOptions(rpcQosOptions)
                  .build());

      // 3. Execute each partitioned query
      PCollection<RunQueryResponse> responses =
          partitionedQueries.apply(
              "Query Documents in Partitions",
              FirestoreIO.v1()
                  .read()
                  .runQuery()
                  .withProjectId(sourceProjectId)
                  .withDatabaseId(sourceDatabaseIdForFirestore)
                  .withReadTime(readTime)
                  .withRpcQosOptions(rpcQosOptions)
                  .build());

      // 4. Process the documents from the responses
      PCollection<Document> documents =
          responses.apply("Extract Documents", ParDo.of(new RunQueryResponseToDocumentFn()));

      // 5. Prepare documents for writing to the destination database
      PCollection<Write> writes =
          documents.apply(
              ParDo.of(
                  new PrepareWritesFn(destinationProjectId, destinationDatabaseIdForFirestore)));

      // 6. Write documents to the destination Firestore database
      writes.apply(
          "Write Documents to Destination",
          FirestoreIO.v1()
              .write()
              .withProjectId(destinationProjectId)
              .withDatabaseId(destinationDatabaseIdForFirestore)
              .batchWrite()
              .withRpcQosOptions(rpcQosOptions)
              .build());

      // 7. Run the pipeline
      PipelineResult result = p.run();
      LOG.info("Pipeline started.");
    } catch (Exception e) {
      LOG.error("Failed to run pipeline: {}", e.getMessage(), e);
      throw e;
    }
  }

  private static void validateOptions(Options options) {
    String sourceProjectId = options.getSourceProjectId();
    if (sourceProjectId == null || sourceProjectId.isEmpty()) {
      throw new IllegalArgumentException("sourceProjectId must be provided");
    }
    String sourceDatabaseId = options.getSourceDatabaseId();
    if (sourceDatabaseId == null) {
      throw new IllegalArgumentException("sourceDatabaseId must be provided");
    }
    String destinationDatabaseId = options.getDestinationDatabaseId();
    if (destinationDatabaseId == null) {
      throw new IllegalArgumentException("destinationDatabaseId must be provided");
    }
  }

  private static PCollection<String> getCollectionGroupIds(Pipeline p, Options options) {
    if (options.getCollectionGroupIds() == null || options.getCollectionGroupIds().isEmpty()) {
      LOG.info("No collectionGroupIds provided. Discovering all...");
      Query query =
          Query.newBuilder().addKind(KindExpression.newBuilder().setName("__kind__")).build();
      String sourceDatabaseIdForDatastore =
          normalizeDatabaseIdForDatastore(options.getSourceDatabaseId());
      return p.apply(
              "Find All Collection Groups",
              DatastoreIO.v1()
                  .read()
                  .withProjectId(options.getSourceProjectId())
                  .withDatabaseId(sourceDatabaseIdForDatastore)
                  .withQuery(query))
          .apply(
              "Extract Collection Group Names",
              ParDo.of(
                  new DoFn<Entity, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      Entity entity = c.element();
                      String kind = entity.getKey().getPath(0).getName();
                      // Filter internal Kinds.
                      if (!kind.startsWith("__")) {
                        c.output(kind);
                      }
                    }
                  }));
    }

    List<String> collectionGroupIdsList =
        Arrays.stream(options.getCollectionGroupIds().split(","))
            .map(String::trim)
            .collect(Collectors.toList());
    return p.apply("Create Collection Groups", Create.of(collectionGroupIdsList));
  }

  /**
   * Normalizes the database ID for use with FirestoreIO methods.
   *
   * <p>The default database ID in the Firestore API is "(default)".
   */
  private static String normalizeDatabaseIdForFirestore(String databaseId) {
    if (databaseId.isEmpty() || FIRESTORE_DEFAULT_DATABASE.equals(databaseId)) {
      return FIRESTORE_DEFAULT_DATABASE;
    }
    return databaseId;
  }

  /**
   * Normalizes the database ID for use with DatastoreIO methods.
   *
   * <p>The default database ID in the Datastore API is an empty string.
   */
  private static String normalizeDatabaseIdForDatastore(String databaseId) {
    if (databaseId.isEmpty() || FIRESTORE_DEFAULT_DATABASE.equals(databaseId)) {
      return DATASTORE_DEFAULT_DATABASE;
    }
    return databaseId;
  }
}
