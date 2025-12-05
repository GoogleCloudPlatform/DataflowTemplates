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

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.transforms.PrepareWritesFn;
import com.google.cloud.teleport.v2.transforms.RunQueryResponseToDocumentFn;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.DocumentRootName;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.Write;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
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
        "The Firestore to Firestore template is a batch pipeline that reads documents from a<a"
            + " href=\"https://cloud.google.com/firestore/docs\">Firestore</a> database and writes"
            + " them to another Firestore database.",
        "Data consistency is guaranteed only at the end of the pipeline when all data has been"
            + " written to the destination database.\n",
        "Any errors that occur during operation are recorded in error queues. The error"
            + " queue is a Cloud Storage folder which stores all the Datastream events that had"
            + " encountered errors."
    },
    flexContainerName = "firestore-to-firestore",
    optionsClass = FirestoreToFirestore.Options.class)
public class FirestoreToFirestore {

  private static final Logger LOG = LoggerFactory.getLogger(FirestoreToFirestore.class);


  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard Firestore configuration options.
   */
  public interface Options extends DataflowPipelineOptions {

    @TemplateParameter.Text(
        groupName = "Source",
        order = 1,
        description = "Source Project ID",
        helpText = "The source project to read from.",
        example = "my-project")
    String getSourceProjectId();

    void setSourceProjectId(String value);

    @TemplateParameter.Text(
        groupName = "Destination",
        order = 1,
        description = "Destination Project ID",
        helpText = "The destination project to write to.",
        example = "my-project")
    String getDestinationProjectId();

    void setDestinationProjectId(String value);

    @TemplateParameter.Text(
        groupName = "Source",
        order = 1,
        description = "Source Database ID",
        helpText = "The source database to read from.",
        example = "my-database")
    @Default.String("(default)")
    String getSourceDatabaseId();

    void setSourceDatabaseId(String value);

    @TemplateParameter.Text(
        groupName = "Destination",
        order = 1,
        description = "Destination Database ID",
        helpText = "The destination database to write to.",
        example = "my-database")
    @Default.String("(default)")
    String getDestinationDatabaseId();

    void setDestinationDatabaseId(String value);

    @TemplateParameter.Text(
        groupName = "Target",
        order = 9,
        description = "Database collection filter (optional)",
        helpText =
            "If specified, only replicate this collection. If not specified, replicate all collections.",
        example = "my-collection",
        optional = true)
    String getDatabaseCollection();

    void setDatabaseCollection(String value);

    @TemplateParameter.Integer(
        order = 11,
        optional = true,
        description = "Batch size",
        helpText = "The batch size for writing to Database.")
    @Default.Integer(500)
    Integer getBatchSize();

    void setBatchSize(Integer value);


    @TemplateParameter.DateTime(
        order = 9,
        optional = true,
        description = "Read Time",
        helpText = "The read time of the Firestore read operations.")
    @Default.String("")
    String getReadTime();

    void setReadTime(String readTime);

  }

  public static void main(String[] args) {
    try {
      UncaughtExceptionLogger.register();

      LOG.info("Starting Firestore to Firestore pipeline...");

      Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
      LOG.info("Pipeline options parsed and validated.");

      Pipeline p = Pipeline.create(options);
      LOG.info("Pipeline created.");

      String sourceProject = options.getSourceProjectId();
      String sourceDb =
          options.getSourceDatabaseId().isEmpty()
              ? "(default)"
              : options.getSourceDatabaseId();

      String destProject = options.getDestinationProjectId();
      String destDb =
          options.getDestinationDatabaseId().isEmpty()
              ? "(default)"
              : options.getDestinationProjectId();

      String collectionId = options.getDatabaseCollection();
      long partitionCount =
          options.getBatchSize();

      int maxNumWorkers = options.as(DataflowPipelineOptions.class).getMaxNumWorkers();
      RpcQosOptions rpcQosOptions =
          RpcQosOptions.newBuilder()
              .withHintMaxNumWorkers(maxNumWorkers)
              .build();

      Instant readTime =
          options.getReadTime().isEmpty()
              ? Instant.now()
              : Instant.parse(options.getReadTime());

      LOG.info(
          "Starting pipeline execution with options: sourceProjectId={}, sourceDatabaseId={}, "
              + "destinationProjectId={}, destinationDatabaseId={}, maxNumWorkers={}, readTime={}",
          sourceProject,
          sourceDb,
          destProject,
          destDb,
          maxNumWorkers,
          readTime);

      // 1. Define the base query to be partitioned
      StructuredQuery baseQuery =
          StructuredQuery.newBuilder()
              // TODO: pacoavila - uncomment
              // .addFrom(CollectionSelector.newBuilder().setCollectionId(collectionId))
              .addOrderBy(
                  StructuredQuery.Order.newBuilder()
                      .setField(
                          StructuredQuery.FieldReference.newBuilder().setFieldPath("__name__"))
                      .setDirection(StructuredQuery.Direction.ASCENDING))
              .build();

      // 2. Create the PartitionQueryRequest
      PartitionQueryRequest partitionRequest =
          PartitionQueryRequest.newBuilder()
              .setParent(DocumentRootName.format(sourceProject, sourceDb))
              .setStructuredQuery(baseQuery)
              .setPartitionCount(partitionCount)
              .build();

      // 3. Apply FirestoreIO to get partitions (as RunQueryRequests)
      PCollection<RunQueryRequest> partitionedQueries =
          p.apply("CreatePartitionRequest", Create.of(partitionRequest))
              .apply(
                  "GetPartitions",
                  FirestoreIO.v1()
                      .read()
                      .partitionQuery()
                      .withReadTime(readTime)
                      .withRpcQosOptions(rpcQosOptions)
                      .build());
      LOG.info("Completed constructing PartitionQuery requests.");

      // 4. Execute each partitioned query
      PCollection<RunQueryResponse> responses =
          partitionedQueries
              // TODO: pacoavila - Include the project / database here.
              .apply(
                  "ExecutePartitions",
                  FirestoreIO.v1()
                      .read()
                      .runQuery()
                      .withReadTime(readTime)
                      .withRpcQosOptions(rpcQosOptions)
                      .build());
      LOG.info("Completed PartitionQuery requests.");

      // 5. Process the documents from the responses
      PCollection<Document> documents =
          responses.apply("ExtractDocuments", ParDo.of(new RunQueryResponseToDocumentFn()));
      LOG.info("Finished converting PartitionQuery results into Documents");

      // 6. Prepare documents for writing to the destination database
      PCollection<Write> writes = documents.apply(
          ParDo.of(new PrepareWritesFn(destProject, destDb)));
      LOG.info("Finished converting Documents to Write requests.");

      // 7. Write documents to the destination Firestore database
      writes.apply(
          FirestoreIO.v1()
              .write()
              .withProjectId(destProject)
              .withDatabaseId(destDb)
              .batchWrite()
              .withRpcQosOptions(rpcQosOptions)
              .build());
      LOG.info("Finished applying writes to destination database.");

      p.run().waitUntilFinish();
      LOG.info("Pipeline Finished!");
    } catch (Exception e) {
      LOG.error("Failed to run pipeline: {}", e.getMessage(), e);
      throw e;
    }
  }
}
