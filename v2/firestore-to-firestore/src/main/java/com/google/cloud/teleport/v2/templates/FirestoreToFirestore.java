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
import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreReadOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreWriteOptions;
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
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

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
    optionsClass = FirestoreToFirestore.Options.class,
    skipOptions = {
        "firestoreReadGqlQuery",
        "datastoreReadGqlQuery",
        "datastoreReadProjectId",
        "datastoreWriteProjectId"
    })
public class FirestoreToFirestore {

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard Firestore configuration options.
   */
  public interface Options
      extends DatastoreReadOptions, DatastoreWriteOptions, DataflowPipelineOptions {

  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    String sourceProject = options.getFirestoreReadProjectId().get();
    String sourceDb =
        options.getFirestoreReadDatabaseId().get().isEmpty()
            ? "(default)"
            : options.getFirestoreReadDatabaseId().get();

    String destProject = options.getFirestoreWriteProjectId().get();
    String destDb =
        options.getFirestoreWriteDatabaseId().get().isEmpty()
            ? "(default)"
            : options.getFirestoreWriteProjectId().get();

    String collectionId = options.getFirestoreReadCollection().get();
    long partitionCount =
        options.getFirestoreHintNumWorkers() != null
            ? options.getFirestoreHintNumWorkers().get()
            : 500L;

    RpcQosOptions rpcQosOptions =
        RpcQosOptions.newBuilder()
            .withHintMaxNumWorkers(options.as(DataflowPipelineOptions.class).getMaxNumWorkers())
            .build();

    // 1. Define the base query to be partitioned
    StructuredQuery baseQuery =
        StructuredQuery.newBuilder()
            // TODO: pacoavila - uncomment
            // .addFrom(CollectionSelector.newBuilder().setCollectionId(collectionId))
            .addOrderBy(
                StructuredQuery.Order.newBuilder()
                    .setField(StructuredQuery.FieldReference.newBuilder().setFieldPath("__name__"))
                    .setDirection(StructuredQuery.Direction.ASCENDING))
            .build();

    // 2. Create the PartitionQueryRequest
    PartitionQueryRequest partitionRequest =
        PartitionQueryRequest.newBuilder()
            .setParent(DocumentRootName.format(sourceProject, sourceDb))
            .setStructuredQuery(baseQuery)
            .setPartitionCount(partitionCount)
            .build();

    Instant readTime =
        options.getFirestoreReadTime().get().isEmpty()
            ? Instant.now()
            : Instant.parse(options.getFirestoreReadTime().get());

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

    // 5. Process the documents from the responses
    PCollection<Document> documents =
        responses.apply("ExtractDocuments", ParDo.of(new RunQueryResponseToDocumentFn()));

    // 3. Prepare documents for writing to the destination database
    PCollection<Write> writes = documents.apply(ParDo.of(new PrepareWritesFn(destProject, destDb)));

    // 4. Write documents to the destination Firestore database
    writes.apply(
        FirestoreIO.v1()
            .write()
            .withProjectId(destProject)
            .withDatabaseId(destDb)
            .batchWrite()
            .withRpcQosOptions(rpcQosOptions)
            .build());

    p.run().waitUntilFinish();
  }
}
