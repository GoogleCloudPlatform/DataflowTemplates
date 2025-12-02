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
import com.google.firestore.v1.Document;
import com.google.firestore.v1.DocumentRootName;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.CollectionSelector;
import com.google.firestore.v1.Write;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

@Template(
    name = "Cloud_Firestore_to_Firestore",
    category = TemplateCategory.BATCH,
    displayName = "Firestore to Firestore",
    // TODO: pacoavila - update this.
    description = {
      "The Firestore to Firestore template is a batch pipeline that reads <a"
          + " href=\"https://cloud.google.com/firestore/docs\">Firestore</a> documents from a"
          + " Firestore database and writes them to a Firestore with MongoDB compatibility database. It is intended for data"
          + " migration from Firestore Standard to Firestore with MongoDB compatibility.\n",
      "Data consistency is guaranteed only at the end of migration when all data has been written"
          + " to the destination database.\n",
      "The pipeline by default processes backfill events first with batch write, which is"
          + " optimized for performance, followed by cdc events. This is configurable via setting"
          + " `processBackfillFirst` to false to process backfill and cdc events together.\n",
      "Any errors that occur during operation are recorded in error queues. The error"
          + " queue is a Cloud Storage folder which stores all the Datastream events that had"
          + " encountered errors."
    },
    flexContainerName = "firestore-to-firestore",
    optionsClass = FirestoreToFirestore.Options.class)
public class FirestoreToFirestore {

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options
      extends DatastoreReadOptions, DatastoreWriteOptions, DataflowPipelineOptions {

    // String getSourceProjectId();
    //
    // void setSourceProjectId(String value);
    //
    // @TemplateParameter.Text(
    //     groupName = "Source",
    //     order = 8,
    //     description = "Database ID",
    //     helpText = "The database to read from.",
    //     example = "(default)")
    // @Default.String("(default)")
    // String getSourceDatabaseId();
    //
    // void setSourceDatabaseId(String value);
    //
    // @TemplateParameter.Text(
    //     groupName = "Destination",
    //     order = 8,
    //     description = "Database ID",
    //     helpText = "The database to write to.",
    //     example = "(default)")
    // @Default.String("(default)")
    // String getDestinationDatabaseId();
    //
    // void setDestinationDatabaseId(String value);
    //
    // @TemplateParameter.Text(
    //     groupName = "Destination",
    //     order = 9,
    //     description = "Database collection filter (optional)",
    //     helpText =
    //         "If specified, only replicate this collection. If not specified, replicate all
    // collections.",
    //     example = "my-collection",
    //     optional = true)
    // String getDatabaseCollection();

    //
    // Long getPartitionCount();
    //
    // void setPartitionCount(Long value);
    //
    // @TemplateParameter.Integer(
    //     order = 11,
    //     optional = true,
    //     description = "Batch size",
    //     helpText = "The batch size for writing to Database.")
    // @Default.Integer(500)
    // Integer getBatchSize();
    //
    // void setBatchSize(Integer value);
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

    // TODO:pacoavila - dedicated param.
    String collectionId = options.getFirestoreWriteEntityKind().get();
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
            .addFrom(CollectionSelector.newBuilder().setCollectionId(collectionId))
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

    // TODO: pacoavila - Add read time flag.
    Instant readTime = Instant.now();

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
        responses.apply("ExtractDocuments", ParDo.of(new RunQueryResponseToDocument()));

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

  // DoFn to extract Documents from RunQueryResponse
  static class RunQueryResponseToDocument extends DoFn<RunQueryResponse, Document> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      RunQueryResponse response = c.element();
      if (response != null && response.hasDocument()) {
        c.output(response.getDocument());
      }
    }
  }

  // DoFn to convert Document to Write requests for the destination database
  static class PrepareWritesFn extends DoFn<Document, Write> {

    private final String projectId;
    private final String databaseId;

    PrepareWritesFn(String projectId, String databaseId) {
      this.projectId = projectId;
      this.databaseId = databaseId;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Document doc = c.element();
      // Rebuild the document name for the destination project/database
      String originalName = doc.getName();
      String path = originalName.substring(originalName.indexOf("/documents/") + 1);
      String newName = String.format("projects/%s/databases/%s/%s", projectId, databaseId, path);

      Document newDoc = doc.toBuilder().setName(newName).build();
      c.output(Write.newBuilder().setUpdate(newDoc).build());
    }
  }
}
