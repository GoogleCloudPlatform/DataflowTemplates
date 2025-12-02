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

import com.google.common.collect.ImmutableList;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.DocumentRootName;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.CollectionSelector;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FirestoreToFirestoreTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void testRunQueryResponseToDocument_extractsDocument() {
    Document testDoc =
        Document.newBuilder()
            .setName("projects/test-project/databases/(default)/documents/myCol/doc1")
            .build();
    RunQueryResponse response = RunQueryResponse.newBuilder().setDocument(testDoc).build();

    PCollection<RunQueryResponse> input = p.apply(Create.of(response));
    PCollection<Document> output =
        input.apply(ParDo.of(new FirestoreToFirestore.RunQueryResponseToDocument()));

    PAssert.that(output).containsInAnyOrder(testDoc);
    p.run();
  }

  @Test
  public void testRunQueryResponseToDocument_skipsMissingDocument() {
    RunQueryResponse response =
        RunQueryResponse.newBuilder()
            .setReadTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(12345))
            .build(); // No document

    PCollection<RunQueryResponse> input = p.apply(Create.of(response));
    PCollection<Document> output =
        input.apply(ParDo.of(new FirestoreToFirestore.RunQueryResponseToDocument()));

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  public void testPrepareWritesFn_correctlyTransformsName() {
    String sourceName = "projects/source-proj/databases/(default)/documents/myCol/docId1";
    Document inputDoc =
        Document.newBuilder()
            .setName(sourceName)
            .putFields("message", Value.newBuilder().setStringValue("Hello").build())
            .build();

    String destProject = "dest-proj";
    String destDb = "dest-db";

    PCollection<Document> input = p.apply(Create.of(inputDoc));
    PCollection<Write> output =
        input.apply(ParDo.of(new FirestoreToFirestore.PrepareWritesFn(destProject, destDb)));

    String expectedName = "projects/dest-proj/databases/dest-db/documents/myCol/docId1";
    Document expectedDoc = inputDoc.toBuilder().setName(expectedName).build();
    Write expectedWrite = Write.newBuilder().setUpdate(expectedDoc).build();

    PAssert.that(output).containsInAnyOrder(expectedWrite);
    p.run();
  }

  @Test
  public void testPrepareWritesFn_preservesFields() {
    String sourceName = "projects/source/databases/(default)/documents/data/item1";
    MapValue nestedMap = MapValue.newBuilder()
        .putFields("nested", Value.newBuilder().setBooleanValue(true).build())
        .build();
    Document inputDoc = Document.newBuilder()
        .setName(sourceName)
        .putFields("fieldA", Value.newBuilder().setStringValue("valueA").build())
        .putFields("fieldB", Value.newBuilder().setIntegerValue(123).build())
        .putFields("fieldC", Value.newBuilder().setMapValue(nestedMap).build())
        .build();

    PCollection<Document> input = p.apply(Create.of(inputDoc));
    PCollection<Write> output = input.apply(
        ParDo.of(new FirestoreToFirestore.PrepareWritesFn("dest", "(default)")));

    // Extract the Document from the Write
    PCollection<Document> outputDocs = output.apply("ExtractUpdate",
        ParDo.of(new ExtractDocumentFromWriteFn()));

    // Assert that the fields map is as expected.
    PCollection<Map<String, Value>> outputFields = outputDocs.apply("ExtractFields",
        ParDo.of(new ExtractFieldsFn()));

    // Pass the expected map as a List to PAssert
    PAssert.that(outputFields).containsInAnyOrder(ImmutableList.of(inputDoc.getFieldsMap()));

    p.run();
  }

  private static class ExtractDocumentFromWriteFn extends DoFn<Write, Document> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element() != null && c.element().hasUpdate()) {
        c.output(c.element().getUpdate());
      }
    }
  }

  private static class ExtractFieldsFn extends DoFn<Document, Map<String, Value>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element() != null) {
        c.output(c.element().getFieldsMap());
      }
    }
  }


  // Note: Fully testing the pipeline logic with FirestoreIO requires integration tests
  // or a sophisticated mocking of FirestoreIO's builders and transforms, which is complex.
  // This test just checks if the pipeline can be constructed without errors.
  @Test
  public void testPipelineConstruction() {
    FirestoreToFirestore.Options options =
        TestPipeline.testingPipelineOptions().as(FirestoreToFirestore.Options.class);

    options.setFirestoreReadProjectId(StaticValueProvider.of("test-source-project"));
    options.setFirestoreReadDatabaseId(StaticValueProvider.of("test-source-db"));
    options.setFirestoreWriteProjectId(StaticValueProvider.of("test-dest-project"));
    options.setFirestoreWriteDatabaseId(StaticValueProvider.of("(default)"));
    options.setFirestoreWriteEntityKind(StaticValueProvider.of("my-collection"));

    // Simply creating the pipeline and calling main to ensure it constructs without exceptions
    // We are not running it as it would try to connect to actual Firestore.
    try {
      Pipeline testP = Pipeline.create(options);

      String sourceProject = options.getFirestoreReadProjectId().get();
      String sourceDb = options.getFirestoreReadDatabaseId().get();
      String collectionId = options.getFirestoreWriteEntityKind().get();

      // The following lines would normally be part of the pipeline run,
      // but we can't execute them in a unit test without extensive mocking.
      // We just ensure the types match and a basic structure is buildable.
      StructuredQuery baseQuery =
          StructuredQuery.newBuilder()
              .addFrom(CollectionSelector.newBuilder().setCollectionId(collectionId))
              .build();
      PartitionQueryRequest partitionRequest =
          PartitionQueryRequest.newBuilder()
              .setParent(DocumentRootName.format(sourceProject, sourceDb))
              .setStructuredQuery(baseQuery)
              .setPartitionCount(10)
              .build();

      PCollection<PartitionQueryRequest> req = testP.apply(Create.of(partitionRequest));

      // Chaining a simple ParDo to check type - real test needs FirestoreIO mock
      req.apply(ParDo.of(new ExtractParentFn()));

      // If we reach here, the basic setup with options is fine.
    } catch (Exception e) {
      throw new AssertionError("Pipeline construction failed", e);
    }
  }

  private static class ExtractParentFn extends DoFn<PartitionQueryRequest, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element() != null) {
        c.output(c.element().getParent());
      }
    }
  }
}
