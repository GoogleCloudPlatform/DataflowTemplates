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
package com.google.cloud.teleport.v2.transforms;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.RunQueryResponse;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;

public class RunQueryResponseToDocumentFnTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void testRunQueryResponseToDocument_extractsDocument() {
    Document testDoc =
        Document.newBuilder()
            .setName("projects/test-project/databases/(default)/documents/myCol/doc1")
            .build();
    RunQueryResponse response = RunQueryResponse.newBuilder().setDocument(testDoc).build();

    PCollection<RunQueryResponse> input = p.apply(Create.of(response));
    PCollectionTuple output =
        input.apply(
            ParDo.of(new RunQueryResponseToDocumentFn())
                .withOutputTags(
                    RunQueryResponseToDocumentFn.DOCUMENT_TAG,
                    TupleTagList.of(RunQueryResponseToDocumentFn.ERROR_TAG)));
    PCollection<Document> outputDocs = output.get(RunQueryResponseToDocumentFn.DOCUMENT_TAG);
    PCollection<String> outputErrors = output.get(RunQueryResponseToDocumentFn.ERROR_TAG);

    PAssert.that(outputDocs).containsInAnyOrder(testDoc);
    PAssert.that(outputErrors).empty();
    p.run();
  }

  @Test
  public void testRunQueryResponseToDocument_skipsMissingDocument() {
    RunQueryResponse response =
        RunQueryResponse.newBuilder()
            .setReadTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(12345))
            .build(); // No document

    PCollection<RunQueryResponse> input = p.apply(Create.of(response));
    PCollectionTuple output =
        input.apply(
            ParDo.of(new RunQueryResponseToDocumentFn())
                .withOutputTags(
                    RunQueryResponseToDocumentFn.DOCUMENT_TAG,
                    TupleTagList.of(RunQueryResponseToDocumentFn.ERROR_TAG)));
    PCollection<Document> outputDocs = output.get(RunQueryResponseToDocumentFn.DOCUMENT_TAG);
    PCollection<String> outputErrors = output.get(RunQueryResponseToDocumentFn.ERROR_TAG);

    PAssert.that(outputDocs).empty();
    PAssert.that(outputErrors)
        .containsInAnyOrder("Response is null or has no document: " + response);
    p.run();
  }
}
