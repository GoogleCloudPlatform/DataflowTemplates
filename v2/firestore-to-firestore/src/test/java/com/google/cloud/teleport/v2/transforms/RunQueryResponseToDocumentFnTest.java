package com.google.cloud.teleport.v2.transforms;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.RunQueryResponse;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class RunQueryResponseToDocumentFnTest {

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
        input.apply(ParDo.of(new RunQueryResponseToDocumentFn()));

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
        input.apply(ParDo.of(new RunQueryResponseToDocumentFn()));

    PAssert.that(output).empty();
    p.run();
  }
}
