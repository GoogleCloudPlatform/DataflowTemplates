package com.google.cloud.teleport.v2.transforms;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.RunQueryResponse;
import org.apache.beam.sdk.transforms.DoFn;


/**
 * DoFn to extract Documents from RunQueryResponse
 */
public class RunQueryResponseToDocumentFn extends DoFn<RunQueryResponse, Document> {
  @ProcessElement
  public void processElement(ProcessContext c) {
    RunQueryResponse response = c.element();
    if (response != null && response.hasDocument()) {
      c.output(response.getDocument());
    }
  }
}
